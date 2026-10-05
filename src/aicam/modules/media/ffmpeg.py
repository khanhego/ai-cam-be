"""Lệnh FFmpeg / ffprobe cho cắt clip (J-01, stream copy — ADR-003) và bản xuất (J-03 — ADR-008).

Hàm dựng lệnh là hàm thuần (unit test so chuỗi lệnh); hàm chạy dùng asyncio subprocess có timeout.
"""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from pathlib import Path


class FFmpegError(Exception):
    pass


async def run(cmd: list[str], timeout_s: float) -> str:
    """Chạy lệnh, trả stdout; lỗi / quá giờ → FFmpegError (kèm đuôi stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise FFmpegError(f"Quá {timeout_s:.0f} giây: {cmd[0]}") from exc
    if proc.returncode != 0:
        raise FFmpegError(err.decode(errors="replace").strip()[-500:] or f"{cmd[0]} lỗi {proc.returncode}")
    return out.decode(errors="replace")


async def probe_duration(ffprobe: str, path: Path, timeout_s: float = 20) -> float:
    out = await run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)], timeout_s
    )
    try:
        return float(out.strip())
    except ValueError as exc:
        raise FFmpegError(f"Không đọc được độ dài video: {path.name}") from exc


def concat_listing(paths: list[Path]) -> str:
    """Nội dung file danh sách cho concat demuxer (đường dẫn tuyệt đối, escape dấu nháy đơn)."""
    lines = []
    for p in paths:
        escaped = str(p).replace("'", "'\\''")
        lines.append(f"file '{escaped}'\n")
    return "".join(lines)


def cut_command(ffmpeg: str, listing: Path, ss: float, to: float, out: Path) -> list[str]:
    """Cắt bằng stream copy: `-ss`/`-to` là tùy chọn đầu vào → đầu clip lùi về keyframe trước `ss`
    (luôn phủ đủ khoảng yêu cầu), cuối clip chính xác tới `to` (spike S3)."""
    return [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
        "-f", "concat", "-safe", "0", "-ss", f"{ss:.3f}", "-to", f"{to:.3f}", "-i", str(listing),
        "-map", "0", "-c", "copy", "-movflags", "+faststart", str(out),
    ]  # fmt: skip


def sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(chunk):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------- bản xuất (J-03)

_SCALE = "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1"
_BOX = "fontcolor=white:fontsize=30:box=1:boxcolor=black@0.55:boxborderw=8"


def _font(font_file: Path | None) -> str:
    if font_file is None:
        return ""
    return "fontfile=" + str(font_file).replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'") + ":"


def _filter_path(path: Path) -> str:
    return str(path).replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def clock_overlays(
    pieces: list[tuple[float, float, int]], font_file: Path | None, x: str = "24", y: str = "72"
) -> list[str]:
    """Một drawtext giờ thực cho mỗi đoạn liền mạch `(từ giây, tới giây, epoch giờ hiển thị)`.

    `epoch` đã cộng chênh múi giờ hiển thị (giờ VN), nên dùng `gmtime` — không phụ thuộc TZ của tiến trình.
    Clip thiếu đoạn có nhiều đoạn với epoch khác nhau → giờ trên hình luôn là giờ thật của khung (DEC-102).
    """
    out = []
    for start, end, epoch in pieces:
        base = round(epoch - start)
        enable = f":enable='between(t,{start:.3f},{end:.3f})'" if len(pieces) > 1 else ""
        out.append(
            f"drawtext={_font(font_file)}"
            f"text='%{{pts\\:gmtime\\:{base}\\:%d/%m/%Y %H\\\\\\:%M\\\\\\:%S}}':x={x}:y={y}:{_BOX}{enable}"
        )
    return out


def export_command(
    ffmpeg: str,
    inputs: list[tuple[Path, float]],
    out: Path,
    *,
    text_file: Path,
    clock: list[str],
    font_file: Path | None,
    duration: float,
) -> list[str]:
    """H.264 720p mỗi camera (`veryfast`, CRF 26), overlay chữ tĩnh + giờ thực; 2 đầu vào → `hstack`.

    `inputs`: (file clip, giây bỏ qua ở đầu để hai camera khớp giờ). Âm thanh lấy từ đầu vào đầu tiên nếu có.
    """
    static = (
        f"drawtext={_font(font_file)}textfile='{_filter_path(text_file)}':expansion=none:x=24:y=24:{_BOX}"
    )
    overlay = ",".join([static, *clock])
    if len(inputs) == 2:
        graph = f"[0:v]{_SCALE}[a];[1:v]{_SCALE}[b];[a][b]hstack=inputs=2,{overlay}[v]"
    else:
        graph = f"[0:v]{_SCALE},{overlay}[v]"
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]
    for path, skip in inputs:
        cmd += ["-ss", f"{skip:.3f}", "-i", str(path)]
    cmd += [
        "-filter_complex", graph, "-map", "[v]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "26", "-pix_fmt", "yuv420p",
        "-profile:v", "high", "-c:a", "aac", "-b:a", "96k",
        "-t", f"{duration:.3f}", "-movflags", "+faststart",
        "-progress", "pipe:1", "-nostats", str(out),
    ]  # fmt: skip
    return cmd


async def run_with_progress(
    cmd: list[str], duration: float, timeout_s: float, on_progress: Callable[[int], Awaitable[None]]
) -> None:
    """Chạy ffmpeg có `-progress pipe:1`, gọi `on_progress(0..99)` khi phần trăm tăng."""
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    assert proc.stdout is not None  # noqa: S101 — PIPE luôn có
    assert proc.stderr is not None  # noqa: S101

    async def _read() -> None:
        assert proc.stdout is not None  # noqa: S101
        last = -1
        while line := await proc.stdout.readline():
            key, _, value = line.decode(errors="replace").strip().partition("=")
            if key == "out_time_us" and value.isdigit() and duration > 0:
                pct = min(99, int(int(value) / 1_000_000 / duration * 100))
                if pct > last:
                    last = pct
                    await on_progress(pct)

    try:
        await asyncio.wait_for(_read(), timeout=timeout_s)
        err = await asyncio.wait_for(proc.stderr.read(), timeout=10)
        await asyncio.wait_for(proc.wait(), timeout=10)
    except TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise FFmpegError(f"Quá {timeout_s:.0f} giây khi tạo bản xuất") from exc
    if proc.returncode != 0:
        raise FFmpegError(err.decode(errors="replace").strip()[-500:] or "ffmpeg lỗi")
