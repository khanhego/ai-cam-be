"""Lệnh FFmpeg / ffprobe cho cắt clip (J-01, stream copy — ADR-003) và bản xuất (J-03 — ADR-008).

Hàm dựng lệnh là hàm thuần (unit test so chuỗi lệnh); hàm chạy dùng asyncio subprocess có timeout.
"""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
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

_BOX = "fontcolor=white:fontsize=30:box=1:boxcolor=black@0.55:boxborderw=8"


def _font(font_file: Path | None) -> str:
    return "" if font_file is None else f"fontfile={_filter_path(font_file)}:"


def _filter_path(path: Path) -> str:
    """Escape giá trị tùy chọn filter không đặt trong nháy (`\\`, `:`, `'`, `,`)."""
    out = str(path)
    for ch in ("\\", ":", "'", ",", ";", "[", "]"):
        out = out.replace(ch, "\\" + ch)
    return out


def clock_overlays(
    pieces: list[tuple[float, float, float]], font_file: Path | None, x: str = "24", y: str = "72"
) -> list[str]:
    """Một drawtext giờ thực cho mỗi đoạn liền mạch `(từ giây, tới giây, epoch giờ hiển thị)`.

    `epoch` đã cộng chênh múi giờ hiển thị (giờ VN), nên dùng `gmtime` — không phụ thuộc TZ của tiến trình.
    Clip thiếu đoạn có nhiều đoạn với epoch khác nhau → giờ trên hình luôn là giờ thật của khung (DEC-102).
    """
    out = []
    for start, end, epoch in pieces:
        base = epoch - start
        enable = f":enable='between(t,{start:.3f},{end:.3f})'" if len(pieces) > 1 else ""
        out.append(
            f"drawtext={_font(font_file)}"
            f"text='%{{pts\\:gmtime\\:{base:.3f}\\:%d/%m/%Y %H\\\\\\:%M\\\\\\:%S}}'"
            f":x={x}:y={y}:{_BOX}{enable}"
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
    preset: str = "veryfast",
    side_scale: str = "1280:720",
) -> list[str]:
    """H.264 720p mỗi camera (`veryfast`, CRF 26), overlay chữ tĩnh + giờ thực; 2 đầu vào → `hstack`.

    `inputs`: (file clip, giây bỏ qua ở đầu để hai camera khớp giờ). Âm thanh lấy từ đầu vào đầu tiên nếu có.
    """
    scale = _scale(side_scale)
    static = f"drawtext={_font(font_file)}textfile={_filter_path(text_file)}:expansion=none:x=24:y=24:{_BOX}"
    overlay = ",".join([static, *clock])
    if len(inputs) == 2:
        graph = f"[0:v]{scale}[a];[1:v]{scale}[b];[a][b]hstack=inputs=2,{overlay}[v]"
    else:
        graph = f"[0:v]{scale},{overlay}[v]"
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]
    for path, skip in inputs:
        cmd += ["-ss", f"{skip:.3f}", "-i", str(path)]
    cmd += [
        "-filter_complex", graph, "-map", "[v]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", preset, "-crf", "26", "-pix_fmt", "yuv420p",
        "-profile:v", "high", "-c:a", "aac", "-b:a", "96k",
        "-t", f"{duration:.3f}", "-movflags", "+faststart",
        "-progress", "pipe:1", "-nostats", str(out),
    ]  # fmt: skip
    return cmd


@dataclass(frozen=True)
class Part:
    """Một đoạn của nửa hình bản xuất ghép: video giây `start`→`end` của clip, hoặc khe hở (khung đen)."""

    kind: str  # VIDEO | GAP
    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start


EXPORT_FPS = 25  # mọi đoạn đưa về cùng fps trước concat (khung đen của khe hở dùng cùng nhịp)


def _side_chain(index: int, parts: list[Part], scale: str, gap_label: str, label: str) -> str:
    """Một nửa hình căn giờ thực: tách đầu vào theo đoạn video, chèn khung đen vào khe hở, nối lại."""
    video_parts = [p for p in parts if p.kind == "VIDEO"]
    chains: list[str] = []
    sources = [f"s{index}_{j}" for j in range(len(video_parts))]
    if video_parts:
        split = f"split={len(video_parts)}" + "".join(f"[{x}]" for x in sources)
        chains.append(f"[{index}:v]setpts=PTS-STARTPTS,{split}")
    w, _, h = scale.partition(":")
    norm = f"fps={EXPORT_FPS},format=yuv420p"
    names, vi = [], 0
    for j, part in enumerate(parts):
        name = f"p{index}_{j}"
        names.append(f"[{name}]")
        if part.kind == "VIDEO":
            chains.append(
                f"[{sources[vi]}]trim=start={part.start:.3f}:end={part.end:.3f},setpts=PTS-STARTPTS,"
                f"{_scale(scale)},{norm}[{name}]"
            )
            vi += 1
        else:
            label_filter = f",{gap_label}" if gap_label else ""
            chains.append(
                f"color=c=black:s={w}x{h}:r={EXPORT_FPS}:d={part.duration:.3f},setsar=1,{norm}{label_filter}[{name}]"
            )
    chains.append("".join(names) + f"concat=n={len(parts)}:v=1:a=0[{label}]")
    return ";".join(chains)


def _scale(side_scale: str) -> str:
    w, _, h = side_scale.partition(":")
    return f"scale={w}:{h}:force_original_aspect_ratio=decrease,pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,setsar=1"


def aligned_export_command(
    ffmpeg: str,
    inputs: list[tuple[Path, list[Part]]],
    out: Path,
    *,
    text_file: Path | None,
    gap_text_file: Path | None,
    clock: list[str],
    font_file: Path | None,
    duration: float,
    preset: str = "veryfast",
    side_scale: str = "1280:720",
) -> list[str]:
    """Bản xuất ghép 2 camera khi clip có khe hở (G3-F2): mỗi nửa được dựng lại theo **giờ thực** — đoạn video
    đặt đúng vị trí, khe hở lấp khung đen "Không có video" — nên hai nửa thẳng hàng giờ thực từng khung và một
    đồng hồ chung (bắt đầu ở giờ đầu cửa sổ chung) đúng cho cả hai nửa. Không lấy âm thanh (đã bị cắt khúc).

    `text_file` / `gap_text_file` = None → bỏ chữ (test máy không có drawtext).
    """
    gap_label = (
        f"drawtext={_font(font_file)}textfile={_filter_path(gap_text_file)}:expansion=none"
        f":x=(w-text_w)/2:y=(h-text_h)/2:{_BOX}"
        if gap_text_file is not None
        else ""
    )
    chains = [_side_chain(i, parts, side_scale, gap_label, f"side{i}") for i, (_, parts) in enumerate(inputs)]
    overlays = list(clock)
    if text_file is not None:
        overlays.insert(
            0,
            f"drawtext={_font(font_file)}textfile={_filter_path(text_file)}:expansion=none:x=24:y=24:{_BOX}",
        )
    tail = "," + ",".join(overlays) if overlays else ""
    stacked = "".join(f"[side{i}]" for i in range(len(inputs)))
    stack = f"hstack=inputs={len(inputs)}" if len(inputs) > 1 else "null"
    graph = ";".join(chains) + f";{stacked}{stack}{tail}[v]"
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]
    for path, _ in inputs:
        cmd += ["-i", str(path)]
    cmd += [
        "-filter_complex", graph, "-map", "[v]", "-an",
        "-c:v", "libx264", "-preset", preset, "-crf", "26", "-pix_fmt", "yuv420p",
        "-profile:v", "high", "-t", f"{duration:.3f}", "-movflags", "+faststart",
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
