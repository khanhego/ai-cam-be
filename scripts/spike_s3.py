"""Spike S3 (T-5): đo cắt clip từ segment fMP4 MediaMTX bằng FFmpeg stream copy + đo encode bản xuất (AC-08).

Chỉ dùng thư viện chuẩn + ffmpeg/ffprobe để chạy được trong image `aicam-dev`:

    docker compose -f docker/compose.dev.yml run --rm --user root -v "$PWD/scripts:/spike" \
        worker python /spike/spike_s3.py --cam1 /data/video/raw/spike-cam1 --cam2 /data/video/raw/spike-cam2

Đo:
1. Danh sách segment (tên file = giờ bắt đầu UTC, ffprobe duration) và khe hở giữa các segment.
2. Cắt `[t0, t1]` qua concat demuxer `-c copy`: thời gian cắt, độ dài thực so với yêu cầu (lệch keyframe đầu).
3. Độ trễ "đóng phiên → có clip" khi cắt cả segment đang ghi (chờ t1 + settle rồi cắt).
4. Encode bản xuất 720p `veryfast` CRF 26 + drawtext: layout 1 camera và SIDE_BY_SIDE (hstack), clip 180 giây.
   Thêm nguồn tổng hợp 1080p25 H.265 (gần camera thật hơn camera giả 720p15 H.264).

Kết quả in JSON ra stdout; số liệu ghi vào 02a-be-spec.md (mục Spike S3).
"""

import argparse
import itertools
import json
import os
import re
import shutil
import statistics
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

NAME_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})-(\d{2})-(\d{2})-(\d{6})\.mp4$")
FONT = "/usr/share/fonts/truetype/bevietnampro/BeVietnamPro-SemiBold.ttf"


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603


def probe_duration(path: Path) -> float:
    out = run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)])
    return float(out.stdout.strip() or 0)


def segments(root: Path) -> list[dict[str, object]]:
    items = []
    for f in sorted(root.rglob("*.mp4")):
        m = NAME_RE.search(f.as_posix())
        if not m:
            continue
        y, mo, d, h, mi, s, us = (int(x) for x in m.groups())
        start = datetime(y, mo, d, h, mi, s, us, tzinfo=UTC)
        items.append({"path": f, "start": start})
    items.sort(key=lambda x: x["start"])  # type: ignore[arg-type,return-value]
    for item in items:
        item["duration"] = probe_duration(item["path"])  # type: ignore[arg-type]
        item["end"] = item["start"] + timedelta(seconds=item["duration"])  # type: ignore[operator]
    return items


def gaps(items: list[dict[str, object]]) -> list[float]:
    return [
        round((b["start"] - a["end"]).total_seconds(), 3)  # type: ignore[operator]
        for a, b in itertools.pairwise(items)
    ]


def cut(items: list[dict[str, object]], t0: datetime, t1: datetime, out: Path) -> dict[str, float]:
    chosen = [s for s in items if s["end"] > t0 and s["start"] < t1]  # type: ignore[operator]
    if not chosen:
        raise RuntimeError("không có segment trong khoảng")
    # Vị trí theo thời gian media đã ghép (concat bỏ khe hở): cộng dồn duration các segment trước.
    offset = 0.0
    ss = max(0.0, (t0 - chosen[0]["start"]).total_seconds())  # type: ignore[operator]
    to = 0.0
    for s in chosen:
        if s["start"] <= t1 <= s["end"]:  # type: ignore[operator]
            to = offset + (t1 - s["start"]).total_seconds()  # type: ignore[operator]
            break
        offset += float(s["duration"])  # type: ignore[arg-type]
    else:
        to = offset
    listing = out.with_suffix(".txt")
    listing.write_text("".join(f"file '{s['path']}'\n" for s in chosen))
    began = time.monotonic()
    run(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0",
            "-ss", f"{ss:.3f}", "-to", f"{to:.3f}", "-i", str(listing),
            "-map", "0", "-c", "copy", "-movflags", "+faststart", str(out),
        ]
    )  # fmt: skip
    elapsed = time.monotonic() - began
    requested = (t1 - t0).total_seconds()
    actual = probe_duration(out)
    return {
        "requested_s": round(requested, 3),
        "actual_s": round(actual, 3),
        "extra_s": round(actual - requested, 3),
        "cut_s": round(elapsed, 3),
        "segments": len(chosen),
    }


def encode(inputs: list[Path], out: Path, side_by_side: bool, offsets: list[float]) -> float:
    scale = "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1"
    text_static = "SPXTST0000001 · Đơn 2410TST00001 · TST Station 01"
    text_file = out.with_suffix(".txt")
    text_file.write_text(text_static)
    font = f"fontfile={FONT}:" if Path(FONT).exists() else ""
    epoch = int(datetime(2026, 10, 5, 1, 0, tzinfo=UTC).timestamp()) + 7 * 3600
    overlay = (
        f"drawtext={font}textfile={text_file}:expansion=none:x=24:y=24:fontsize=30:fontcolor=white:"
        "box=1:boxcolor=black@0.55:boxborderw=8,"
        f"drawtext={font}text='%{{pts\\:gmtime\\:{epoch}\\:%d/%m/%Y %H\\\\\\:%M\\\\\\:%S}}':"
        "x=24:y=72:fontsize=30:fontcolor=white:box=1:boxcolor=black@0.55:boxborderw=8"
    )
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
    for path, off in zip(inputs, offsets, strict=True):
        cmd += ["-ss", f"{off:.3f}", "-i", str(path)]
    if side_by_side:
        graph = f"[0:v]{scale}[a];[1:v]{scale}[b];[a][b]hstack=inputs=2,{overlay}[v]"
    else:
        graph = f"[0:v]{scale},{overlay}[v]"
    cmd += [
        "-filter_complex", graph, "-map", "[v]", "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-shortest", str(out),
    ]  # fmt: skip
    began = time.monotonic()
    run(cmd)
    return time.monotonic() - began


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cam1", type=Path, required=True)
    parser.add_argument("--cam2", type=Path, required=True)
    parser.add_argument("--encode-runs", type=int, default=5)
    parser.add_argument("--settle", type=float, nargs="*", default=[1.0, 2.0, 3.0])
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix="spike-s3-"))
    report: dict[str, object] = {
        "ffmpeg": run(["ffmpeg", "-version"]).stdout.splitlines()[0],
        "cpus": os.cpu_count(),
        "now_utc": datetime.now(UTC).isoformat(),
    }

    seg1, seg2 = segments(args.cam1), segments(args.cam2)
    report["segments"] = {
        "cam1": [(s["start"].isoformat(), round(float(s["duration"]), 3)) for s in seg1],  # type: ignore[attr-defined]
        "cam1_gaps_s": gaps(seg1),
        "cam2_gaps_s": gaps(seg2),
    }

    # 2. Cắt trong phần đã ghi xong: 30 giây, 120 giây, cắt ngang ranh giới segment.
    cuts = []
    closed_end = seg1[-2]["end"]  # segment cuối có thể đang ghi
    first = seg1[0]["start"]
    for length in (30, 120, 170):
        t1 = closed_end - timedelta(seconds=3)  # type: ignore[operator]
        t0 = t1 - timedelta(seconds=length)
        if t0 < first:  # type: ignore[operator]
            continue
        cuts.append({"length": length, **cut(seg1, t0, t1, work / f"cut{length}.mp4")})
    boundary = seg1[1]["start"]
    cuts.append(
        {
            "length": "boundary",
            **cut(
                seg1, boundary - timedelta(seconds=20), boundary + timedelta(seconds=20), work / "cutb.mp4"
            ),  # type: ignore[operator]
        }
    )
    report["cuts_closed"] = cuts

    # 3. Đóng phiên lúc T = now; chờ T + 5 (đệm) + settle rồi cắt [T − 65, T + 5] gồm segment đang ghi.
    latency = []
    for settle in args.settle:
        close_at = datetime.now(UTC)
        target = close_at + timedelta(seconds=5 + settle)
        time.sleep(max(0.0, (target - datetime.now(UTC)).total_seconds()))
        items = segments(args.cam1)
        began = time.monotonic()
        try:
            result = cut(
                items, close_at - timedelta(seconds=65), close_at + timedelta(seconds=5), work / "open.mp4"
            )
            ok = True
        except (subprocess.CalledProcessError, RuntimeError) as exc:
            result, ok = {"error": str(exc)[:300]}, False
        total = (datetime.now(UTC) - close_at).total_seconds()
        latency.append({"settle_s": settle, "ok": ok, "close_to_clip_s": round(total, 2),
                        "probe_and_cut_s": round(time.monotonic() - began, 2), **result})  # fmt: skip
    report["latency_open_segment"] = latency

    # 4. Encode bản xuất với clip 180 giây.
    end = min(seg1[-2]["end"], seg2[-2]["end"]) - timedelta(seconds=2)  # type: ignore[operator]
    start = end - timedelta(seconds=180)
    clip1, clip2 = work / "clip1.mp4", work / "clip2.mp4"
    report["clip180"] = {"cam1": cut(seg1, start, end, clip1), "cam2": cut(seg2, start, end, clip2)}
    timings: dict[str, list[float]] = {"single_720p15_h264": [], "sbs_720p15_h264": []}
    for i in range(args.encode_runs):
        timings["single_720p15_h264"].append(encode([clip1], work / f"e1_{i}.mp4", False, [0.0]))
        timings["sbs_720p15_h264"].append(encode([clip1, clip2], work / f"e2_{i}.mp4", True, [0.0, 0.0]))

    # Nguồn tổng hợp gần camera thật: 1080p25 H.265, 180 giây.
    synth = work / "synth1080.mp4"
    began = time.monotonic()
    run(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
            "-i", "testsrc2=size=1920x1080:rate=25", "-t", "180", "-c:v", "libx265", "-preset", "ultrafast",
            "-x265-params", "log-level=error:keyint=50", "-pix_fmt", "yuv420p", "-tag:v", "hvc1", str(synth),
        ]
    )  # fmt: skip
    report["synth_build_s"] = round(time.monotonic() - began, 1)
    timings["single_1080p25_h265"] = []
    timings["sbs_1080p25_h265"] = []
    for i in range(max(3, args.encode_runs // 2)):
        timings["single_1080p25_h265"].append(encode([synth], work / f"e3_{i}.mp4", False, [0.0]))
        timings["sbs_1080p25_h265"].append(encode([synth, synth], work / f"e4_{i}.mp4", True, [0.0, 0.0]))
    report["encode"] = {
        k: {
            "runs": len(v),
            "min_s": round(min(v), 2),
            "median_s": round(statistics.median(v), 2),
            "p95_s": round(p95(v), 2),
            "max_s": round(max(v), 2),
        }
        for k, v in timings.items()
    }
    report["export_sizes_mb"] = {
        "single_720p": round((work / "e1_0.mp4").stat().st_size / 1e6, 1),
        "sbs_720p": round((work / "e2_0.mp4").stat().st_size / 1e6, 1),
    }
    print(json.dumps(report, indent=2, default=str, ensure_ascii=False))
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
