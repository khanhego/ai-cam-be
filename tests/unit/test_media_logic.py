"""Logic thuần media: tên segment, khe hở, kế hoạch cắt, lệnh FFmpeg, chữ ký URL (02a §11)."""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aicam.core import clock
from aicam.modules.media import ffmpeg, signing
from aicam.modules.media.segments import (
    Segment,
    SegmentFile,
    candidates,
    gaps,
    is_closed,
    is_incomplete,
    parse_start,
    plan_cut,
    timeline_bounds,
)
from aicam.modules.media.service import is_first_byte

T = datetime(2026, 10, 5, 3, 0, 0, tzinfo=UTC)


def _seg(start_s: float, duration: float) -> Segment:
    return Segment(Path(f"/v/{start_s}.mp4"), T + timedelta(seconds=start_s), duration)


def test_parse_start_from_mediamtx_path() -> None:
    path = "/data/video/raw/cam-x/2026/10/05/03-59-57-468949.mp4"
    assert parse_start(path) == datetime(2026, 10, 5, 3, 59, 57, 468949, tzinfo=UTC)
    assert parse_start("/data/video/raw/cam-x/khac.mp4") is None
    assert parse_start("/raw/2026/13/40/03-59-57-000000.mp4") is None


def test_candidates_take_segment_containing_start() -> None:
    files = [SegmentFile(Path(f"{i}.mp4"), T + timedelta(seconds=60 * i), 0) for i in range(4)]
    picked = candidates(files, T + timedelta(seconds=70), T + timedelta(seconds=130))
    assert [f.path.name for f in picked] == ["1.mp4", "2.mp4"]


def test_is_closed_last_segment_by_mtime() -> None:
    files = [SegmentFile(Path("a"), T, 1000.0), SegmentFile(Path("b"), T, 1000.0)]
    assert is_closed(files, 0, 1001.0, 15)  # đã có file mới hơn
    assert not is_closed(files, 1, 1005.0, 15)  # file cuối vừa được ghi
    assert is_closed(files, 1, 1020.0, 15)  # file cuối đứng yên quá 15 giây → camera ngừng


def test_gaps_detect_missing_video() -> None:
    """TC-02.03 (logic): camera rớt 20 giây giữa phiên → thiếu đoạn → VIDEO_INCOMPLETE."""
    segs = [_seg(0, 60), _seg(80, 60)]
    missing = gaps(segs, T + timedelta(seconds=10), T + timedelta(seconds=130))
    assert missing == [(T + timedelta(seconds=60), T + timedelta(seconds=80))]
    assert is_incomplete(missing, 1.5)


def test_tiny_boundary_gap_is_tolerated() -> None:
    segs = [_seg(0, 59.9), _seg(60, 60)]
    missing = gaps(segs, T, T + timedelta(seconds=100))
    assert not is_incomplete(missing, 1.5)


def test_gap_at_start_and_end() -> None:
    segs = [_seg(30, 30)]
    missing = gaps(segs, T, T + timedelta(seconds=90))
    assert missing == [(T, T + timedelta(seconds=30)), (T + timedelta(seconds=60), T + timedelta(seconds=90))]


def test_plan_cut_across_boundary() -> None:
    plan = plan_cut([_seg(0, 60), _seg(60, 60)], T + timedelta(seconds=50), T + timedelta(seconds=70))
    assert (plan.ss, plan.to) == (50.0, 70.0)
    assert len(plan.segments) == 2


def test_plan_cut_with_gap_uses_concat_timeline() -> None:
    """Concat demuxer gộp khe hở: vị trí cuối = tổng độ dài segment trước + phần trong segment cuối."""
    plan = plan_cut([_seg(0, 60), _seg(80, 60)], T + timedelta(seconds=50), T + timedelta(seconds=100))
    assert (plan.ss, plan.to) == (50.0, 80.0)


def test_timeline_maps_clip_seconds_to_wall_clock() -> None:
    """Đầu clip lùi về keyframe (clip dài hơn 1,5 giây) và có khe hở 20 giây → 2 đoạn giờ thực đúng."""
    plan = plan_cut([_seg(0, 60), _seg(80, 60)], T + timedelta(seconds=50), T + timedelta(seconds=100))
    timeline = plan.timeline(actual_duration=31.5)
    assert timeline == [
        {"t": 0.0, "wall": (T + timedelta(seconds=48.5)).isoformat()},
        {"t": 11.5, "wall": (T + timedelta(seconds=80)).isoformat()},
    ]
    start, end = timeline_bounds(timeline, 31.5)
    assert (start, end) == (T + timedelta(seconds=48.5), T + timedelta(seconds=100))


def test_cut_command_is_stream_copy() -> None:
    cmd = ffmpeg.cut_command("ffmpeg", Path("/t/list.txt"), 1.5, 61.25, Path("/c/out.mp4"))
    assert cmd[cmd.index("-ss") + 1] == "1.500"
    assert cmd[cmd.index("-to") + 1] == "61.250"
    assert cmd.index("-ss") < cmd.index("-i")  # tùy chọn đầu vào → lùi về keyframe, phủ đủ khoảng
    assert cmd[cmd.index("-c") + 1] == "copy"


def test_concat_listing_escapes_quotes() -> None:
    assert ffmpeg.concat_listing([Path("/a/it's.mp4")]) == "file '/a/it'\\''s.mp4'\n"


def test_clock_overlay_one_piece_per_continuous_part() -> None:
    overlays = ffmpeg.clock_overlays([(0.0, 11.5, 1000), (11.5, 31.5, 1100)], None)
    assert len(overlays) == 2
    assert "gmtime\\:1000" in overlays[0]
    assert "between(t,0.000,11.500)" in overlays[0]
    assert "gmtime\\:1088" in overlays[1]  # epoch − giây bắt đầu đoạn


def test_signed_clip_url_roundtrip_and_expiry() -> None:
    """TC-02.12, TC-02.19."""
    clock.freeze(T)
    clip_id, uid, other = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    exp = signing.expiry(600)
    url = signing.clip_url("k", clip_id, uid, exp)
    sig = url.split("sig=")[1]
    assert signing.is_valid("k", signing.clip_message(clip_id, uid, exp), sig, exp)
    assert not signing.is_valid("k", signing.clip_message(clip_id, other, exp), sig, exp)  # đổi uid
    clock.advance(timedelta(minutes=11))
    assert not signing.is_valid("k", signing.clip_message(clip_id, uid, exp), sig, exp)


def test_view_audit_only_from_first_byte() -> None:
    """DEC-13."""
    assert is_first_byte(None)
    assert is_first_byte("bytes=0-")
    assert is_first_byte("bytes=0-1023")
    assert not is_first_byte("bytes=1024-")
