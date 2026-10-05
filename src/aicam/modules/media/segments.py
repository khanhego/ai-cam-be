"""Segment fMP4 của MediaMTX: đọc tên file, chọn segment cho một khoảng, tính khe hở và kế hoạch cắt.

Đường dẫn ghi (docker/mediamtx.yml): `raw/<path>/YYYY/MM/DD/HH-MM-SS-ffffff.mp4`, giờ trong tên là **UTC**
lúc bắt đầu segment. Mọi hàm ở đây thuần (không I/O trừ `list_segment_files`) để unit test được.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# Spike S3: ranh giới segment MediaMTX lệch −0,09 … 0 giây → coi là liền mạch nếu lệch ≤ 0,5 giây.
_CONTIGUOUS_S = 0.5
_NAME_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})-(\d{2})-(\d{2})-(\d{6})\.mp4$")


@dataclass(frozen=True)
class SegmentFile:
    path: Path
    start: datetime
    mtime: float  # giờ sửa file (hệ điều hành), để biết segment cuối đã đóng chưa


@dataclass(frozen=True)
class Segment:
    path: Path
    start: datetime
    duration: float

    @property
    def end(self) -> datetime:
        return self.start + timedelta(seconds=self.duration)


def parse_start(path: Path | str) -> datetime | None:
    """Giờ bắt đầu (UTC) từ đường dẫn segment; None nếu không đúng mẫu tên."""
    match = _NAME_RE.search(Path(path).as_posix())
    if match is None:
        return None
    y, mo, d, h, mi, s, us = (int(x) for x in match.groups())
    try:
        return datetime(y, mo, d, h, mi, s, us, tzinfo=UTC)
    except ValueError:
        return None


def list_segment_files(camera_dir: Path, since: datetime | None = None) -> list[SegmentFile]:
    """Liệt kê segment của một camera, sắp theo giờ bắt đầu. `since`: bỏ thư mục ngày trước đó."""
    if not camera_dir.is_dir():
        return []
    out: list[SegmentFile] = []
    min_day = (since - timedelta(days=1)).date() if since else None
    for day_dir in camera_dir.glob("*/*/*"):
        if min_day is not None:
            try:
                y, m, d = (int(p) for p in day_dir.relative_to(camera_dir).parts)
                if datetime(y, m, d, tzinfo=UTC).date() < min_day:
                    continue
            except ValueError:
                continue
        for f in day_dir.glob("*.mp4"):
            start = parse_start(f)
            if start is None or (since is not None and start < since - timedelta(hours=1)):
                continue
            try:
                mtime = f.stat().st_mtime
            except FileNotFoundError:  # J-02 vừa xóa
                continue
            out.append(SegmentFile(f, start, mtime))
    out.sort(key=lambda s: s.start)
    return out


def is_closed(files: list[SegmentFile], index: int, now_ts: float, closed_after_s: float) -> bool:
    """Segment đã ghi xong: đã có segment mới hơn, hoặc file không đổi quá `closed_after_s` giây."""
    return index < len(files) - 1 or now_ts - files[index].mtime > closed_after_s


def candidates(files: list[SegmentFile], t0: datetime, t1: datetime) -> list[SegmentFile]:
    """File có thể giao [t0, t1]: file cuối cùng bắt đầu ≤ t0 và mọi file bắt đầu trong (t0, t1)."""
    before = [f for f in files if f.start <= t0]
    inside = [f for f in files if t0 < f.start < t1]
    return ([before[-1]] if before else []) + inside


def overlapping(segments: list[Segment], t0: datetime, t1: datetime) -> list[Segment]:
    return [s for s in segments if s.end > t0 and s.start < t1 and s.duration > 0]


def gaps(segments: list[Segment], t0: datetime, t1: datetime) -> list[tuple[datetime, datetime]]:
    """Các khoảng trong [t0, t1] không có video (segments đã sắp theo start)."""
    out: list[tuple[datetime, datetime]] = []
    cursor = t0
    for seg in segments:
        if seg.start > cursor:
            out.append((cursor, min(seg.start, t1)))
        cursor = max(cursor, seg.end)
        if cursor >= t1:
            break
    if cursor < t1:
        out.append((cursor, t1))
    return [(a, b) for a, b in out if b > a]


def is_incomplete(missing: list[tuple[datetime, datetime]], tolerance_s: float) -> bool:
    return any((b - a).total_seconds() > tolerance_s for a, b in missing)


@dataclass(frozen=True)
class CutPlan:
    """Vị trí cắt trên dòng thời gian đã ghép bằng concat demuxer (khe hở giữa segment bị gộp)."""

    segments: list[Segment]
    ss: float
    to: float

    @property
    def offsets(self) -> list[float]:
        acc, out = 0.0, []
        for seg in self.segments:
            out.append(acc)
            acc += seg.duration
        return out

    def timeline(self, actual_duration: float) -> list[dict[str, Any]]:
        """Ánh xạ giây trong clip → giờ thực. Đầu clip lùi về keyframe trước `ss` (stream copy)."""
        extra = min(max(0.0, actual_duration - (self.to - self.ss)), self.ss)
        start = self.ss - extra  # vị trí trên dòng ghép ứng với giây 0 của clip
        pieces: list[dict[str, Any]] = []
        for seg, offset in zip(self.segments, self.offsets, strict=True):
            seg_end = offset + seg.duration
            if seg_end <= start or offset - start >= actual_duration:
                continue
            t = max(0.0, offset - start)
            wall = seg.start + timedelta(seconds=max(0.0, start - offset))
            if pieces:
                prev_t, prev_wall = pieces[-1]["t"], datetime.fromisoformat(pieces[-1]["wall"])
                drift = (wall - prev_wall).total_seconds() - (t - prev_t)
                if abs(drift) <= _CONTIGUOUS_S:  # segment nối tiếp liền mạch → cùng một đoạn giờ
                    continue
            pieces.append({"t": round(t, 3), "wall": wall.isoformat()})
        return pieces


def plan_cut(segments: list[Segment], t0: datetime, t1: datetime) -> CutPlan:
    """Chọn segment giao [t0, t1] và tính `-ss` / `-to` trên dòng thời gian đã ghép."""
    chosen = overlapping(segments, t0, t1)
    if not chosen:
        raise ValueError("Không có segment nào trong khoảng")
    ss = max(0.0, (t0 - chosen[0].start).total_seconds())
    acc = 0.0
    to = None
    for seg in chosen:
        if t1 <= seg.end:
            to = acc + max(0.0, (t1 - seg.start).total_seconds())
            break
        acc += seg.duration
    if to is None:  # video kết thúc trước t1 (camera ngừng): cắt tới hết
        to = acc
    return CutPlan(chosen, ss, max(to, ss))


def timeline_bounds(timeline: list[dict[str, Any]], duration: float) -> tuple[datetime, datetime]:
    """Giờ thực của giây đầu và giây cuối clip."""
    first = datetime.fromisoformat(timeline[0]["wall"])
    last = timeline[-1]
    end = datetime.fromisoformat(last["wall"]) + timedelta(seconds=max(0.0, duration - float(last["t"])))
    return first, end
