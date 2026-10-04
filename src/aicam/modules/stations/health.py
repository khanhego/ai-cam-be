"""Theo dõi camera online/offline (J-08, DEC-31).

Mỗi 2 giây đọc MediaMTX; 6 giây không có byte mới → OFFLINE.
"""

from dataclasses import dataclass, field

from aicam.modules.stations.mediamtx import PathStat

OFFLINE_AFTER_S = 6.0


@dataclass
class _Track:
    last_bytes: int = -1
    last_change_at: float = 0.0
    status: str | None = None


@dataclass
class HealthTracker:
    """Logic thuần (không I/O) để test được; vòng lặp ở tiến trình `vision` gọi `update()`."""

    offline_after_s: float = OFFLINE_AFTER_S
    _tracks: dict[str, _Track] = field(default_factory=dict)

    def update(self, now_s: float, paths: dict[str, PathStat], watched: list[str]) -> dict[str, str]:
        """Trả các path đổi trạng thái: {path: "ONLINE" | "OFFLINE"}."""
        changes: dict[str, str] = {}
        for name in watched:
            track = self._tracks.setdefault(name, _Track(last_change_at=now_s))
            stat = paths.get(name)
            if stat is not None and stat.inbound_bytes != track.last_bytes:
                if track.last_bytes >= 0 or stat.ready:
                    track.last_change_at = now_s
                track.last_bytes = stat.inbound_bytes
            fresh = now_s - track.last_change_at < self.offline_after_s
            online = stat is not None and stat.ready and fresh and track.last_bytes > 0
            status = "ONLINE" if online else "OFFLINE"
            if status != track.status:
                track.status = status
                changes[name] = status
        for gone in set(self._tracks) - set(watched):
            del self._tracks[gone]
        return changes
