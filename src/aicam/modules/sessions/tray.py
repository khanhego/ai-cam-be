"""Trạng thái khay Cam 2 (02 API-10 `tray`).

Tiến trình vision ghi Redis `tray:{station_id}` = `{"codes": [...], "updated_at": iso}` (TTL 5 giây, làm mới
mỗi khung) và phát `tray.changed` `{"station_id"}` khi tập mã đổi (T-12). Khóa không còn (vision dừng / mất
stream Cam 2) → `UNAVAILABLE`.
"""

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from redis.asyncio import Redis

TRAY_CHANGED_CHANNEL = "tray.changed"
TRAY_TTL_S = 5

TrayMatch = Literal["MATCH", "NOT_SEEN", "DIFFERENT", "MULTIPLE", "UNAVAILABLE"]


@dataclass(frozen=True)
class Tray:
    codes: tuple[str, ...]
    match: TrayMatch
    updated_at: datetime | None

    @property
    def blocks_close(self) -> bool:
        """BR-06: khay có mã khác → không được đóng phiên."""
        return self.match in ("DIFFERENT", "MULTIPLE")


def tray_key(station_id: uuid.UUID) -> str:
    return f"tray:{station_id}"


def compute_match(codes: tuple[str, ...] | None, expected: str | None) -> TrayMatch:
    """None = không có dữ liệu (vision dừng / camera mất) → UNAVAILABLE.

    Không có phiên: mô tả khay theo số mã (S1 không dùng tới).
    """
    if codes is None:
        return "UNAVAILABLE"
    if not codes:
        return "NOT_SEEN"
    if len(codes) > 1:
        return "MULTIPLE"
    if expected is not None and codes[0] == expected.upper():
        return "MATCH"
    return "DIFFERENT"


async def read_tray(redis: Redis, station_id: uuid.UUID, expected: str | None) -> Tray:
    raw = await redis.get(tray_key(station_id))
    if raw is None:
        return Tray((), compute_match(None, expected), None)
    data = json.loads(raw)
    codes = tuple(sorted({str(c).upper() for c in data.get("codes", [])}))
    updated = datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
    return Tray(codes, compute_match(codes, expected), updated)


async def write_tray(
    redis: Redis, station_id: uuid.UUID, codes: tuple[str, ...], updated_at: datetime, ttl_s: int = TRAY_TTL_S
) -> None:
    """Vision ghi tập mã đã khử nhiễu (giữ một định dạng với `read_tray`)."""
    payload = {"codes": sorted(codes), "updated_at": updated_at.isoformat()}
    await redis.set(tray_key(station_id), json.dumps(payload), ex=ttl_s)


async def clear_tray(redis: Redis, station_id: uuid.UUID) -> None:
    """Mất stream / bỏ camera → khay `UNAVAILABLE`."""
    await redis.delete(tray_key(station_id))


async def announce_tray_changed(redis: Redis, station_id: uuid.UUID) -> None:
    await redis.publish(TRAY_CHANGED_CHANNEL, json.dumps({"station_id": str(station_id)}))
