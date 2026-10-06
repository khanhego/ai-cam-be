"""Khung hình mới nhất của mỗi camera do tiến trình `vision` giữ trong Redis (T-121, NFR-32, RB-21; DEC-320).

`vision` đã đọc relay RTSP liên tục (Cam 2 đọc mã khay) — nay đọc cả Cam 1 và mỗi ~1 giây ghi JPEG mới nhất
của mọi camera vào `frame:{camera_id}` = `{"at": iso UTC, "jpeg": base64}` (TTL ngắn). API-103 / ảnh lúc đóng
gói lấy khung ≤ `SNAPSHOT_FRAME_MAX_AGE_S` thay vì mở RTSP + chờ keyframe mỗi lần; không có khung mới →
người gọi dùng đường cũ (`grab_frame` / trích từ clip) và ghi log `snapshot_frame_cache_miss`.

Redis client dùng chung `decode_responses=True` → ảnh mã hóa base64 trong JSON (≈ 1,33× kích thước, ~1 lần /
giây / camera trong LAN).
"""

import base64
import binascii
import json
import uuid
from dataclasses import dataclass
from datetime import datetime

from redis.asyncio import Redis

from aicam.core import clock

FRAME_TTL_S = 5  # khung quá cũ tự mất (vision dừng / camera rớt) — người gọi rơi về đường cũ


@dataclass(frozen=True)
class CachedFrame:
    jpeg: bytes
    taken_at: datetime


def frame_key(camera_id: uuid.UUID) -> str:
    return f"frame:{camera_id}"


async def store(redis: Redis, camera_id: uuid.UUID, jpeg: bytes, taken_at: datetime) -> None:
    payload = json.dumps({"at": clock.iso_z(taken_at), "jpeg": base64.b64encode(jpeg).decode("ascii")})
    await redis.set(frame_key(camera_id), payload, ex=FRAME_TTL_S)


async def latest(
    redis: Redis,
    camera_id: uuid.UUID,
    *,
    max_age_s: float,
    at: datetime | None = None,
    max_ahead_s: float = 0.5,
) -> CachedFrame | None:
    """Khung mới nhất nếu chụp trong `[at − max_age_s, at + max_ahead_s]` (`at` mặc định = bây giờ); hỏng /
    không có / cũ → None. `max_ahead_s`: lệch giờ nhỏ giữa máy vision và api (cùng server, NTP)."""
    raw = await redis.get(frame_key(camera_id))
    if not raw:
        return None
    try:
        data = json.loads(raw)
        taken_at = datetime.fromisoformat(str(data["at"]).replace("Z", "+00:00"))
        jpeg = base64.b64decode(data["jpeg"], validate=True)
    except (ValueError, KeyError, TypeError, binascii.Error):
        return None
    ref = at or clock.now()
    age = (ref - taken_at).total_seconds()
    if not jpeg or age > max_age_s or age < -max_ahead_s:
        return None
    return CachedFrame(jpeg, taken_at)
