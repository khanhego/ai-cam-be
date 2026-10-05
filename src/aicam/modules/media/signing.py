"""URL media ký HMAC-SHA256 (02 §8, 02a API-40/41/44/45). Hạn mặc định 10 phút; so sánh hằng thời gian."""

import uuid
from datetime import UTC, datetime
from urllib.parse import urlencode

from aicam.core import clock
from aicam.core.security import sign, verify_signature

EXPORT_FILES = ("video.mp4", "info.json")


def clip_message(clip_id: uuid.UUID, uid: uuid.UUID, exp: int) -> str:
    return f"clip:{clip_id}:{uid}:{exp}"


def export_message(export_id: uuid.UUID, file: str, uid: uuid.UUID, exp: int) -> str:
    return f"export:{export_id}:{file}:{uid}:{exp}"


def expiry(ttl_s: int) -> int:
    return int(clock.now().timestamp()) + ttl_s


def expires_at(exp: int) -> datetime:
    return datetime.fromtimestamp(exp, tz=UTC)


def clip_url(key: str, clip_id: uuid.UUID, uid: uuid.UUID, exp: int) -> str:
    query = urlencode({"uid": str(uid), "exp": exp, "sig": sign(key, clip_message(clip_id, uid, exp))})
    return f"/api/v1/media/clips/{clip_id}?{query}"


def export_url(key: str, export_id: uuid.UUID, file: str, uid: uuid.UUID, exp: int) -> str:
    sig = sign(key, export_message(export_id, file, uid, exp))
    query = urlencode({"uid": str(uid), "exp": exp, "sig": sig})
    return f"/api/v1/media/exports/{export_id}/{file}?{query}"


def is_valid(key: str, message: str, sig: str, exp: int) -> bool:
    """Chữ ký đúng và chưa hết hạn (giờ theo `core.clock`)."""
    return verify_signature(key, message, sig) and exp > clock.now().timestamp()
