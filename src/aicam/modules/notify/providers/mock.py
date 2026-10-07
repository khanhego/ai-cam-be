"""`NOTIFY_TRANSPORT=mock` (02a §7.2): `RPUSH notify:mock:{TELEGRAM|ZALO_OA}` JSON `{target, text, at}`
giữ 1.000 tin gần nhất + log `notify_mock_sent` (không ghi nội dung). `NOTIFY_MOCK_FAIL=TELEGRAM` → loại
đó luôn lỗi (kiểm thử lại / bỏ)."""

import json

import structlog

from aicam.core import clock
from aicam.core.redis import get_redis
from aicam.modules.notify.providers.base import SendError

log = structlog.get_logger()

KEEP = 1000


def mock_key(channel_type: str) -> str:
    return f"notify:mock:{channel_type}"


class MockProvider:
    def __init__(self, channel_type: str, fail: bool = False) -> None:
        self.type = channel_type
        self._fail = fail

    async def send(self, target: str, text: str) -> None:
        if self._fail:
            raise SendError(f"Lỗi giả lập ({self.type}) — NOTIFY_MOCK_FAIL.", provider_code="MOCK_FAIL")
        key = mock_key(self.type)
        redis = get_redis()
        payload = json.dumps(
            {"target": target, "text": text, "at": clock.iso_z(clock.now())}, ensure_ascii=False
        )
        await redis.rpush(key, payload)  # type: ignore[misc]
        await redis.ltrim(key, -KEEP, -1)  # type: ignore[misc]
        log.info("notify_mock_sent", type=self.type, target_len=len(target), text_len=len(text))
