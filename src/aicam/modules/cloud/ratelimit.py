"""Token bucket Redis dùng chung cho mọi lần tải lên cloud (02a §7.3, NFR-44, ADR-010 §4).

Khóa `upload:bucket` (hash `tokens`, `ts`): dung lượng = 1 giây × tốc độ; tiêu thụ ngay, cho phép "nợ" —
`acquire(n)` trả số giây phải chờ để bù phần thiếu (khối 8 MiB lớn hơn dung lượng 1,25 MB ở 10 Mbit/s vẫn
đi được, tốc độ trung bình đúng giới hạn). Job link gọi `priority=True`: không chờ khi bucket còn ≥ 50 % và
đặt `share:active` (TTL) để J-22 nhường lượt.

Chạy trong luồng tải lên (`asyncio.to_thread`) nên dùng client Redis **đồng bộ**.
"""

import time
from collections.abc import Callable

from redis import Redis

BUCKET_KEY = "upload:bucket"
SHARE_ACTIVE_KEY = "share:active"
SHARE_ACTIVE_TTL_S = 120

# KEYS[1] bucket; ARGV: tốc độ (byte / ms), dung lượng (byte), now (ms), n (byte), priority (0/1).
_LUA = """
local rate = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local priority = tonumber(ARGV[5])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil or ts == nil then tokens = cap; ts = now end
if now > ts then tokens = math.min(cap, tokens + (now - ts) * rate) end
local wait = 0
if priority == 1 and tokens >= cap / 2 then
  wait = 0
elseif tokens < n then
  wait = math.ceil((n - tokens) / rate)
end
tokens = tokens - n
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(math.max(now, ts)))
redis.call('PEXPIRE', KEYS[1], 600000)
return wait
"""


def mbps_to_bytes_per_s(mbps: int) -> float:
    return mbps * 1_000_000 / 8


class TokenBucket:
    def __init__(
        self,
        redis: Redis,
        rate_mbps: int,
        *,
        key: str = BUCKET_KEY,
        now_ms: Callable[[], float] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_mbps < 1:
            raise ValueError("rate_mbps ≥ 1")
        self._redis = redis
        self._key = key
        self.rate_bps = mbps_to_bytes_per_s(rate_mbps)
        self._now_ms = now_ms or (lambda: time.time() * 1000)
        self._sleep = sleep
        self._script = redis.register_script(_LUA)

    def acquire(self, n_bytes: int, *, priority: bool = False) -> float:
        """Tiêu `n_bytes`; trả số giây phải chờ (người gọi / `throttle` ngủ)."""
        if n_bytes <= 0:
            return 0.0
        rate_per_ms = self.rate_bps / 1000
        wait_ms = self._script(
            keys=[self._key],
            args=[rate_per_ms, self.rate_bps, int(self._now_ms()), n_bytes, 1 if priority else 0],
        )
        return float(wait_ms) / 1000

    def throttle(self, n_bytes: int) -> None:
        wait = self.acquire(n_bytes)
        if wait > 0:
            self._sleep(wait)

    def priority_throttle(self, n_bytes: int) -> None:
        mark_share_active(self._redis)
        wait = self.acquire(n_bytes, priority=True)
        if wait > 0:
            self._sleep(wait)


def mark_share_active(redis: Redis, ttl_s: int = SHARE_ACTIVE_TTL_S) -> None:
    redis.set(SHARE_ACTIVE_KEY, "1", ex=ttl_s)


def share_active(redis: Redis) -> bool:
    return bool(redis.exists(SHARE_ACTIVE_KEY))
