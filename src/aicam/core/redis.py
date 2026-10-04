"""Kết nối Redis dùng chung (02a §2)."""

from redis.asyncio import Redis

_client: Redis | None = None


def init_redis(url: str) -> Redis:
    global _client
    _client = Redis.from_url(url, decode_responses=True)
    return _client


def get_redis() -> Redis:
    if _client is None:
        raise RuntimeError("Chưa gọi init_redis()")
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
    _client = None
