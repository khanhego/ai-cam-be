"""Token bucket Redis (02a §7.3, NFR-44): tốc độ trung bình đúng giới hạn, khối lớn hơn dung lượng vẫn đi được
(nợ), ưu tiên link không chờ khi bucket còn ≥ 50 %, đặt `share:active`."""

import pytest
from redis import Redis

from aicam.modules.cloud import ratelimit

from .conftest import TEST_REDIS_URL

pytestmark = pytest.mark.integration


@pytest.fixture
def sync_redis() -> Redis:
    client = Redis.from_url(TEST_REDIS_URL)
    client.delete("test:upload:bucket", ratelimit.SHARE_ACTIVE_KEY)
    yield client
    client.delete("test:upload:bucket", ratelimit.SHARE_ACTIVE_KEY)
    client.close()


def test_rate_limit_average_matches_mbps(sync_redis: Redis) -> None:
    now = [0.0]
    slept: list[float] = []

    def sleep(s: float) -> None:
        slept.append(s)
        now[0] += s * 1000

    bucket = ratelimit.TokenBucket(
        sync_redis, 8, key="test:upload:bucket", now_ms=lambda: now[0], sleep=sleep
    )  # 8 Mbit/s = 1 MB/s
    for _ in range(10):  # 10 × 1 MB, bucket đầy 1 MB lúc đầu
        bucket.throttle(1_000_000)
    assert sum(slept) == pytest.approx(9.0, abs=0.01)  # 10 MB / 1 MB/s − 1 giây dung lượng ban đầu


def test_chunk_larger_than_capacity_goes_through_with_debt(sync_redis: Redis) -> None:
    now = [0.0]
    bucket = ratelimit.TokenBucket(sync_redis, 10, key="test:upload:bucket", now_ms=lambda: now[0])
    first = bucket.acquire(8 * 1024 * 1024)  # 8 MiB > 1,25 MB
    assert first == pytest.approx((8 * 1024 * 1024 - 1_250_000) / 1_250_000, abs=0.01)


def test_priority_skips_wait_when_half_full_and_marks_share_active(sync_redis: Redis) -> None:
    now = [0.0]
    bucket = ratelimit.TokenBucket(sync_redis, 8, key="test:upload:bucket", now_ms=lambda: now[0])
    assert bucket.acquire(2_000_000, priority=True) == 0.0  # đầy → không chờ dù vượt dung lượng
    assert bucket.acquire(100, priority=True) > 0  # đã nợ → chờ
    assert not ratelimit.share_active(sync_redis)
    bucket.priority_throttle(0)
    assert ratelimit.share_active(sync_redis)
