"""NFR-44 (đo ở T-221): token bucket giới hạn tốc độ tải lên thật tới MinIO tạm. Chạy riêng:
`RUN_PERF=1 TEST_S3_ENDPOINT=… uv run pytest tests/integration/test_perf_backup_throttle.py -s`.

Chưa test — thiếu tài nguyên: quét p95 khi tải 5 GB qua Internet thật của kho (mạng kho, máy kho).
"""

import io
import os
import time
import uuid

import pytest
from redis import Redis

from aicam.modules.backup import transfer
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.ratelimit import TokenBucket

from .backup_fixtures import minio_settings, needs_minio
from .conftest import TEST_REDIS_URL

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perf,
    needs_minio,
    pytest.mark.skipif(os.environ.get("RUN_PERF") != "1", reason="RUN_PERF=1"),
]


@pytest.mark.parametrize(("mbps", "size_mb"), [(40, 50), (80, 100)])
def test_upload_rate_close_to_limit(mbps: int, size_mb: int) -> None:
    store = cloud.backup_store(minio_settings())
    redis = Redis.from_url(TEST_REDIS_URL)
    redis.delete("perf:upload:bucket")
    bucket = TokenBucket(redis, mbps, key="perf:upload:bucket")
    data = os.urandom(size_mb * 1_000_000)
    key = f"backup/perf/{uuid.uuid4()}.enc"
    started = time.monotonic()
    up = transfer.upload_encrypted(store, key, io.BytesIO(data), b"P" * 32, {"kind": "PERF"}, bucket.throttle)
    elapsed = time.monotonic() - started
    rate = up.encrypted_size * 8 / elapsed / 1_000_000
    print(f"NFR-44 MinIO: {size_mb} MB giới hạn {mbps} Mbit/s → {elapsed:.1f} giây, {rate:.1f} Mbit/s thực")
    store.delete(key)
    redis.close()
    assert rate <= mbps * 1.1  # không vượt giới hạn (dung lượng 1 giây đầu cho phép vượt nhẹ)
    assert rate >= mbps * 0.7  # và không chậm vô lý
