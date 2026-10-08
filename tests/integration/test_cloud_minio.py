"""`S3Store` trên MinIO tạm (02a §7.3, §11): PUT nhiều phần / GET / HEAD metadata / LIST / xóa = delete
marker ở bucket versioning; URL ký; khóa ứng dụng **không** xóa được phiên bản / đổi versioning (DEC-501,
RK-28).

Chỉ chạy khi có `TEST_S3_ENDPOINT` (container MinIO riêng — không phải stack dev). Nhà cung cấp S3 thật (Q20):
chưa test — thiếu tài nguyên.
"""

import io
import os
import uuid

import pytest

from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import AUTH_FAILED, CloudError, S3Store

from .backup_fixtures import S3_ENDPOINT, minio_settings, needs_minio

pytestmark = [pytest.mark.integration, needs_minio]


def _store() -> S3Store:
    store = cloud.backup_store(minio_settings())
    assert isinstance(store, S3Store)
    return store


def test_put_get_head_list_delete_marker() -> None:
    import boto3

    store = _store()
    prefix = f"backup/test/{uuid.uuid4()}/"
    payload = os.urandom(9 * 1024 * 1024)  # > 8 MiB → multipart
    sent = store.put_stream(f"{prefix}a.enc", io.BytesIO(payload), metadata={"sha256": "abc", "kind": "CLIP"})
    assert sent == len(payload)
    head = store.head(f"{prefix}a.enc")
    assert head is not None
    assert head.size == len(payload)
    assert head.metadata["sha256"] == "abc"
    with store.get_stream(f"{prefix}a.enc") as body:
        assert body.read() == payload
    assert [o.key for o in store.list(prefix)] == [f"{prefix}a.enc"]
    store.delete(f"{prefix}a.enc")
    assert store.head(f"{prefix}a.enc") is None
    root = boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=os.environ["TEST_S3_ROOT_KEY"],
        aws_secret_access_key=os.environ["TEST_S3_ROOT_SECRET"], region_name="us-east-1",
    )  # fmt: skip
    versions = root.list_object_versions(Bucket=store.bucket, Prefix=prefix)
    assert len(versions.get("Versions", [])) == 1  # bản cũ còn (khôi phục được ≤ 7 ngày)
    assert len(versions.get("DeleteMarkers", [])) == 1


def test_app_key_cannot_delete_versions_or_change_versioning() -> None:
    store = _store()
    key = f"backup/test/{uuid.uuid4()}.enc"
    store.put_stream(key, io.BytesIO(b"x"))
    client = store._client
    version = client.head_object(Bucket=store.bucket, Key=key)["VersionId"]
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as err:
        client.delete_object(Bucket=store.bucket, Key=key, VersionId=version)
    assert err.value.response["Error"]["Code"] == "AccessDenied"
    with pytest.raises(ClientError) as err2:
        client.put_bucket_versioning(Bucket=store.bucket, VersioningConfiguration={"Status": "Suspended"})
    assert err2.value.response["Error"]["Code"] == "AccessDenied"


def test_probe_and_wrong_secret() -> None:
    assert _store().probe() >= 0
    bad = cloud.backup_store(minio_settings(s3_secret_access_key="wrong-secret"))
    with pytest.raises(CloudError) as err:
        bad.probe()
    assert err.value.code == AUTH_FAILED


def test_presign_get_reads_object() -> None:
    import httpx

    store = _store()
    key = f"backup/test/{uuid.uuid4()}.txt"
    store.put_stream(key, io.BytesIO(b"hello"), content_type="text/plain")
    url = store.presign_get(key, 60)
    assert "X-Amz-Signature" in url
    assert httpx.get(url).content == b"hello"
    store.delete(key)
