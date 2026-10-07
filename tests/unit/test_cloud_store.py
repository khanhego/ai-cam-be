"""`cloud.store` (02a §7.3, ADR-010): MemoryStore mô phỏng versioning, ThrottledReader, lỗi boto3."""

import io

import pytest
from botocore import exceptions as bx

from aicam.modules.cloud.store import (
    AUTH_FAILED,
    ERROR,
    PROBE_PREFIX,
    UNREACHABLE,
    CloudError,
    MemoryStore,
    ObjectNotFound,
    ThrottledReader,
    classify,
)


def test_memory_store_versioning_delete_is_marker() -> None:
    store = MemoryStore()
    store.put_stream("backup/a.enc", io.BytesIO(b"v1"), metadata={"sha256": "x"})
    store.put_stream("backup/a.enc", io.BytesIO(b"v2"))
    head = store.head("backup/a.enc")
    assert head is not None
    assert head.size == 2
    store.delete("backup/a.enc")
    assert store.head("backup/a.enc") is None
    assert store.versions("backup/a.enc") == [b"v1", b"v2", None]  # phiên bản cũ còn (delete marker)
    with pytest.raises(ObjectNotFound):
        store.get_stream("backup/a.enc")
    assert store.keys("backup/") == []


def test_memory_store_list_prefix_and_delete_prefix_all_versions() -> None:
    store = MemoryStore("share", versioning=False)
    for name in ("share/t1/index.html", "share/t1/v1.mp4", "share/t2/index.html"):
        store.put_stream(name, io.BytesIO(b"x"))
    assert store.keys("share/t1/") == ["share/t1/index.html", "share/t1/v1.mp4"]
    assert store.delete_prefix("share/t1/", all_versions=True) == 2
    assert store.keys("share/") == ["share/t2/index.html"]


def test_probe_writes_reads_deletes_under_probe_prefix() -> None:
    store = MemoryStore()
    elapsed = store.probe()
    assert elapsed >= 0
    ops = [op for op, key in store.calls if key.startswith(PROBE_PREFIX)]
    assert ops == ["put", "get", "delete"]
    assert store.keys(PROBE_PREFIX) == []


def test_probe_failure_raises_cloud_error() -> None:
    store = MemoryStore()
    store.fail = CloudError(AUTH_FAILED)
    with pytest.raises(CloudError) as err:
        store.probe()
    assert err.value.code == AUTH_FAILED


def test_throttled_reader_counts_and_calls_throttle() -> None:
    seen: list[int] = []
    reader = ThrottledReader(io.BytesIO(b"a" * 10), seen.append)
    assert reader.read(4) == b"aaaa"
    assert reader.read() == b"a" * 6
    assert reader.read() == b""
    assert reader.count == 10
    assert seen == [4, 6]


def _client_error(code: str, status: int = 400) -> bx.ClientError:
    return bx.ClientError(
        {"Error": {"Code": code, "Message": f"msg {code}"}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "PutObject",
    )


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (_client_error("InvalidAccessKeyId", 403), AUTH_FAILED),
        (_client_error("SignatureDoesNotMatch", 403), AUTH_FAILED),
        (_client_error("AccessDenied", 403), AUTH_FAILED),
        (_client_error("NoSuchBucket", 404), ERROR),
        (_client_error("SlowDown", 503), ERROR),
        (bx.EndpointConnectionError(endpoint_url="http://x"), UNREACHABLE),
        (bx.ConnectTimeoutError(endpoint_url="http://x"), UNREACHABLE),
        (bx.NoCredentialsError(), AUTH_FAILED),
        (TimeoutError(), UNREACHABLE),
        (RuntimeError("boom"), ERROR),
    ],
)
def test_classify_boto_errors(exc: BaseException, code: str) -> None:
    assert classify(exc).code == code


def test_memory_store_fail_mid_upload() -> None:
    store = MemoryStore()
    store.fail_on_put_after = 1
    with pytest.raises(CloudError) as err:
        store.put_stream("k", io.BytesIO(b"abc"))
    assert err.value.code == UNREACHABLE
    assert store.head("k") is None


def test_log_redaction_hides_s3_presign_params() -> None:
    from aicam.core.logging import redact_query

    url = "https://s3.vn/b/share/t/index.html?X-Amz-Algorithm=AWS4&X-Amz-Credential=AK%2F20261007&X-Amz-Signature=abc123"
    out = redact_query(url)
    assert "abc123" not in out
    assert "AK%2F" not in out


class _FakeS3:
    """Client boto3 giả cho `S3Store.delete_prefix(all_versions=True)` (G3-SH-3)."""

    def __init__(self, errors: list[dict[str, str]], left: bool) -> None:
        self.errors, self.left = errors, left
        self.deleted: list[str] = []

    def get_paginator(self, _name: str) -> "_FakeS3":
        return self

    def paginate(self, **_: object) -> list[dict[str, object]]:
        return [{"Versions": [{"Key": "share/x/v1.mp4", "VersionId": "1"}],
                 "DeleteMarkers": [{"Key": "share/x/v1.mp4", "VersionId": "2"}]}]  # fmt: skip

    def delete_objects(self, **kw: object) -> dict[str, object]:
        self.deleted.extend(o["VersionId"] for o in kw["Delete"]["Objects"])  # type: ignore[index]
        return {"Errors": self.errors} if self.errors else {}

    def list_object_versions(self, **_: object) -> dict[str, object]:
        return {"Versions": [{"Key": "share/x/v1.mp4", "VersionId": "3"}]} if self.left else {}


@pytest.mark.parametrize(
    ("errors", "left", "ok"),
    [
        ([], False, True),
        ([{"Key": "share/x/v1.mp4", "Code": "AccessDenied"}], False, False),
        ([], True, False),
    ],
)
def test_s3_delete_prefix_checks_errors_and_versions(
    errors: list[dict[str, str]], left: bool, ok: bool
) -> None:
    """G3-SH-3: `Errors[]` trong phản hồi `delete_objects` hoặc còn phiên bản sau khi xóa → `CloudError` (J-25
    để `pending`, không đặt `cloud_deleted_at`)."""
    from aicam.modules.cloud.store import CloudError, S3Store

    s3 = S3Store.__new__(S3Store)
    s3.bucket = "share-test"
    fake = _FakeS3(errors, left)
    s3._client = fake
    if ok:
        assert s3.delete_prefix("share/x/", all_versions=True) == 2
    else:
        with pytest.raises(CloudError):
            s3.delete_prefix("share/x/", all_versions=True)
