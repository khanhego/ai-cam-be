"""`ObjectStore` (02a §7.3, ADR-010): `S3Store` (boto3) + `MemoryStore` (test, mô phỏng versioning).

Giao diện **đồng bộ** (boto3 chặn luồng): người gọi bọc cả một thao tác (mã hóa + tải lên, tải về + giải mã)
trong `asyncio.to_thread` — không đổi luồng giữa từng khối (DEC-651).

Lỗi nhà cung cấp quy về `CloudError(code)`: `CLOUD_AUTH_FAILED` (sai khóa / không quyền), `CLOUD_UNREACHABLE`
(không kết nối / quá giờ), `CLOUD_ERROR` (khác); không thấy đối tượng → `ObjectNotFound`.
"""

from __future__ import annotations

import builtins
import io
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, BinaryIO, Protocol

AUTH_FAILED = "CLOUD_AUTH_FAILED"
UNREACHABLE = "CLOUD_UNREACHABLE"
ERROR = "CLOUD_ERROR"
PROBE_PREFIX = "backup/_probe/"
MULTIPART_CHUNK = 8 * 1024 * 1024  # 02a §7.3: multipart 8 MiB với stream không seek được

Throttle = Callable[[int], None]

MESSAGES = {
    AUTH_FAILED: "Kho lưu từ chối: sai khóa truy cập.",
    UNREACHABLE: "Không kết nối được kho lưu. Kiểm tra Internet.",
    ERROR: "Kho lưu báo lỗi.",
}


class CloudError(Exception):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or MESSAGES.get(code, code))
        self.code = code
        self.message = message or MESSAGES.get(code, code)


class ObjectNotFound(CloudError):
    def __init__(self, key: str) -> None:
        super().__init__(ERROR, "Không thấy đối tượng trên kho lưu.")
        self.key = key


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    last_modified: datetime
    metadata: dict[str, str] = field(default_factory=dict)


class ObjectStore(Protocol):
    bucket: str

    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
        throttle: Throttle | None = None,
        cache_control: str | None = None,
    ) -> int:
        """Tải lên từ luồng đọc tuần tự (không cần seek); trả số byte đã gửi."""
        ...

    def get_stream(self, key: str) -> BinaryIO: ...

    def head(self, key: str) -> ObjectInfo | None: ...

    def delete(self, key: str) -> None:
        """Bucket versioning: tạo delete marker (không xóa phiên bản — khóa ứng dụng không có quyền)."""
        ...

    def delete_prefix(self, prefix: str, *, all_versions: bool = False) -> int: ...

    def list(self, prefix: str) -> Iterator[ObjectInfo]: ...

    def presign_get(
        self, key: str, expires_s: int, *, filename: str | None = None, content_type: str | None = None
    ) -> str: ...

    def probe(self) -> int:
        """API-183: ghi → đọc → xóa một đối tượng 1 KB dưới `backup/_probe/`; trả số ms."""
        ...


class ThrottledReader(io.RawIOBase):
    """Bọc luồng đọc: mỗi lần `read` xong gọi `throttle(n)` (token bucket — NFR-44) và đếm byte."""

    def __init__(self, src: BinaryIO, throttle: Throttle | None) -> None:
        self._src = src
        self._throttle = throttle
        self.count = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        data = self._src.read(size)
        if data:
            self.count += len(data)
            if self._throttle is not None:
                self._throttle(len(data))
        return data

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def _probe(store: ObjectStore) -> int:
    started = time.monotonic()
    key = f"{PROBE_PREFIX}{uuid.uuid4()}"
    payload = b"aicam-probe-" + b"\0" * 1012  # 1 KB
    store.put_stream(key, io.BytesIO(payload), metadata={"kind": "PROBE"})
    with store.get_stream(key) as body:
        if body.read() != payload:
            raise CloudError(ERROR, "Đọc lại tệp thử không khớp.")
    store.delete(key)
    return int((time.monotonic() - started) * 1000)


# ---------------------------------------------------------------- S3 (boto3)

_AUTH_CODES = frozenset(
    {
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "AccessDenied",
        "InvalidToken",
        "ExpiredToken",
        "AuthorizationHeaderMalformed",
        "InvalidClientTokenId",
        "Forbidden",
        "403",
    }
)
_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NotFound", "404"})


def classify(exc: BaseException) -> CloudError:
    """Lỗi boto3 / botocore → `CloudError` (02a API-183)."""
    from botocore import exceptions as bx

    if isinstance(exc, CloudError):
        return exc
    if isinstance(exc, bx.ClientError):
        error = exc.response.get("Error", {})
        code = str(error.get("Code", ""))
        if code in _AUTH_CODES:
            return CloudError(AUTH_FAILED)
        message = str(error.get("Message") or code or "lỗi không rõ")[:200]
        return CloudError(ERROR, f"Kho lưu báo lỗi: {message}")
    if isinstance(
        exc,
        bx.EndpointConnectionError
        | bx.ConnectTimeoutError
        | bx.ReadTimeoutError
        | bx.ConnectionClosedError
        | bx.ConnectionError,
    ):
        return CloudError(UNREACHABLE)
    if isinstance(exc, bx.NoCredentialsError | bx.PartialCredentialsError):
        return CloudError(AUTH_FAILED)
    if isinstance(exc, TimeoutError | ConnectionError):
        return CloudError(UNREACHABLE)
    return CloudError(ERROR, f"Kho lưu báo lỗi: {type(exc).__name__}")


class S3Store:
    """boto3 S3 với `endpoint_url` (MinIO, nhà cung cấp VN, AWS…) — ADR-010."""

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        addressing_style: str = "path",
        public_endpoint: str = "",
    ) -> None:
        self.bucket = bucket
        self._endpoint = endpoint
        self._public_endpoint = public_endpoint or endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._addressing = addressing_style
        self._client = self._make_client(endpoint)
        self._signer: Any = None

    def _make_client(self, endpoint: str) -> Any:
        import boto3
        from botocore.config import Config

        config = Config(
            signature_version="s3v4",
            s3={"addressing_style": self._addressing},
            connect_timeout=5,
            read_timeout=30,
            retries={"mode": "standard", "max_attempts": 3},
        )
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=self._region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=config,
        )

    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
        throttle: Throttle | None = None,
        cache_control: str | None = None,
    ) -> int:
        from boto3.s3.transfer import TransferConfig

        reader = ThrottledReader(stream, throttle)
        config = TransferConfig(
            multipart_threshold=MULTIPART_CHUNK,
            multipart_chunksize=MULTIPART_CHUNK,
            max_concurrency=1,
            use_threads=False,
        )
        extra: dict[str, Any] = {"ContentType": content_type, "Metadata": dict(metadata or {})}
        if cache_control:
            extra["CacheControl"] = cache_control  # W1 `index.html`: `no-store` (02a §7.4)
        try:
            self._client.upload_fileobj(reader, self.bucket, key, ExtraArgs=extra, Config=config)
        except Exception as exc:
            raise classify(exc) from exc
        return reader.count

    def get_stream(self, key: str) -> BinaryIO:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            error = classify(exc)
            if _is_not_found(exc):
                raise ObjectNotFound(key) from exc
            raise error from exc
        body: BinaryIO = response["Body"]
        return body

    def head(self, key: str) -> ObjectInfo | None:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise classify(exc) from exc
        return ObjectInfo(
            key=key,
            size=int(response.get("ContentLength", 0)),
            last_modified=response["LastModified"],
            metadata=dict(response.get("Metadata", {})),
        )

    def delete(self, key: str) -> None:
        try:
            self._client.delete_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            raise classify(exc) from exc

    def delete_prefix(self, prefix: str, *, all_versions: bool = False) -> int:
        """Bucket link (J-25): `all_versions` xóa mọi phiên bản nếu nhà cung cấp buộc versioning (ADR-010)."""
        deleted = 0
        try:
            if all_versions:
                pages = self._client.get_paginator("list_object_versions").paginate(
                    Bucket=self.bucket, Prefix=prefix
                )
                for page in pages:
                    items = [
                        {"Key": v["Key"], "VersionId": v["VersionId"]}
                        for v in (*page.get("Versions", []), *page.get("DeleteMarkers", []))
                    ]
                    for i in range(0, len(items), 1000):
                        response = self._client.delete_objects(
                            Bucket=self.bucket, Delete={"Objects": items[i : i + 1000], "Quiet": True}
                        )
                        # G3-SH-3: `delete_objects` trả 200 kể cả khi từng đối tượng lỗi (`Errors[]`).
                        errors = (response or {}).get("Errors") or []
                        if errors:
                            raise CloudError(
                                ERROR,
                                f"Không xóa được {len(errors)} đối tượng ({errors[0].get('Code', '?')}).",
                            )
                        deleted += len(items[i : i + 1000])
                # G3-SH-3: kiểm lại theo phiên bản (list_objects_v2 không thấy phiên bản cũ / delete marker).
                left = self._client.list_object_versions(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
                if left.get("Versions") or left.get("DeleteMarkers"):
                    raise CloudError(ERROR, "Còn phiên bản chưa xóa dưới thư mục link — thử lại lượt sau.")
                return deleted
            for info in self.list(prefix):
                self._client.delete_object(Bucket=self.bucket, Key=info.key)
                deleted += 1
        except CloudError:
            raise
        except Exception as exc:
            raise classify(exc) from exc
        return deleted

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        """Đối tượng hiện hành (không gồm delete marker); metadata đọc riêng bằng `head` khi cần."""
        try:
            pages = self._client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
            for page in pages:
                for item in page.get("Contents", []):
                    yield ObjectInfo(
                        key=item["Key"], size=int(item["Size"]), last_modified=item["LastModified"]
                    )
        except Exception as exc:
            raise classify(exc) from exc

    def presign_get(
        self, key: str, expires_s: int, *, filename: str | None = None, content_type: str | None = None
    ) -> str:
        if self._signer is None:
            self._signer = self._make_client(self._public_endpoint)
        params: dict[str, str] = {"Bucket": self.bucket, "Key": key}
        if filename:
            params["ResponseContentDisposition"] = f'attachment; filename="{filename}"'
        if content_type:
            params["ResponseContentType"] = content_type
        url: str = self._signer.generate_presigned_url("get_object", Params=params, ExpiresIn=int(expires_s))
        return url

    def probe(self) -> int:
        return _probe(self)


def _is_not_found(exc: BaseException) -> bool:
    from botocore import exceptions as bx

    if isinstance(exc, bx.ClientError):
        error = exc.response.get("Error", {})
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return str(error.get("Code", "")) in _NOT_FOUND_CODES or status == 404
    return False


# ---------------------------------------------------------------- bộ nhớ (unit / integration test)


@dataclass
class _Version:
    version_id: str
    data: bytes | None  # None = delete marker
    metadata: dict[str, str]
    content_type: str
    last_modified: datetime
    cache_control: str | None = None


class MemoryStore:
    """Kho giả trong bộ nhớ, an toàn đa luồng, mô phỏng bucket **versioning** (02a §7.2): `delete` thêm
    delete marker, phiên bản cũ còn trong `versions(key)`. `fail` đặt lỗi cho mọi thao tác sau (mất mạng,
    sai khóa)."""

    def __init__(self, bucket: str = "memory-backup", *, versioning: bool = True) -> None:
        self.bucket = bucket
        self.versioning = versioning
        self.fail: CloudError | None = None
        self.fail_on_put_after: int | None = None  # byte — mô phỏng đứt giữa chừng
        self.calls: list[tuple[str, str]] = []
        self._objects: dict[str, list[_Version]] = {}
        self._lock = threading.Lock()
        self.now: Callable[[], datetime] = lambda: datetime.now(UTC)

    def _check(self, op: str, key: str) -> None:
        self.calls.append((op, key))
        if self.fail is not None:
            raise self.fail

    def _current(self, key: str) -> _Version | None:
        versions = self._objects.get(key)
        if not versions or versions[-1].data is None:
            return None
        return versions[-1]

    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
        throttle: Throttle | None = None,
        cache_control: str | None = None,
    ) -> int:
        self._check("put", key)
        reader = ThrottledReader(stream, throttle)
        chunks = []
        while True:
            chunk = reader.read(MULTIPART_CHUNK)
            if not chunk:
                break
            chunks.append(chunk)
            if self.fail_on_put_after is not None and reader.count >= self.fail_on_put_after:
                raise CloudError(UNREACHABLE)
        version = _Version(
            uuid.uuid4().hex, b"".join(chunks), dict(metadata or {}), content_type, self.now(), cache_control
        )
        with self._lock:
            if self.versioning:
                self._objects.setdefault(key, []).append(version)
            else:
                self._objects[key] = [version]
        return reader.count

    def get_stream(self, key: str) -> BinaryIO:
        self._check("get", key)
        with self._lock:
            current = self._current(key)
        if current is None or current.data is None:
            raise ObjectNotFound(key)
        return io.BytesIO(current.data)

    def head(self, key: str) -> ObjectInfo | None:
        self._check("head", key)
        with self._lock:
            current = self._current(key)
        if current is None or current.data is None:
            return None
        return ObjectInfo(key, len(current.data), current.last_modified, dict(current.metadata))

    def delete(self, key: str) -> None:
        self._check("delete", key)
        with self._lock:
            if self.versioning:
                if self._current(key) is not None:
                    self._objects[key].append(_Version(uuid.uuid4().hex, None, {}, "", self.now()))
            else:
                self._objects.pop(key, None)

    def delete_prefix(self, prefix: str, *, all_versions: bool = False) -> int:
        self._check("delete_prefix", prefix)
        with self._lock:
            keys = [k for k in self._objects if k.startswith(prefix)]
        if all_versions:
            with self._lock:
                for k in keys:
                    del self._objects[k]
            return len(keys)
        count = 0
        for k in keys:
            if self.head(k) is not None:
                self.delete(k)
                count += 1
        return count

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        self._check("list", prefix)
        with self._lock:
            items = [
                (k, v)
                for k, versions in sorted(self._objects.items())
                if k.startswith(prefix) and (v := versions[-1]).data is not None
            ]
        for k, v in items:
            yield ObjectInfo(k, len(v.data or b""), v.last_modified)

    def presign_get(
        self, key: str, expires_s: int, *, filename: str | None = None, content_type: str | None = None
    ) -> str:
        return f"memory://{self.bucket}/{key}?expires={int(expires_s)}"

    def probe(self) -> int:
        return _probe(self)

    # ---- chỉ test
    def versions(self, key: str) -> builtins.list[bytes | None]:
        with self._lock:
            return [v.data for v in self._objects.get(key, [])]

    def headers(self, key: str) -> tuple[str, str | None]:
        """(Content-Type, Cache-Control) bản hiện hành — test W1 (02a §7.4)."""
        with self._lock:
            current = self._current(key)
        if current is None:
            raise ObjectNotFound(key)
        return current.content_type, current.cache_control

    def keys(self, prefix: str = "") -> builtins.list[str]:
        return [o.key for o in self.list(prefix)]

    def raw(self, key: str) -> bytes:
        with self._lock:
            current = self._current(key)
        if current is None or current.data is None:
            raise ObjectNotFound(key)
        return current.data

    def tamper(self, key: str, data: bytes) -> None:
        """Ghi đè nội dung bản hiện hành (diễn tập: sửa 1 byte đối tượng cloud)."""
        with self._lock:
            current = self._current(key)
            if current is None:
                raise ObjectNotFound(key)
            current.data = data
