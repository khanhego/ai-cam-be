"""Thao tác chặn luồng (chạy trong `asyncio.to_thread` — DEC-651): mã hóa + tải lên, tải về + kiểm / giải mã.

Không đụng DB. Tải lên đi qua token bucket (`throttle`); metadata đối tượng chỉ gồm băm bản rõ, loại, id,
đường dẫn tương đối, dấu vân tay khóa — không bao giờ có khóa (NFR-41).
"""

import hashlib
import os
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import CloudError, ObjectStore, Throttle

HASH_BLOCK = 1024 * 1024


@dataclass(frozen=True)
class Uploaded:
    sha256: str  # bản rõ
    plain_size: int
    encrypted_size: int
    fingerprint: str


def sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while block := fh.read(HASH_BLOCK):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def upload_encrypted(
    store: ObjectStore,
    object_key: str,
    src: BinaryIO,
    key: bytes,
    metadata: Mapping[str, str],
    throttle: Throttle | None = None,
) -> Uploaded:
    """Mã hóa luồng `src` rồi tải lên `object_key`; HEAD kiểm `ContentLength` = số byte bản mã (DEC-466)."""
    reader = crypto.EncryptingReader(src, key)
    meta = {**metadata, "key-fp": reader.fingerprint}
    # `sha256` của bản rõ chưa biết trước khi đọc hết → người gọi truyền sẵn (tệp đã băm) hoặc bỏ trống.
    sent = store.put_stream(object_key, reader, metadata=meta, throttle=throttle)  # type: ignore[arg-type]
    result = reader.result
    head = store.head(object_key)
    if head is None or head.size != result.encrypted_size or sent != result.encrypted_size:
        raise CloudError("CLOUD_ERROR", "Kích thước đối tượng trên kho lưu không khớp sau khi tải.")
    return Uploaded(result.sha256, result.plain_size, result.encrypted_size, result.fingerprint)


def upload_file(
    store: ObjectStore,
    object_key: str,
    path: Path,
    key: bytes,
    metadata: Mapping[str, str],
    throttle: Throttle | None = None,
    *,
    expect_sha256: str | None = None,
) -> Uploaded:
    with path.open("rb") as fh:
        out = upload_encrypted(store, object_key, fh, key, metadata, throttle)
    if expect_sha256 is not None and out.sha256 != expect_sha256:
        # Tệp đổi giữa lúc băm và lúc tải (đang bị ghi) — không coi là đã sao lưu.
        raise CloudError("CLOUD_ERROR", "Tệp thay đổi trong lúc tải lên.")
    return out


def verify_object(store: ObjectStore, object_key: str, keys: Mapping[str, bytes]) -> crypto.Result:
    """Tải lại + giải mã toàn bộ, trả SHA-256 bản rõ (J-20 "kiểm đọc lại được" — DEC-466)."""
    with store.get_stream(object_key) as body:
        return crypto.verify_stream(body, keys)


def download_decrypt_to(
    store: ObjectStore,
    object_key: str,
    target: Path,
    keys: Mapping[str, bytes],
    *,
    on_header: Callable[[crypto.Header], None] | None = None,
) -> crypto.Result:
    """Tải + giải mã vào **tệp tạm** cạnh `target`, chỉ đổi tên khi xác thực xong mọi khối (DEC-518) — lỗi thì
    không để lại tệp dở. `WrongKeyError` / `CryptoError` / `CloudError` lan ra cho người gọi phân loại."""
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".part", dir=target.parent)
    tmp = Path(tmp_name)
    try:
        with store.get_stream(object_key) as body, os.fdopen(fd, "wb") as out:
            if on_header is not None:
                header = crypto.read_header(body)
                on_header(header)
                result = crypto.decrypt_stream(_Prepend(header.raw, body), out, keys)
            else:
                result = crypto.decrypt_stream(body, out, keys)
            out.flush()
            os.fsync(out.fileno())
        tmp.replace(target)
        return result
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


class _Prepend:
    """Đọc lại header đã lấy ra trước (`on_header`) rồi tới phần còn lại của luồng."""

    def __init__(self, head: bytes, rest: BinaryIO) -> None:
        self._head = head
        self._rest = rest

    def read(self, n: int = -1) -> bytes:
        if self._head:
            if n < 0:
                out, self._head = self._head + self._rest.read(), b""
                return out
            out, self._head = self._head[:n], self._head[n:]
            return out
        return self._rest.read(n)
