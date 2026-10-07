"""Định dạng mã hóa luồng `AICAMENC1` (02a §7.3, DEC-465, ADR-010; FR-02.13, NFR-41).

Header 32 byte = `b"AICAMENC"` ‖ phiên bản `0x01` ‖ log2 khối `0x16` (4 MiB) ‖ `0x0000` ‖ dấu vân tay
khóa 8 byte ‖ tiền tố nonce 8 byte ngẫu nhiên ‖ 4 byte dự phòng (0).
Mỗi khối: `len` u32 big-endian (độ dài bản mã kèm tag 16 byte) ‖ AES-256-GCM(nonce = tiền tố ‖ `counter`
u32, AAD = header ‖ `counter` u32 ‖ `final` u8). Khối cuối `final = 1` (kể cả rỗng) → cắt cụt / đảo / chèn
khối, sửa header (dấu vân tay, tiền tố) đều làm giải mã lỗi. Giải mã kiểm dấu vân tay **trước khi ghi gì**
(sai khóa → `WrongKeyError`).

Dấu vân tay = 8 byte đầu SHA-256(`"aicam-backup-key-fp:"` ‖ khóa), hiển thị `XXXX-XXXX-XXXX-XXXX`.
Khóa không bao giờ được log / ghi DB / đưa vào bản sao (chỉ dấu vân tay).
"""

import base64
import binascii
import hashlib
import os
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC = b"AICAMENC"
VERSION = 1
CHUNK_LOG2 = 22
CHUNK_SIZE = 1 << CHUNK_LOG2  # 4 MiB
HEADER_SIZE = 32
TAG_SIZE = 16
KEY_SIZE = 32
_FP_PREFIX = b"aicam-backup-key-fp:"
_HEADER = struct.Struct(">8sBBH8s8s4s")
_LEN = struct.Struct(">I")
_COUNTER = struct.Struct(">I")
MAX_CHUNKS = 2**32 - 1


class Reader(Protocol):
    def read(self, n: int = -1, /) -> bytes: ...


class Writer(Protocol):
    def write(self, data: bytes, /) -> int: ...


class CryptoError(Exception):
    """Bản mã hỏng / bị sửa / cắt cụt (`DECRYPT_FAILED` khi khôi phục — DEC-518)."""


class WrongKeyError(CryptoError):
    """Không khóa nào khớp dấu vân tay trong header (`UNKNOWN_KEY`). Chưa ghi byte nào ra đích."""

    def __init__(self, fingerprint: str) -> None:
        super().__init__(f"Khóa giải mã không khớp (dấu vân tay {fingerprint})")
        self.fingerprint = fingerprint


def parse_key(value: str) -> bytes:
    """Base64 → 32 byte (lỗi → ValueError, không lộ giá trị khóa trong thông báo)."""
    try:
        key = base64.b64decode(value.strip(), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Khóa sao lưu không phải base64 hợp lệ") from exc
    if len(key) != KEY_SIZE:
        raise ValueError("Khóa sao lưu phải đúng 32 byte (256 bit)")
    return key


def generate_key() -> str:
    """`aicam backup-keygen`: 256 bit ngẫu nhiên (base64)."""
    return base64.b64encode(os.urandom(KEY_SIZE)).decode()


def _fp_bytes(key: bytes) -> bytes:
    return hashlib.sha256(_FP_PREFIX + key).digest()[:8]


def format_fingerprint(raw: bytes) -> str:
    h = raw.hex().upper()
    return "-".join(h[i : i + 4] for i in range(0, 16, 4))


def fingerprint(key: bytes) -> str:
    """`XXXX-XXXX-XXXX-XXXX` — hiển thị ở D23 / API-180, ghi `backup_*.…key_fingerprint`."""
    return format_fingerprint(_fp_bytes(key))


def keyring(current: str, old: str = "", extra: list[bytes] | None = None) -> dict[str, bytes]:
    """`BACKUP_ENCRYPTION_KEY` + `BACKUP_OLD_KEYS` (cách dấu phẩy) + khóa `--key-file` → {dấu vân tay: khóa}
    (DEC-495: khôi phục chọn khóa theo dấu vân tay trong header từng đối tượng)."""
    keys: dict[str, bytes] = {}
    values = [current, *old.split(",")]
    for value in values:
        if value.strip():
            k = parse_key(value)
            keys[fingerprint(k)] = k
    for k in extra or []:
        keys[fingerprint(k)] = k
    return keys


@dataclass(frozen=True)
class Header:
    fingerprint: str
    nonce_prefix: bytes
    raw: bytes


def parse_header(raw: bytes) -> Header:
    if len(raw) != HEADER_SIZE:
        raise CryptoError("Không đọc đủ header AICAMENC1 (tệp rỗng / cắt cụt)")
    magic, version, log2, _flags, fp, prefix, _reserved = _HEADER.unpack(raw)
    if magic != MAGIC:
        raise CryptoError("Không phải tệp AICAMENC (sai magic)")
    if version != VERSION or log2 != CHUNK_LOG2:
        raise CryptoError(f"Phiên bản / kích thước khối AICAMENC không hỗ trợ ({version}, {log2})")
    return Header(fingerprint=format_fingerprint(fp), nonce_prefix=prefix, raw=raw)


def read_header(src: Reader) -> Header:
    return parse_header(_read_exact(src, HEADER_SIZE, allow_short=True))


def _read_exact(src: Reader, n: int, *, allow_short: bool = False) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = src.read(n - len(buf))
        if not chunk:
            break
        buf += chunk
    if len(buf) < n and not allow_short:
        raise CryptoError("Bản mã bị cắt cụt")
    return bytes(buf)


def _aad(header: bytes, counter: int, final: bool) -> bytes:
    return header + _COUNTER.pack(counter) + (b"\x01" if final else b"\x00")


@dataclass(frozen=True)
class Result:
    fingerprint: str
    sha256: str  # SHA-256 bản rõ (hex)
    plain_size: int
    encrypted_size: int


class EncryptingReader:
    """Luồng đọc trả bản mã `AICAMENC1` của `src` (để `put_stream` tải lên không cần tệp tạm).

    Sau khi đọc hết: `result` có SHA-256 + kích thước bản rõ, kích thước bản mã, dấu vân tay khóa.
    """

    def __init__(self, src: Reader, key: bytes, *, chunk_size: int = CHUNK_SIZE) -> None:
        if len(key) != KEY_SIZE:
            raise ValueError("Khóa sao lưu phải đúng 32 byte")
        if not 0 < chunk_size <= CHUNK_SIZE:
            raise ValueError("chunk_size ≤ 4 MiB")
        self._src = src
        self._aes = AESGCM(key)
        self._chunk = chunk_size
        self._fp = fingerprint(key)
        prefix = os.urandom(8)
        self._prefix = prefix
        self._header = _HEADER.pack(MAGIC, VERSION, CHUNK_LOG2, 0, _fp_bytes(key), prefix, b"\0\0\0\0")
        self._buffer = bytearray(self._header)
        self._counter = 0
        self._hash = hashlib.sha256()
        self._plain = 0
        self._encrypted = 0
        self._done = False
        self._pending = self._read_plain()

    def _read_plain(self) -> bytes:
        data = _read_exact(self._src, self._chunk, allow_short=True)
        self._hash.update(data)
        self._plain += len(data)
        return data

    def _emit(self) -> None:
        current = self._pending
        nxt = self._read_plain() if len(current) == self._chunk else b""
        final = not nxt
        if self._counter >= MAX_CHUNKS:
            raise CryptoError("Tệp quá lớn cho AICAMENC1")
        nonce = self._prefix + _COUNTER.pack(self._counter)
        sealed = self._aes.encrypt(nonce, current, _aad(self._header, self._counter, final))
        self._buffer += _LEN.pack(len(sealed)) + sealed
        self._counter += 1
        self._pending = nxt
        if final:
            self._done = True

    def read(self, size: int = -1) -> bytes:
        while not self._done and (size < 0 or len(self._buffer) < size):
            self._emit()
        if size < 0 or size >= len(self._buffer):
            out = bytes(self._buffer)
            self._buffer.clear()
        else:
            out = bytes(self._buffer[:size])
            del self._buffer[:size]
        self._encrypted += len(out)
        return out

    @property
    def fingerprint(self) -> str:
        return self._fp

    @property
    def result(self) -> Result:
        if not self._done or self._buffer:
            raise RuntimeError("EncryptingReader chưa đọc hết")
        return Result(self._fp, self._hash.hexdigest(), self._plain, self._encrypted)


def encrypt_stream(src: Reader, dst: Writer, key: bytes) -> Result:
    reader = EncryptingReader(src, key)
    while True:
        block = reader.read(CHUNK_SIZE + TAG_SIZE + 4)
        if not block:
            break
        dst.write(block)
    return reader.result


def decrypt_stream(src: Reader, dst: Writer, keys: Mapping[str, bytes]) -> Result:
    """Giải mã luồng; chọn khóa theo dấu vân tay header (DEC-495). Sai khóa → `WrongKeyError` trước khi
    ghi; hỏng / cắt cụt → `CryptoError` (có thể đã ghi một phần — khôi phục ghi vào tệp tạm rồi mới đổi tên,
    DEC-518)."""
    header = read_header(src)
    key = keys.get(header.fingerprint)
    if key is None:
        raise WrongKeyError(header.fingerprint)
    aes = AESGCM(key)
    digest = hashlib.sha256()
    plain = 0
    encrypted = HEADER_SIZE
    counter = 0
    while True:
        raw_len = _read_exact(src, _LEN.size, allow_short=True)
        if len(raw_len) < _LEN.size:
            raise CryptoError("Bản mã bị cắt cụt (thiếu khối cuối)")
        (length,) = _LEN.unpack(raw_len)
        if length < TAG_SIZE or length > CHUNK_SIZE + TAG_SIZE:
            raise CryptoError("Độ dài khối bản mã không hợp lệ")
        sealed = _read_exact(src, length)
        nonce = header.nonce_prefix + _COUNTER.pack(counter)
        try:
            data = aes.decrypt(nonce, sealed, _aad(header.raw, counter, False))
            final = False
        except InvalidTag:
            try:
                data = aes.decrypt(nonce, sealed, _aad(header.raw, counter, True))
                final = True
            except InvalidTag as exc:
                raise CryptoError("Xác thực khối thất bại (bản mã hỏng / bị sửa / sai thứ tự)") from exc
        encrypted += _LEN.size + length
        digest.update(data)
        plain += len(data)
        dst.write(data)
        counter += 1
        if final:
            if src.read(1):
                raise CryptoError("Có dữ liệu thừa sau khối cuối")
            return Result(header.fingerprint, digest.hexdigest(), plain, encrypted)


class _HashSink:
    def __init__(self) -> None:
        self.size = 0

    def write(self, data: bytes) -> int:
        self.size += len(data)
        return len(data)


def verify_stream(src: Reader, keys: Mapping[str, bytes]) -> Result:
    """Giải mã toàn bộ không ghi ra đâu — chỉ lấy SHA-256 bản rõ (J-20 kiểm đọc lại — DEC-466)."""
    return decrypt_stream(src, _HashSink(), keys)
