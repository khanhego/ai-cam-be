"""`AICAMENC1` (02a §7.3, §11 unit `crypto`; FR-02.13, NFR-41): khứ hồi 0 B / 1 B / 4 MiB / 9 MiB; cắt
cụt, đảo khối, sửa byte, sai khóa → lỗi; dấu vân tay; chọn đúng khóa trong 2 khóa; bản mã không chứa bản rõ
/ khóa."""

import base64
import hashlib
import io
import os

import pytest

from aicam.entrypoints import cli
from aicam.modules.cloud import crypto

KEY = b"K" * 32
KEY2 = b"Q" * 32


def _enc(data: bytes, key: bytes = KEY) -> tuple[bytes, crypto.Result]:
    out = io.BytesIO()
    res = crypto.encrypt_stream(io.BytesIO(data), out, key)
    return out.getvalue(), res


@pytest.mark.parametrize("size", [0, 1, crypto.CHUNK_SIZE, 9 * 1024 * 1024])
def test_round_trip(size: int) -> None:
    data = os.urandom(size)
    blob, res = _enc(data)
    assert res.plain_size == size
    assert res.encrypted_size == len(blob)
    assert res.sha256 == hashlib.sha256(data).hexdigest()
    assert res.fingerprint == crypto.fingerprint(KEY)
    out = io.BytesIO()
    back = crypto.decrypt_stream(io.BytesIO(blob), out, {crypto.fingerprint(KEY): KEY})
    assert out.getvalue() == data
    assert back.sha256 == res.sha256


def test_encrypting_reader_small_reads_match_encrypt_stream_size() -> None:
    data = os.urandom(5 * 1024 * 1024 + 7)
    reader = crypto.EncryptingReader(io.BytesIO(data), KEY)
    parts = []
    while chunk := reader.read(1000):
        parts.append(chunk)
    blob = b"".join(parts)
    assert reader.result.encrypted_size == len(blob)
    out = io.BytesIO()
    crypto.decrypt_stream(io.BytesIO(blob), out, crypto.keyring(base64.b64encode(KEY).decode()))
    assert out.getvalue() == data


def test_fingerprint_format_and_header() -> None:
    fp = crypto.fingerprint(KEY)
    assert len(fp) == 19
    assert fp.count("-") == 3
    assert fp == fp.upper()
    blob, _ = _enc(b"x")
    header = crypto.parse_header(blob[:32])
    assert blob[:8] == b"AICAMENC"
    assert blob[8] == 1
    assert blob[9] == 0x16
    assert header.fingerprint == fp


def test_wrong_key_detected_before_writing() -> None:
    blob, _ = _enc(b"secret data")
    out = io.BytesIO()
    with pytest.raises(crypto.WrongKeyError) as err:
        crypto.decrypt_stream(io.BytesIO(blob), out, {crypto.fingerprint(KEY2): KEY2})
    assert err.value.fingerprint == crypto.fingerprint(KEY)
    assert out.getvalue() == b""


def test_picks_right_key_among_two() -> None:
    blob_a, _ = _enc(b"from-A", KEY)
    blob_b, _ = _enc(b"from-B", KEY2)
    ring = crypto.keyring(base64.b64encode(KEY2).decode(), base64.b64encode(KEY).decode())
    for blob, want in ((blob_a, b"from-A"), (blob_b, b"from-B")):
        out = io.BytesIO()
        crypto.decrypt_stream(io.BytesIO(blob), out, ring)
        assert out.getvalue() == want


def _chunks(blob: bytes) -> tuple[bytes, list[bytes]]:
    header, rest, parts = blob[:32], blob[32:], []
    while rest:
        n = int.from_bytes(rest[:4], "big")
        parts.append(rest[: 4 + n])
        rest = rest[4 + n :]
    return header, parts


def _fails(blob: bytes) -> None:
    with pytest.raises(crypto.CryptoError):
        crypto.decrypt_stream(io.BytesIO(blob), io.BytesIO(), {crypto.fingerprint(KEY): KEY})


def test_truncation_reorder_tamper_fail() -> None:
    data = os.urandom(2 * crypto.CHUNK_SIZE + 10)  # 3 khối
    blob, _ = _enc(data)
    header, parts = _chunks(blob)
    assert len(parts) == 3
    _fails(header + parts[0] + parts[1])  # bỏ khối cuối
    _fails(header + parts[1] + parts[0] + parts[2])  # đảo khối
    _fails(header + parts[0] + parts[1] + parts[2] + parts[2])  # dữ liệu thừa
    _fails(blob[:-1])  # cắt 1 byte
    tampered = bytearray(blob)
    tampered[100] ^= 0x01
    _fails(bytes(tampered))
    bad_prefix = bytearray(blob)
    bad_prefix[20] ^= 0x01  # tiền tố nonce trong header
    _fails(bytes(bad_prefix))
    _fails(b"")
    _fails(b"NOTAICAM" + blob[8:])


def test_ciphertext_hides_plaintext_and_key() -> None:
    data = b"SPXTST0000001 buyer-label " * 2000
    blob, _ = _enc(data)
    assert b"SPXTST0000001" not in blob
    assert KEY not in blob
    assert base64.b64encode(KEY) not in blob
    assert KEY.hex().encode() not in blob


def test_parse_key_and_keyring() -> None:
    with pytest.raises(ValueError, match="32 byte"):
        crypto.parse_key(base64.b64encode(b"short").decode())
    with pytest.raises(ValueError, match="base64"):
        crypto.parse_key("***")
    ring = crypto.keyring(base64.b64encode(KEY).decode(), "", [KEY2])
    assert set(ring) == {crypto.fingerprint(KEY), crypto.fingerprint(KEY2)}


def test_backup_keygen_prints_valid_key_and_fingerprint(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["backup-keygen"]) == 0
    out = capsys.readouterr().out.splitlines()
    key = out[0].split("=", 1)[1]
    assert len(crypto.parse_key(key)) == 32
    assert out[1] == f"Dấu vân tay: {crypto.fingerprint(crypto.parse_key(key))}"
