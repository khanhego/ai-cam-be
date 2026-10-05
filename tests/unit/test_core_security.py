import uuid
from datetime import timedelta

import jwt
import pytest
from cryptography.fernet import Fernet

from aicam.core import clock
from aicam.core.security import (
    Cipher,
    TokenError,
    decode_access_token,
    encode_access_token,
    hash_password,
    new_refresh_token,
    sha256_hex,
    sign,
    verify_password,
    verify_signature,
)

SECRET = "test-secret-0123456789abcdef0123456789"


def test_password_hash_is_argon2id_and_verifies() -> None:
    hashed = hash_password("matkhau123")

    assert hashed.startswith("$argon2id$")
    assert verify_password(hashed, "matkhau123")
    assert not verify_password(hashed, "sai-mat-khau")
    assert not verify_password("khong-phai-hash", "matkhau123")


def test_access_token_roundtrip() -> None:
    user_id, station_id = uuid.uuid4(), uuid.uuid4()

    token, expires = encode_access_token(SECRET, user_id, "STATION", station_id, minutes=15)
    claims = decode_access_token(SECRET, token)

    assert claims.user_id == user_id
    assert claims.role == "STATION"
    assert claims.station_id == station_id
    assert claims.expires_at == expires.replace(microsecond=0)


def test_access_token_expires_by_system_clock() -> None:
    token, _ = encode_access_token(SECRET, uuid.uuid4(), "CSKH", None, minutes=15)

    clock.advance(timedelta(minutes=15, seconds=1))

    with pytest.raises(TokenError, match="expired"):
        decode_access_token(SECRET, token)


def test_access_token_rejects_wrong_secret_and_type() -> None:
    token, _ = encode_access_token(SECRET, uuid.uuid4(), "ADMIN", None, minutes=15)
    other = jwt.encode({"sub": str(uuid.uuid4()), "role": "ADMIN", "exp": 9_999_999_999}, SECRET)

    with pytest.raises(TokenError):
        decode_access_token("another-secret-0123456789abcdef0123", token)
    with pytest.raises(TokenError, match="wrong token type"):
        decode_access_token(SECRET, other)


def test_refresh_token_stores_only_hash() -> None:
    token, token_hash = new_refresh_token()

    assert token != token_hash
    assert sha256_hex(token) == token_hash
    assert len(token) >= 60


def test_cipher_roundtrip() -> None:
    cipher = Cipher(Fernet.generate_key().decode())

    encrypted = cipher.encrypt("rtsp-password")

    assert b"rtsp-password" not in encrypted
    assert cipher.decrypt(encrypted) == "rtsp-password"


def test_signature_detects_tampering() -> None:
    message = "clip:0192:uid-1:1790000000"
    signature = sign("media-key", message)

    assert verify_signature("media-key", message, signature)
    assert not verify_signature("media-key", "clip:0192:uid-2:1790000000", signature)
    assert not verify_signature("other-key", message, signature)


def test_verify_dummy_takes_comparable_time() -> None:
    """Review M1 #21: username không tồn tại vẫn tốn một lần Argon2 verify."""
    import time

    from aicam.core.security import hash_password, verify_dummy, verify_password

    verify_dummy("x")  # lần đầu tạo hash giả
    real = hash_password("matkhau123")
    t0 = time.perf_counter()
    verify_password(real, "sai")
    t_real = time.perf_counter() - t0
    t0 = time.perf_counter()
    verify_dummy("sai")
    t_dummy = time.perf_counter() - t0
    assert t_dummy > t_real / 3
