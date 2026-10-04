"""Mật khẩu (Argon2id), JWT, mã hóa Fernet, ký URL HMAC (02 §8, 02a §3)."""

import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from cryptography.fernet import Fernet

from aicam.core import clock

_hasher = PasswordHasher()  # mặc định argon2id


def hash_password(password: str) -> str:
    return _hasher.hash(password)


_DUMMY_HASH: str | None = None


def verify_dummy(password: str) -> None:
    """Username không tồn tại vẫn tốn một lần verify: thời gian phản hồi không lộ username (review #21)."""
    global _DUMMY_HASH
    if _DUMMY_HASH is None:
        _DUMMY_HASH = _hasher.hash("aicam-dummy-password")
    verify_password(_DUMMY_HASH, password)


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerificationError, InvalidHashError):
        return False


@dataclass(frozen=True)
class AccessClaims:
    user_id: uuid.UUID
    role: str
    station_id: uuid.UUID | None
    expires_at: datetime


def encode_access_token(
    secret: str, user_id: uuid.UUID, role: str, station_id: uuid.UUID | None, minutes: int
) -> tuple[str, datetime]:
    issued = clock.now()
    expires = issued + timedelta(minutes=minutes)
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "role": role,
        "sid": str(station_id) if station_id else None,
        "iat": int(issued.timestamp()),
        "exp": int(expires.timestamp()),
        "typ": "access",
    }
    return jwt.encode(payload, secret, algorithm="HS256"), expires


class TokenError(Exception):
    pass


def decode_access_token(secret: str, token: str) -> AccessClaims:
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            # Kiểm thời gian bằng core.clock (một nguồn giờ, test tua được), không dùng giờ hệ điều hành.
            options={
                "require": ["sub", "exp", "role"],
                "verify_exp": False,
                "verify_iat": False,
                "verify_nbf": False,
            },
        )
    except jwt.PyJWTError as exc:
        raise TokenError(str(exc)) from exc
    if payload.get("typ") != "access":
        raise TokenError("wrong token type")
    expires = datetime.fromtimestamp(int(payload["exp"]), tz=clock.now().tzinfo)
    # Kiểm hạn bằng clock của hệ thống để test tua giờ được.
    if expires <= clock.now():
        raise TokenError("expired")
    sid = payload.get("sid")
    return AccessClaims(
        user_id=uuid.UUID(payload["sub"]),
        role=str(payload["role"]),
        station_id=uuid.UUID(sid) if sid else None,
        expires_at=expires,
    )


def new_refresh_token() -> tuple[str, str]:
    """Trả (token gửi cho client, SHA-256 lưu DB)."""
    token = secrets.token_urlsafe(48)
    return token, sha256_hex(token)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class Cipher:
    """Mã hóa token sàn, mật khẩu camera (Fernet)."""

    def __init__(self, key: str) -> None:
        self._fernet = Fernet(key.encode())

    def encrypt(self, plaintext: str) -> bytes:
        return self._fernet.encrypt(plaintext.encode())

    def decrypt(self, token: bytes) -> str:
        return self._fernet.decrypt(token).decode()


def sign(key: str, message: str) -> str:
    return hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest()


def verify_signature(key: str, message: str, signature: str) -> bool:
    return hmac.compare_digest(sign(key, message), signature)
