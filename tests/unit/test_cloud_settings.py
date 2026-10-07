"""Validator `core/settings.py` cho kho lưu + khóa sao lưu (02a §9, DEC-501)."""

import base64

import pytest
from pydantic import ValidationError

from aicam.core.settings import Settings

KEY = base64.b64encode(b"k" * 32).decode()
OLD = base64.b64encode(b"o" * 32).decode()
PROD = {
    "app_env": "production",
    "jwt_secret": "real-jwt-secret-0123456789abcdef0123",
    "media_signing_key": "real-media-signing-key",
    "fernet_key": base64.urlsafe_b64encode(b"f" * 32).decode(),
    "platform_adapter": "shopee",
}
S3 = {
    "s3_endpoint": "https://s3.example.vn",
    "s3_access_key_id": "ak",
    "s3_secret_access_key": "sk",
    "s3_bucket": "aicam-backup",
    "s3_share_bucket": "aicam-share",
}


def test_backup_key_must_be_32_bytes_base64() -> None:
    Settings(app_env="test", backup_encryption_key=KEY, backup_old_keys=f"{OLD}")
    with pytest.raises(ValidationError, match="BACKUP_ENCRYPTION_KEY"):
        Settings(app_env="test", backup_encryption_key=base64.b64encode(b"short").decode())
    with pytest.raises(ValidationError, match="BACKUP_OLD_KEYS"):
        Settings(app_env="test", backup_encryption_key=KEY, backup_old_keys="not-base64!!")
    with pytest.raises(ValidationError, match="khóa hiện tại"):
        Settings(app_env="test", backup_encryption_key=KEY, backup_old_keys=f"{OLD},{KEY}")


def test_production_s3_requires_two_distinct_buckets_and_keys() -> None:
    Settings(**PROD, **S3)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="hai bucket khác nhau"):
        Settings(**PROD, **{**S3, "s3_share_bucket": "aicam-backup"})  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="S3_SECRET_ACCESS_KEY"):
        Settings(**PROD, **{**S3, "s3_secret_access_key": ""})  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="localhost"):
        Settings(**PROD, **{**S3, "s3_endpoint": "http://localhost:59000"})  # type: ignore[arg-type]


def test_dev_allows_partial_s3() -> None:
    s = Settings(app_env="dev", s3_endpoint="http://minio:9000")
    assert s.s3_bucket == ""
