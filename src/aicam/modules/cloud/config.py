"""Cấu hình kho lưu từ biến môi trường `S3_*` (02a §9, ADR-010) + chỗ thay kho trong test."""

from urllib.parse import urlsplit

from aicam.core.settings import Settings
from aicam.modules.cloud.store import ObjectStore, S3Store

BACKUP = "backup"
SHARE = "share"

_overrides: dict[str, ObjectStore] = {}


def is_configured(settings: Settings) -> bool:
    """Đủ endpoint, khóa truy cập và bucket sao lưu (EX-K1)."""
    return (
        all(
            v.strip()
            for v in (
                settings.s3_endpoint,
                settings.s3_bucket,
                settings.s3_access_key_id,
                settings.s3_secret_access_key,
            )
        )
        or BACKUP in _overrides
    )


def share_configured(settings: Settings) -> bool:
    """Link chia sẻ (EX-S1): đủ endpoint, khóa truy cập **và** bucket link riêng `S3_SHARE_BUCKET`.

    Không rơi về bucket sao lưu: bucket đó bật versioning và khóa ứng dụng không có quyền xóa phiên bản →
    thu hồi link không xóa thật được (NFR-42, DEC-665)."""
    return (
        all(
            v.strip()
            for v in (
                settings.s3_endpoint,
                settings.s3_share_bucket,
                settings.s3_access_key_id,
                settings.s3_secret_access_key,
            )
        )
        or SHARE in _overrides
    )


def endpoint_host(settings: Settings) -> str | None:
    if not settings.s3_endpoint.strip():
        return "memory" if BACKUP in _overrides else None
    return urlsplit(settings.s3_endpoint).netloc or settings.s3_endpoint


def _store(settings: Settings, bucket: str) -> ObjectStore:
    return S3Store(
        endpoint=settings.s3_endpoint,
        bucket=bucket,
        access_key=settings.s3_access_key_id,
        secret_key=settings.s3_secret_access_key,
        region=settings.s3_region,
        addressing_style=settings.s3_addressing_style,
        public_endpoint=settings.s3_public_endpoint,
    )


def backup_store(settings: Settings) -> ObjectStore:
    """Bucket sao lưu `S3_BUCKET` (versioning — `delete` là delete marker)."""
    if BACKUP in _overrides:
        return _overrides[BACKUP]
    return _store(settings, settings.s3_bucket)


def share_store(settings: Settings) -> ObjectStore:
    """Bucket link `S3_SHARE_BUCKET` (không versioning — thu hồi xóa thật)."""
    if SHARE in _overrides:
        return _overrides[SHARE]
    if not settings.s3_share_bucket.strip():
        raise ValueError("Chưa cấu hình S3_SHARE_BUCKET (link chia sẻ) — DEC-665")
    return _store(settings, settings.s3_share_bucket)


def use_store(role: str, store: ObjectStore | None) -> None:
    """Chỉ test: thay kho (`MemoryStore`, MinIO tạm). None → bỏ thay."""
    if store is None:
        _overrides.pop(role, None)
    else:
        _overrides[role] = store
