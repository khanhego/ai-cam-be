"""Nhóm trạng thái theo sàn (02 §5.3, ADR-011, BR-30, BR-31; T-203) + sổ đăng ký sàn + cờ TikTok."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from aicam.core.settings import Settings
from aicam.modules.orders.models import ORDER_STATUS_GROUPS as MODEL_ORDER_GROUPS
from aicam.modules.platforms import registry
from aicam.modules.platforms.base import (
    CANCEL_GROUPS,
    ORDER_STATUS_GROUPS,
    RETURN_STATUS_GROUPS,
    PlatformOrder,
)
from aicam.modules.platforms.service import UnconfiguredAdapter
from aicam.modules.platforms.shopee import mapping, returns_mapping
from aicam.modules.returns.models import RETURN_STATUS_GROUPS as MODEL_RETURN_GROUPS

ROOT = Path(__file__).resolve().parents[2]


def _migration_0006() -> ModuleType:
    path = ROOT / "alembic" / "versions" / "0006_phase3_schema.py"
    spec = importlib.util.spec_from_file_location("migration_0006", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("status", "group"),
    [
        ("UNPAID", "UNPAID"),
        ("READY_TO_SHIP", "AWAITING_SHIPMENT"),
        ("PROCESSED", "AWAITING_SHIPMENT"),
        ("RETRY_SHIP", "AWAITING_SHIPMENT"),
        ("SHIPPED", "SHIPPED"),
        ("TO_CONFIRM_RECEIVE", "DELIVERED"),
        ("COMPLETED", "DELIVERED"),
        ("IN_CANCEL", "CANCEL_REQUESTED"),
        ("CANCELLED", "CANCELLED"),
        ("TO_RETURN", "RETURNING"),
        ("in_cancel", "CANCEL_REQUESTED"),
        ("SOMETHING_NEW", "UNKNOWN"),
        ("", "UNKNOWN"),
        (None, "UNKNOWN"),
    ],
)
def test_shopee_order_group(status: str | None, group: str) -> None:
    assert mapping.order_group(status) == group


def test_groups_are_closed_sets() -> None:
    """Mọi nhóm adapter trả ∈ tập chung; CHECK model = hằng chung (một nguồn)."""
    assert set(mapping.ORDER_GROUPS.values()) <= set(ORDER_STATUS_GROUPS)
    assert set(returns_mapping.STATUS_GROUPS.values()) <= set(RETURN_STATUS_GROUPS)
    assert MODEL_ORDER_GROUPS == ORDER_STATUS_GROUPS
    assert MODEL_RETURN_GROUPS == RETURN_STATUS_GROUPS


def test_migration_0006_backfill_matches_shopee_mapping() -> None:
    """0006 chép bảng Shopee thành hằng (không import code app) — phải khớp `shopee/mapping` từng chữ."""
    mig = _migration_0006()
    from_migration = {s: g for g, statuses in mig.SHOPEE_ORDER_GROUPS.items() for s in statuses}
    assert from_migration == mapping.ORDER_GROUPS
    returns_from_migration = {s: g for g, statuses in mig.SHOPEE_RETURN_GROUPS.items() for s in statuses}
    assert returns_from_migration == returns_mapping.STATUS_GROUPS
    assert tuple(mig.ORDER_GROUPS) == ORDER_STATUS_GROUPS
    assert tuple(mig.RETURN_GROUPS) == RETURN_STATUS_GROUPS


@pytest.mark.parametrize("group", ORDER_STATUS_GROUPS)
def test_is_cancelled_reads_group_not_status(group: str) -> None:
    """BR-01: chặn theo nhóm (Shopee `IN_CANCEL` → `CANCEL_REQUESTED` vẫn chặn như Phase 2)."""
    order = PlatformOrder("SN", "WHATEVER", (), (), status_group=group)
    assert order.is_cancelled is (group in CANCEL_GROUPS)
    assert (
        PlatformOrder("SN", "CANCELLED", (), ()).is_cancelled is False
    )  # chữ không quyết, nhóm mặc định UNKNOWN


def _settings(**kw: object) -> Settings:
    return Settings(app_env="test", **kw)  # type: ignore[arg-type]


def test_registry_flags_and_configuration() -> None:
    off = _settings()
    assert registry.enabled_platforms(off) == []
    assert not registry.is_configured("TIKTOK", off)
    both = _settings(shopee_enabled=True, tiktok_enabled=True, tiktok_returns_enabled=True)
    assert registry.enabled_platforms(both) == ["SHOPEE", "TIKTOK"]
    assert registry.is_configured("shopee", both)  # adapter mock
    assert registry.is_configured("TIKTOK", both)
    assert registry.returns_enabled("SHOPEE", both)  # mock luôn chạy J-13 (G3 F-11)
    assert registry.returns_enabled("TIKTOK", both)
    assert not registry.returns_enabled("TIKTOK", _settings(tiktok_enabled=True))
    real = _settings(tiktok_enabled=True, tiktok_adapter="tiktok", tiktok_app_key="k")
    assert not registry.is_configured("TIKTOK", real)  # thiếu secret / service id
    assert isinstance(registry.adapter_for("TIKTOK", both), UnconfiguredAdapter)
    with pytest.raises(ValueError, match="không hỗ trợ"):
        registry.is_enabled("LAZADA", both)


PROD = {
    "app_env": "production",
    "jwt_secret": "x" * 40,
    "media_signing_key": "y" * 40,
    "fernet_key": "Zm9vYmFyYmF6cXV4cXV1eGNvcmdlZ3JhdWx0Z2FycGx5PQ==",
    "platform_adapter": "shopee",
}


def test_tiktok_settings_validator() -> None:
    """02a §9: bật TikTok ở production cần đủ 3 khóa và adapter thật; adapter lạ bị từ chối."""
    Settings(**PROD)  # type: ignore[arg-type]  # TikTok tắt → không cần khóa
    with pytest.raises(ValueError, match="TIKTOK_APP_KEY"):
        Settings(**PROD, tiktok_enabled=True, tiktok_adapter="tiktok")  # type: ignore[arg-type]
    keys = {"tiktok_app_key": "k", "tiktok_app_secret": "s", "tiktok_service_id": "1"}
    with pytest.raises(ValueError, match="TIKTOK_ADAPTER=mock"):
        Settings(**PROD, **keys, tiktok_enabled=True)  # type: ignore[arg-type]
    Settings(**PROD, **keys, tiktok_enabled=True, tiktok_adapter="tiktok")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="TIKTOK_ADAPTER phải là"):
        _settings(tiktok_adapter="lazada")
