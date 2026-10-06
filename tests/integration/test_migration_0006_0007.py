# ruff: noqa: S608, E501 — SQL fixture ghép từ hằng id trong test (không có input ngoài), câu SQL dài
"""Migration 0006 / 0007 Phase 3 (02a §3; T-201, T-202, T-275, T-282, T-289).

DB riêng `<TEST_DATABASE_URL>_mig` như test 0003 / 0004 / rollback (fixture `mig_db` dựng ở 0005 = Phase 2).
"""

import asyncio
import os
from collections.abc import Iterator
from typing import Any

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from aicam.core.schema_guard import SCHEMA_HEAD
from aicam.core.settings import get_settings

from .conftest import TEST_DATABASE_URL, _ensure_database, _reset_schema, alembic_config

pytestmark = pytest.mark.integration

_BASE = make_url(TEST_DATABASE_URL)
MIG_URL = _BASE.set(database=f"{_BASE.database}_mig").render_as_string(hide_password=False)


def i(n: int) -> str:
    return f"01950000-0000-7000-8000-{n:012x}"


U1, U2 = i(0x101), i(0x102)
ST = i(0x201)
SHOP_A, SHOP_B = i(0x301), i(0x302)
CLAIM_SUBMITTED, CLAIM_WON, CLAIM_NOAUDIT_SUB, CLAIM_NOAUDIT_LOST, CLAIM_NEW = (
    i(0xA00 + n) for n in range(1, 6)
)

# (mã đơn, trạng thái sàn, nhóm mong đợi) — bảng Shopee 02 §5.3.
ORDER_CASES = (
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
    ("SOMETHING_NEW", "UNKNOWN"),
    (None, "UNKNOWN"),
)
RETURN_CASES = (
    ("REQUESTED", "REQUESTED"),
    ("JUDGING", "REQUESTED"),
    ("SELLER_DISPUTE", "REQUESTED"),
    ("PROCESSING", "ACCEPTED"),
    ("ACCEPTED", "ACCEPTED"),
    ("CANCELLED", "CANCELLED"),
    ("REFUND_PAID", "DONE"),
    ("CLOSED", "CLOSED"),
    ("WHATEVER", None),
    (None, None),
)


async def _exec(sql: str, params: dict[str, Any] | None = None) -> list[Any]:
    engine = create_async_engine(MIG_URL)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params or {})
            return list(result.all()) if result.returns_rows else []
    finally:
        await engine.dispose()


def run(sql: str, params: dict[str, Any] | None = None) -> list[Any]:
    return asyncio.run(_exec(sql, params))


def run_many(statements: list[str]) -> None:
    async def go() -> None:
        engine = create_async_engine(MIG_URL)
        try:
            async with engine.begin() as conn:
                for sql in statements:
                    await conn.execute(text(sql))
        finally:
            await engine.dispose()

    asyncio.run(go())


@pytest.fixture
def mig_db(migrated_database_url: str) -> Iterator[None]:
    """DB ở 0005 (Phase 2 head)."""
    asyncio.run(_ensure_database(MIG_URL))
    asyncio.run(_reset_schema(MIG_URL))
    os.environ["DATABASE_URL"] = MIG_URL
    get_settings.cache_clear()
    try:
        command.upgrade(alembic_config(), "0005")
        yield
    finally:
        os.environ["DATABASE_URL"] = migrated_database_url
        get_settings.cache_clear()


def seed_phase2_platform_data() -> None:
    """Ở 0005: 2 shop Shopee (một `DISCONNECTED`), đơn mọi trạng thái Shopee + đơn file, hồ sơ hàng hoàn mọi trạng
    thái yêu cầu trả, hồ sơ khiếu nại có / không có audit `CLAIM_UPDATE`."""
    stmts = [
        'INSERT INTO "user" (id, username, display_name, role, password_hash) VALUES '
        f"('{U1}', 'tst_p3_admin', 'Quản trị P3', 'ADMIN', 'x'), ('{U2}', 'tst_p3_cskh', 'Hoa P3', 'CSKH', 'x')",
        f"INSERT INTO station (id, name) VALUES ('{ST}', 'TST Station P3')",
        "INSERT INTO shop (id, platform, platform_shop_id, name, auth_status) VALUES "
        f"('{SHOP_A}', 'SHOPEE', '880001', 'Áo Đẹp', 'CONNECTED'), "
        f"('{SHOP_B}', 'SHOPEE', '880002', 'Áo Cũ', 'DISCONNECTED')",
    ]
    for n, (status, _) in enumerate(ORDER_CASES):
        shop = f"'{SHOP_A}'" if status is not None else "NULL"
        st = f"'{status}'" if status is not None else "NULL"
        source = "'API'" if status is not None else "'CSV'"
        stmts.append(
            'INSERT INTO "order" (id, shop_id, platform_order_sn, platform_status, source) VALUES '
            f"('{i(0x400 + n)}', {shop}, '2410P3{n:05d}', {st}, {source})"
        )
    for n, (status, _) in enumerate(RETURN_CASES):
        st = f"'{status}'" if status is not None else "NULL"
        order = f"'{i(0x400 + 6)}'" if n % 2 == 0 else f"'{i(0x400 + 11)}'"  # đơn shop A / đơn file
        stmts.append(
            "INSERT INTO return_case (id, order_id, kind, status, source, platform_return_sn, platform_status) "
            f"VALUES ('{i(0x800 + n)}', {order}, 'BUYER_RETURN', 'CANCELLED', 'PLATFORM', "
            f"{f"'RS{n}'" if status else 'NULL'}, {st})"
        )
    stmts += [
        "INSERT INTO package (id, order_id, tracking_number, warehouse_status) VALUES "
        f"('{i(0x500)}', '{i(0x406)}', 'SPXP3000001', 'DELIVERED')",
        # Hồ sơ khiếu nại: 2 có audit, 2 không có audit, 1 mới.
        "INSERT INTO claim (id, package_id, type, counterparty, status, source, updated_at) VALUES "
        f"('{CLAIM_SUBMITTED}', '{i(0x500)}', 'DAMAGED', 'PLATFORM', 'WAITING', 'MANUAL', '2026-09-20T03:00:00Z'), "
        f"('{CLAIM_WON}', '{i(0x500)}', 'MISSING_ITEM', 'PLATFORM', 'CLOSED', 'MANUAL', '2026-09-25T03:00:00Z'), "
        f"('{CLAIM_NOAUDIT_SUB}', '{i(0x500)}', 'WRONG_ITEM', 'PLATFORM', 'SUBMITTED', 'MANUAL', '2026-09-21T03:00:00Z'), "
        f"('{CLAIM_NOAUDIT_LOST}', '{i(0x500)}', 'EMPTY_BOX', 'CARRIER', 'LOST', 'MANUAL', '2026-09-22T03:00:00Z'), "
        f"('{CLAIM_NEW}', '{i(0x500)}', 'OTHER', 'CARRIER', 'NEW', 'MANUAL', '2026-09-23T03:00:00Z')",
    ]
    audits = (
        (CLAIM_SUBMITTED, "NEW", "SUBMITTED", "2026-09-10T01:00:00Z"),
        (CLAIM_SUBMITTED, "SUBMITTED", "WAITING", "2026-09-11T01:00:00Z"),
        (CLAIM_SUBMITTED, "WAITING", "SUBMITTED", "2026-09-12T01:00:00Z"),  # gửi lại — lấy lần đầu
        (CLAIM_WON, "NEW", "SUBMITTED", "2026-09-13T01:00:00Z"),
        (CLAIM_WON, "SUBMITTED", "LOST", "2026-09-14T01:00:00Z"),
        (CLAIM_WON, "LOST", "WON", "2026-09-15T01:00:00Z"),  # kháng nghị thắng — lấy lần cuối
        (CLAIM_WON, "WON", "CLOSED", "2026-09-16T01:00:00Z"),
    )
    for claim, before, after, at in audits:
        stmts.append(
            "INSERT INTO audit_log (user_id, action, object_type, object_id, at, data) VALUES "
            f"('{U2}', 'CLAIM_UPDATE', 'CLAIM', '{claim}', '{at}', "
            f"jsonb_build_object('before', jsonb_build_object('status', '{before}'), "
            f"'after', jsonb_build_object('status', '{after}')))"
        )
    run_many(stmts)


def test_0006_backfill_groups_shop_grant_and_claim_times(mig_db: None) -> None:
    """T-201: backfill nhóm trạng thái (Shopee 02 §5.3), `return_case.shop_id`, `grant_ref`, `submitted_at` /
    `result_at` (DEC-461); CHECK mới; `alembic check` khớp model."""
    seed_phase2_platform_data()
    command.upgrade(alembic_config(), "0006")

    groups = dict(
        run(
            "SELECT platform_order_sn, platform_status_group FROM \"order\" WHERE platform_order_sn LIKE '2410P3%'"
        )
    )
    assert groups == {f"2410P3{n:05d}": g for n, (_, g) in enumerate(ORDER_CASES)}
    rgroups = dict(run("SELECT id::text, platform_status_group FROM return_case"))
    assert rgroups == {i(0x800 + n): g for n, (_, g) in enumerate(RETURN_CASES)}
    shops = dict(run("SELECT id::text, shop_id::text FROM return_case"))
    assert shops == {i(0x800 + n): (SHOP_A if n % 2 == 0 else None) for n in range(len(RETURN_CASES))}
    assert run("SELECT platform_shop_id, grant_ref FROM shop ORDER BY 1") == [
        ("880001", "880001"),
        ("880002", "880002"),
    ]
    times = {
        str(r[0]): (r[1].isoformat() if r[1] else None, r[2].isoformat() if r[2] else None)
        for r in run("SELECT id, submitted_at, result_at FROM claim")
    }
    assert times == {
        CLAIM_SUBMITTED: ("2026-09-10T01:00:00+00:00", None),
        CLAIM_WON: ("2026-09-13T01:00:00+00:00", "2026-09-15T01:00:00+00:00"),
        CLAIM_NOAUDIT_SUB: ("2026-09-21T03:00:00+00:00", None),
        CLAIM_NOAUDIT_LOST: (None, "2026-09-22T03:00:00+00:00"),
        CLAIM_NEW: (None, None),
    }
    # Giá trị mặc định setting mới.
    assert run(
        "SELECT packer_name_required, refund_only_default_hours, quiet_hours_enabled, quiet_start::text, "
        "quiet_end::text, backup_enabled, backup_upload_mbps, backup_all_pack_clips, backup_restore_pending "
        "FROM setting"
    ) == [(False, 48, True, "22:00:00", "07:00:00", False, 10, False, False)]
    command.check(alembic_config())


def test_0006_checks_accept_new_values_and_reject_bad(mig_db: None) -> None:
    """CHECK mở rộng (`TIKTOK`, `MISSING`) nhận giá trị mới; CHECK mới chặn dữ liệu sai."""
    seed_phase2_platform_data()
    command.upgrade(alembic_config(), "0006")
    run(
        f"INSERT INTO shop (id, platform, platform_shop_id, auth_status) VALUES ('{i(0x303)}', 'TIKTOK', '7001', 'CONNECTED')"
    )
    for bad in (
        "UPDATE \"order\" SET platform_status_group = 'NOPE'",
        "UPDATE return_case SET platform_status_group = 'NOPE'",
        "UPDATE setting SET refund_only_default_hours = 0",
        "UPDATE setting SET quiet_start = '07:00', quiet_end = '07:00'",
        "UPDATE setting SET backup_upload_mbps = 1001",
        "INSERT INTO shop (id, platform, platform_shop_id, auth_status) VALUES (gen_random_uuid(), 'LAZADA', '1', 'CONNECTED')",
        "INSERT INTO notify_channel (id, name, type, target, events) VALUES (gen_random_uuid(), 'Kho', 'TELEGRAM', '-1', '{}')",
        "INSERT INTO backup_object (id, kind, object_key, cloud_present) VALUES (gen_random_uuid(), 'CLIP', 'k', true)",
    ):
        with pytest.raises(Exception, match=r"check constraint|violates"):
            run(bad)


def test_0006_downgrade_then_upgrade_without_phase3_data(mig_db: None) -> None:
    """Lùi 0006 trên DB không có dữ liệu Phase 3 → về đúng schema 0005; lên lại backfill lại như lần đầu."""
    seed_phase2_platform_data()
    cfg = alembic_config()
    command.upgrade(cfg, "0006")
    command.downgrade(cfg, "0005")
    assert run("SELECT version_num FROM alembic_version") == [("0005",)]
    assert run("SELECT to_regclass('share_link'), to_regclass('package_order')") == [(None, None)]
    assert run(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'order' AND column_name = 'platform_status_group'"
    ) == [(0,)]
    command.upgrade(cfg, SCHEMA_HEAD)
    assert run("SELECT count(*) FROM \"order\" WHERE platform_status_group = 'CANCEL_REQUESTED'") == [(1,)]
