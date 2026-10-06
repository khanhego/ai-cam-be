# ruff: noqa: S608, E501 — SQL fixture ghép từ hằng id trong test (không có input ngoài), câu SQL dài
"""Migration 0006 / 0007 Phase 3 (02a §3; T-201, T-202, T-275, T-282, T-289).

DB riêng `<TEST_DATABASE_URL>_mig` như test 0003 / 0004 / rollback (fixture `mig_db` dựng ở 0005 = Phase 2).
"""

import asyncio
import logging
import os
from collections.abc import Iterator
from datetime import timedelta
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
    command.upgrade(alembic_config(), SCHEMA_HEAD)
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


# ---------------------------------------------------------------- T-202: 0007 + lùi / lên lại (phase3_archive)

SHOP_T1, SHOP_T2, SHOP_C = i(0x311), i(0x312), i(0x313)
O_T1, O_T2, O_C = i(0x481), i(0x482), i(0x483)
PKG_C = i(0x581)
SESS_C = i(0x681)
SHARE, CHANNEL, RUN = i(0xB01), i(0xB02), i(0xB03)

COMPARED = (
    "user", "shop", "setting", "order", "package", "session", "clip", "snapshot", "return_case", "claim",
    "claim_evidence", "package_order", "share_link", "share_item", "backup_run", "backup_object",
    "notify_channel", "notify_event", "notify_message", "notify_provider_token",
)  # fmt: skip


def dump() -> dict[str, list[Any]]:
    return {
        t: [r[0] for r in run(f'SELECT to_jsonb(x) FROM "{t}" x ORDER BY to_jsonb(x)::text')]
        for t in COMPARED
    }


def seed_phase3() -> None:
    """Ở head: shop TikTok 2 shop + shop Shopee thứ 2 kết nối sau (mới hơn shop A), đơn TikTok (không kiện), kiện gộp,
    link `FAILED`, kênh / sự kiện / tin thông báo, token Zalo, lượt + đối tượng sao lưu, setting đổi, cột phiên."""
    run_many(
        [
            "INSERT INTO shop (id, platform, platform_shop_id, name, auth_status, grant_ref, shop_cipher, region, "
            "created_at, sync_warnings) VALUES "
            f"('{SHOP_T1}', 'TIKTOK', '7001', 'Áo Đẹp Official', 'CONNECTED', 'open-1', 'cipher-1', 'VN', now(), "
            '\'[{"code": "TRACKING_OWNED_BY_OTHER_SHOP", "message": "x", "at": "2026-10-01T00:00:00Z"}]\'), '
            f"('{SHOP_T2}', 'TIKTOK', '7002', 'Áo Đẹp Kids', 'CONNECTED', 'open-1', 'cipher-2', 'VN', now(), '[]'), "
            f"('{SHOP_C}', 'SHOPEE', '880003', 'Áo Mới', 'CONNECTED', '880003', NULL, NULL, now() + interval '1 second', '[]')",
            f"UPDATE shop SET error_since = now() - interval '1 hour', access_token_enc = 'tok-a' WHERE id = '{SHOP_A}'",
            'INSERT INTO "order" (id, shop_id, platform_order_sn, platform_status, platform_status_group, source) VALUES '
            f"('{O_T1}', '{SHOP_T1}', '5761000000001', 'AWAITING_SHIPMENT', 'AWAITING_SHIPMENT', 'API'), "
            f"('{O_T2}', '{SHOP_T2}', '5761000000002', 'IN_TRANSIT', 'SHIPPED', 'API'), "
            f"('{O_C}', '{SHOP_C}', '2410C0000001', 'READY_TO_SHIP', 'AWAITING_SHIPMENT', 'API')",
            "INSERT INTO package (id, order_id, tracking_number, warehouse_status) VALUES "
            f"('{PKG_C}', '{O_C}', 'SPXP3C00001', 'PACKED')",
            f"INSERT INTO package_order (package_id, order_id) VALUES ('{PKG_C}', '{i(0x401)}')",
            "INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, "
            f"operator_name) VALUES ('{SESS_C}', 'PACK', '{PKG_C}', '{ST}', 'COMPLETED', now() - interval '1 hour', "
            "now() - interval '58 minutes', 'SPXP3C00001', 'Minh')",
            "INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, path, sha256) VALUES "
            f"('{i(0x781)}', '{SESS_C}', 'CAM1', 'READY', now() - interval '1 hour', now() - interval '58 minutes', "
            "'clips/c.mp4', 'sha-c')",
            "INSERT INTO share_link (id, status, source_type, package_id, layout, recipient, expires_at, "
            f"object_prefix, created_by, error_code) VALUES ('{SHARE}', 'FAILED', 'SESSION', '{PKG_C}', 'CAM1', "
            f"'Shipper GHN', now() + interval '3 days', 'share/tok/', '{U1}', 'RENDER_FAILED')",
            f"INSERT INTO share_item (share_id, ord, session_id) VALUES ('{SHARE}', 1, '{SESS_C}')",
            "INSERT INTO notify_channel (id, name, type, target, events, created_by) VALUES "
            f"('{CHANNEL}', 'Kho', 'TELEGRAM', '-1001', '{{N01,N03}}', '{U1}')",
            "INSERT INTO notify_event (id, code, severity, dedupe_key, occurred_at, data) VALUES "
            f"('{i(0xB11)}', 'N01', 'HIGH', 'cam:1', now(), '{{\"station\": \"S1\"}}')",
            "INSERT INTO notify_message (id, channel_id, event_code, severity, status, item_count, text) VALUES "
            f"('{i(0xB12)}', '{CHANNEL}', 'N01', 'HIGH', 'SENT', 1, 'Camera mất tín hiệu')",
            "INSERT INTO notify_provider_token (provider, access_token_enc) VALUES ('ZALO_OA', 'enc')",
            "INSERT INTO backup_run (id, trigger, status, key_fingerprint, finished_at) VALUES "
            f"('{RUN}', 'SCHEDULE', 'SUCCESS', 'fp1', now())",
            "INSERT INTO backup_object (id, kind, run_id, object_key, status, cloud_present, cloud_key_fingerprint) "
            f"VALUES ('{i(0xB21)}', 'DB_DUMP', '{RUN}', 'backup/db/x.enc', 'UPLOADED', true, 'fp1'), "
            f"('{i(0xB22)}', 'CLIP', NULL, 'backup/evidence/clips/c.enc', 'PENDING', false, NULL)",
            f"UPDATE backup_object SET clip_id = '{i(0x781)}' WHERE id = '{i(0xB22)}'",
            "UPDATE setting SET packer_name_required = true, refund_only_default_hours = 24, quiet_start = '23:00', "
            "backup_enabled = true, backup_confirmed_fingerprint = 'fp1', backup_upload_mbps = 20",
            f"UPDATE session SET cancel_cause = 'OTHER' WHERE id = '{SESS_C}'",
            f"UPDATE claim SET submitted_at = '2026-09-30T00:00:00Z' WHERE id = '{CLAIM_NEW}'",
            "INSERT INTO return_case (id, order_id, shop_id, kind, status, source, platform_return_sn, platform_status, "
            f"platform_status_group) VALUES ('{i(0x8F1)}', '{O_T1}', '{SHOP_T1}', 'REFUND_ONLY', 'NO_PARCEL', "
            "'PLATFORM', 'TT-R-1', 'RETURN_OR_REFUND_REQUEST_PENDING', 'REQUESTED')",
        ]
    )


def test_0007_codes_unique_per_shop(mig_db: None) -> None:
    """BR-29: cùng mã đơn / mã yêu cầu trả ở 2 shop được; trùng trong một shop / giữa hai đơn file bị chặn."""
    seed_phase2_platform_data()
    command.upgrade(alembic_config(), SCHEMA_HEAD)
    run(
        f"INSERT INTO shop (id, platform, platform_shop_id, auth_status) VALUES ('{SHOP_T1}', 'TIKTOK', '7001', 'CONNECTED')"
    )
    run(
        f"INSERT INTO \"order\" (id, shop_id, platform_order_sn, source) VALUES (gen_random_uuid(), '{SHOP_T1}', '2410P300001', 'API')"
    )
    run(
        "INSERT INTO return_case (id, shop_id, kind, status, source, platform_return_sn) VALUES "
        f"(gen_random_uuid(), '{SHOP_T1}', 'BUYER_RETURN', 'CANCELLED', 'PLATFORM', 'RS0')"
    )
    for bad in (
        f"INSERT INTO \"order\" (id, shop_id, platform_order_sn, source) VALUES (gen_random_uuid(), '{SHOP_A}', '2410P300001', 'API')",
        "INSERT INTO \"order\" (id, shop_id, platform_order_sn, source) VALUES (gen_random_uuid(), NULL, '2410P300011', 'CSV')",
        "INSERT INTO return_case (id, shop_id, kind, status, source, platform_return_sn) VALUES "
        f"(gen_random_uuid(), '{SHOP_A}', 'BUYER_RETURN', 'CANCELLED', 'PLATFORM', 'RS0')",
    ):
        with pytest.raises(Exception, match="duplicate key"):
            run(bad)
    assert run("SELECT count(*) FROM \"order\" WHERE platform_order_sn = '2410P300001'") == [(2,)]
    command.check(alembic_config())


def test_0007_downgrade_refused_with_duplicate_codes(mig_db: None) -> None:
    """Trùng mã giữa shop → lùi 0007 từ chối, in mã, DB nguyên vẹn (vẫn 0007)."""
    seed_phase2_platform_data()
    command.upgrade(alembic_config(), SCHEMA_HEAD)
    run(
        f"INSERT INTO shop (id, platform, platform_shop_id, auth_status) VALUES ('{SHOP_T1}', 'TIKTOK', '7001', 'CONNECTED')"
    )
    run(
        f"INSERT INTO \"order\" (id, shop_id, platform_order_sn, source) VALUES (gen_random_uuid(), '{SHOP_T1}', '2410P300001', 'API')"
    )
    before = dump()
    with pytest.raises(RuntimeError, match="2410P300001"):
        command.downgrade(alembic_config(), "0005")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]
    assert dump() == before
    assert run("SELECT to_regnamespace('phase3_archive')") == [(None,)]


def test_round_trip_phase3_data(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Lùi head → 0005 (Phase 2) với dữ liệu Phase 3 → archive đủ, Phase 2 một shop Shopee, đơn TikTok rời shop,
    kiện của shop Shopee bị ngắt tách khỏi đơn (cờ 1b) → lên lại → mọi dòng y hệt (so `to_jsonb`), archive drop."""
    cfg = alembic_config()
    seed_phase2_platform_data()
    command.upgrade(cfg, SCHEMA_HEAD)
    seed_phase3()
    before = dump()
    monkeypatch.setenv("AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS", "1")

    command.downgrade(cfg, "0005")

    assert run(f"SELECT order_id FROM package WHERE id = '{i(0x500)}'") == [
        (None,)
    ]  # đơn của shop A (bị ngắt)
    assert run(f"SELECT order_id::text FROM package WHERE id = '{PKG_C}'") == [(O_C,)]  # shop C còn kết nối

    assert run("SELECT version_num FROM alembic_version") == [("0005",)]
    assert run("SELECT to_regclass('share_link'), to_regclass('notify_channel')") == [(None, None)]
    # Phase 2: chỉ shop Shopee; shop kết nối mới nhất giữ CONNECTED, shop cũ hơn bị ngắt.
    assert run("SELECT id::text, auth_status FROM shop ORDER BY platform_shop_id") == [
        (SHOP_A, "DISCONNECTED"),
        (SHOP_B, "DISCONNECTED"),
        (SHOP_C, "CONNECTED"),
    ]
    assert run(f"SELECT shop_id FROM \"order\" WHERE id IN ('{O_T1}', '{O_T2}')") == [(None,), (None,)]
    assert run("SELECT conname FROM pg_constraint WHERE conname = 'uq_order_platform_order_sn'") == [
        ("uq_order_platform_order_sn",)
    ]
    counts = dict(run("SELECT key, value FROM phase3_archive.meta WHERE key = 'counts'"))["counts"]
    assert {k: counts[k] for k in ("tiktok_shops", "order_shop", "reconnected_shops", "share_link", "notify_message", "backup_object", "package_order", "setting_cols")} == {
        "tiktok_shops": 2, "order_shop": 2, "reconnected_shops": 1, "share_link": 1, "notify_message": 1,
        "backup_object": 2, "package_order": 1, "setting_cols": 1,
    }  # fmt: skip

    command.upgrade(cfg, SCHEMA_HEAD)

    assert run("SELECT to_regnamespace('phase3_archive')") == [(None,)]
    after = dump()
    for table in COMPARED:
        assert after[table] == before[table], table
    command.check(cfg)


def test_downgrade_refused_with_active_share(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bước 1a: link `ACTIVE` → từ chối (không đổi gì); cờ cho phép → lùi, lên lại link vẫn `ACTIVE`."""
    monkeypatch.setenv("AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS", "1")
    cfg = alembic_config()
    seed_phase2_platform_data()
    command.upgrade(cfg, SCHEMA_HEAD)
    seed_phase3()
    run(f"UPDATE share_link SET status = 'ACTIVE', error_code = NULL WHERE id = '{SHARE}'")
    before = dump()
    with pytest.raises(RuntimeError, match="link chia sẻ đang tạo / đang hoạt động"):
        command.downgrade(cfg, "0005")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]
    assert dump() == before
    monkeypatch.setenv("AICAM_DOWNGRADE_ALLOW_ACTIVE_SHARES", "1")
    command.downgrade(cfg, "0005")
    command.upgrade(cfg, SCHEMA_HEAD)
    assert run(f"SELECT status FROM share_link WHERE id = '{SHARE}'") == [("ACTIVE",)]


# ---------------------------------------------------------------- T-275: 4b, đơn ngoài, bằng chứng đã bỏ, MISSING

PKG_R = i(0x590)
S_PACK, S_A, S_W, S_N, S_D = i(0x690), i(0x691), i(0x692), i(0x693), i(0x694)
CLAIM_OPEN, CLAIM_DONE = i(0xA90), i(0xA91)
SNAP_P = i(0x790)
DETACH_ENV = "AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS"


def _session(
    sid: str,
    kind: str,
    status: str,
    minutes_ago: int,
    *,
    cancel: str | None = None,
    clip: str | None = "READY",
) -> list[str]:
    out = [
        "INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, cancel_reason) "
        f"VALUES ('{sid}', '{kind}', '{PKG_R}', '{ST}', '{status}', now() - interval '{minutes_ago} minutes', "
        f"now() - interval '{minutes_ago - 2} minutes', 'SPXP3R00001', {f"'{cancel}'" if cancel else 'NULL'})"
    ]
    if clip:
        out.append(
            "INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, path, sha256) VALUES "
            f"(gen_random_uuid(), '{sid}', 'CAM1', '{clip}', now() - interval '{minutes_ago} minutes', "
            f"now() - interval '{minutes_ago - 2} minutes', 'clips/{sid}.mp4', 'sha')"
        )
    return out


def seed_return_claims() -> None:
    """Ở 0005: kiện R (đơn shop A) đóng gói 10 ngày trước; 3 phiên RETURN: A bỏ dở có clip, W hủy `WRONG_SCAN` có
    clip, N bỏ dở không clip, D `COMPLETED` có clip; hồ sơ mở `CLAIM_OPEN` (chỉ phiên đóng gói + D), hồ sơ đóng
    `CLAIM_DONE`; 1 ảnh chụp tay của D."""
    stmts = [
        "INSERT INTO package (id, order_id, tracking_number, warehouse_status) VALUES "
        f"('{PKG_R}', '{i(0x406)}', 'SPXP3R00001', 'RETURN_RECEIVED_ISSUE')",
        *_session(S_PACK, "PACK", "COMPLETED", 14400),
        *_session(S_A, "RETURN", "ABANDONED", 300),
        *_session(S_W, "RETURN", "CANCELLED", 200, cancel="WRONG_SCAN"),
        *_session(S_N, "RETURN", "ABANDONED", 150, clip=None),
        *_session(S_D, "RETURN", "COMPLETED", 100),
        "INSERT INTO snapshot (id, session_id, kind, camera_role, taken_at, path, sha256, size_bytes) VALUES "
        f"('{SNAP_P}', '{S_D}', 'MANUAL', 'CAM1', now() - interval '99 minutes', 'snapshots/p.jpg', 's', 1)",
        "INSERT INTO claim (id, package_id, type, counterparty, status, source, version) VALUES "
        f"('{CLAIM_OPEN}', '{PKG_R}', 'EMPTY_BOX', 'PLATFORM', 'NEW', 'AUTO_RETURN', 1), "
        f"('{CLAIM_DONE}', '{PKG_R}', 'DAMAGED', 'PLATFORM', 'CLOSED', 'MANUAL', 1)",
        "INSERT INTO claim_evidence (id, claim_id, kind, session_id, snapshot_id, auto, added_by, added_at) VALUES "
        f"(gen_random_uuid(), '{CLAIM_OPEN}', 'SESSION', '{S_PACK}', NULL, true, NULL, now()), "
        f"(gen_random_uuid(), '{CLAIM_OPEN}', 'SESSION', '{S_D}', NULL, true, NULL, now()), "
        f"(gen_random_uuid(), '{CLAIM_OPEN}', 'SNAPSHOT', NULL, '{SNAP_P}', true, NULL, now())",
    ]
    run_many(stmts)


def _evidence(claim: str) -> set[tuple[str, bool]]:
    rows = run(
        "SELECT COALESCE(session_id, snapshot_id)::text, "
        "COALESCE((to_jsonb(e) ->> 'backfilled')::boolean, false) FROM claim_evidence e "
        f"WHERE claim_id = '{claim}' AND (to_jsonb(e) ->> 'removed_at') IS NULL"
    )
    return {(str(r[0]), bool(r[1])) for r in rows}


def test_backfill_prior_sessions_4b(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """4b (BR-39, DEC-498): hồ sơ mở thêm phiên A (bỏ dở có clip) `backfilled`, không thêm W (`WRONG_SCAN`),
    N (không clip); hồ sơ đóng không đổi; audit + `version + 1`. Lùi → Phase 2 giữ dòng; người dùng bỏ A khi chạy
    Phase 2 → lên lại không thêm lại; lùi / lên lần nữa không nhân đôi."""
    cfg = alembic_config()
    seed_phase2_platform_data()
    seed_return_claims()

    command.upgrade(cfg, SCHEMA_HEAD)

    assert _evidence(CLAIM_OPEN) == {(S_PACK, False), (S_D, False), (SNAP_P, False), (S_A, True)}
    assert run(f"SELECT count(*) FROM claim_evidence WHERE claim_id = '{CLAIM_DONE}'") == [(0,)]
    assert run(f"SELECT version FROM claim WHERE id = '{CLAIM_OPEN}'") == [(2,)]
    audit = run(
        "SELECT object_id, data FROM audit_log WHERE action = 'CLAIM_EVIDENCE_UPDATE' AND data->>'reason' = 'BACKFILL_BR39'"
    )
    assert audit == [(CLAIM_OPEN, {"reason": "BACKFILL_BR39", "session_ids": [S_A]})]

    monkeypatch.setenv(DETACH_ENV, "1")
    command.downgrade(cfg, "0005")
    command.upgrade(cfg, SCHEMA_HEAD)
    assert _evidence(CLAIM_OPEN) == {(S_PACK, False), (S_D, False), (SNAP_P, False), (S_A, True)}
    assert run(f"SELECT version FROM claim WHERE id = '{CLAIM_OPEN}'") == [(2,)]

    command.downgrade(cfg, "0005")
    run(
        f"DELETE FROM claim_evidence WHERE claim_id = '{CLAIM_OPEN}' AND session_id = '{S_A}'"
    )  # Phase 2: bỏ A
    command.upgrade(cfg, SCHEMA_HEAD)
    assert _evidence(CLAIM_OPEN) == {(S_PACK, False), (S_D, False), (SNAP_P, False)}


def test_downgrade_refused_with_foreign_order_packages(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bước 1b (DEC-509): kiện của đơn TikTok / shop Shopee bị ngắt → từ chối, in số kiện theo trạng thái, DB
    nguyên; cờ → tách kiện, lên lại gắn lại (kiện đã được gắn đơn khác khi chạy Phase 2 → giữ)."""
    cfg = alembic_config()
    seed_phase2_platform_data()
    command.upgrade(cfg, SCHEMA_HEAD)
    seed_phase3()
    run(
        f"INSERT INTO package (id, order_id, tracking_number, warehouse_status) VALUES ('{i(0x5A1)}', '{O_T1}', 'TT0001', 'PACKED')"
    )
    run(
        f"INSERT INTO package (id, order_id, tracking_number, warehouse_status) VALUES ('{i(0x5A2)}', '{O_T2}', 'TT0002', 'HANDED_OVER')"
    )
    before = dump()
    with pytest.raises(RuntimeError, match=r"DELIVERED: 1, HANDED_OVER: 1, PACKED: 1"):
        command.downgrade(cfg, "0005")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]
    assert dump() == before

    monkeypatch.setenv(DETACH_ENV, "1")
    command.downgrade(cfg, "0005")
    assert run(
        f"SELECT count(*) FROM package WHERE id IN ('{i(0x5A1)}', '{i(0x5A2)}', '{i(0x500)}') AND order_id IS NULL"
    ) == [(3,)]
    run(f"UPDATE package SET order_id = '{i(0x401)}' WHERE id = '{i(0x5A2)}'")  # Phase 2 gắn tay đơn khác
    command.upgrade(cfg, SCHEMA_HEAD)
    assert dict(run(f"SELECT id::text, order_id::text FROM package WHERE id IN ('{i(0x5A1)}', '{i(0x5A2)}', '{i(0x500)}')")) == {
        i(0x5A1): O_T1, i(0x5A2): i(0x401), i(0x500): i(0x406)
    }  # fmt: skip


def _phase2_j02_candidates(now: str, days: int) -> set[str]:
    """Ứng viên J-02 của code Phase 2 (`main`: `retention_clip_query` + `media.protection`) với đồng hồ `now`."""
    protected = f"""
    SELECT ce.session_id FROM claim_evidence ce JOIN claim c ON c.id = ce.claim_id
    WHERE ce.kind = 'SESSION' AND (c.status <> 'CLOSED' OR c.closed_at >= TIMESTAMPTZ '{now}' - interval '{days} days')
    """
    return {
        str(r[0])
        for r in run(
            "SELECT k.id FROM clip k WHERE NOT k.held AND k.status = 'READY' "
            f"AND k.end_at < TIMESTAMPTZ '{now}' - interval '{days} days' AND k.session_id NOT IN ({protected})"
        )
    }


def _phase2_snapshot_protected(now: str, days: int) -> set[str]:
    return {
        str(r[0])
        for r in run(
            "SELECT ce.snapshot_id FROM claim_evidence ce JOIN claim c ON c.id = ce.claim_id WHERE ce.kind = 'SNAPSHOT' "
            f"AND (c.status <> 'CLOSED' OR c.closed_at >= TIMESTAMPTZ '{now}' - interval '{days} days')"
        )
    }


def test_removed_evidence_kept_as_legacy_hold_then_restored(
    mig_db: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Bước 5–6 (DEC-497): phiên đóng gói + ảnh đã bỏ khỏi hồ sơ mở còn hạn giữ → hồ sơ hệ thống `LEGACY_HOLD`
    `CLOSED` (`closed_at` = lúc bỏ); J-02 **Phase 2** không xóa clip / ảnh trước hạn, xóa sau hạn; bỏ quá hạn →
    không giữ. Lên lại: dòng đã bỏ trở lại nguyên (người, lý do, giờ), hồ sơ hệ thống xóa; người dùng thêm lại ở
    Phase 2 → giữ dòng đó, log `restore_removed_conflict`."""
    cfg = alembic_config()
    seed_phase2_platform_data()
    seed_return_claims()
    run("UPDATE setting SET retention_clip_days = 90")
    command.upgrade(cfg, SCHEMA_HEAD)
    run(
        f"UPDATE clip SET end_at = now() - interval '100 days', start_at = now() - interval '101 days' WHERE session_id = '{S_PACK}'"
    )
    run(
        f"UPDATE claim_evidence SET removed_at = now() - interval '1 day', removed_by = '{U2}', removed_reason = 'Nhầm kiện' "
        f"WHERE claim_id = '{CLAIM_OPEN}' AND (session_id = '{S_PACK}' OR snapshot_id = '{SNAP_P}')"
    )
    run(  # bỏ từ 200 ngày trước, clip cũ → quá hạn giữ, không vào hồ sơ hệ thống
        f"UPDATE claim_evidence SET removed_at = now() - interval '200 days', removed_by = '{U2}', removed_reason = 'Cũ' "
        f"WHERE claim_id = '{CLAIM_OPEN}' AND session_id = '{S_A}'"
    )
    run(f"UPDATE clip SET end_at = now() - interval '300 days' WHERE session_id = '{S_A}'")
    removed_before = run(
        "SELECT id::text, session_id::text, snapshot_id::text, removed_at, removed_by::text, removed_reason, backfilled "
        "FROM claim_evidence WHERE removed_at IS NOT NULL ORDER BY id"
    )
    monkeypatch.setenv(DETACH_ENV, "1")

    command.downgrade(cfg, "0005")

    legacy = run(
        "SELECT id::text, status, closed_at = (SELECT max(removed_at) FROM phase3_archive.claim_evidence_cols "
        "WHERE removed_at > now() - interval '30 days'), close_reason FROM claim WHERE source = 'LEGACY_HOLD'"
    )
    assert len(legacy) == 1
    legacy_id, status, closed_ok, reason = legacy[0]
    assert (status, closed_ok) == ("CLOSED", True)
    assert reason.startswith("Bằng chứng đã bỏ — giữ tới ")
    assert {(r[0], r[1]) for r in run(f"SELECT kind, COALESCE(session_id, snapshot_id)::text FROM claim_evidence WHERE claim_id = '{legacy_id}'")} == {
        ("SESSION", S_PACK), ("SNAPSHOT", SNAP_P)
    }  # fmt: skip
    notes = [r[0] for r in run(f"SELECT text FROM claim_note WHERE claim_id = '{legacy_id}'")]
    assert len(notes) == 2
    assert all("(lý do: Nhầm kiện)" in n and n.startswith("Bằng chứng đã bỏ khỏi KN-") for n in notes)
    assert run(
        "SELECT count(*) FROM claim_evidence WHERE claim_id = :c AND session_id = :s",
        {"c": CLAIM_OPEN, "s": S_A},
    ) == [(0,)]
    pack_clips = {str(r[0]) for r in run(f"SELECT id FROM clip WHERE session_id = '{S_PACK}'")}
    now = run("SELECT now()")[0][0]
    today = now.isoformat()
    assert not pack_clips & _phase2_j02_candidates(today, 90)
    assert SNAP_P in _phase2_snapshot_protected(today, 90)
    later = (now + timedelta(days=91)).isoformat()  # bỏ 1 ngày trước + 90 ngày → hết hạn sau 89 ngày
    assert pack_clips <= _phase2_j02_candidates(later, 90)
    assert SNAP_P not in _phase2_snapshot_protected(later, 90)

    # Phase 2: người dùng thêm lại phiên đóng gói vào hồ sơ (dòng mới, đang dùng).
    run(
        "INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_by, added_at) VALUES "
        f"(gen_random_uuid(), '{CLAIM_OPEN}', 'SESSION', '{S_PACK}', false, '{U2}', now())"
    )
    warnings: list[str] = []
    real_warning = logging.Logger.warning

    def capture(self: logging.Logger, msg: object, *args: object, **kw: object) -> None:
        warnings.append(str(msg) % args if args else str(msg))
        real_warning(self, msg, *args, **kw)  # type: ignore[arg-type]

    # env.py gọi `fileConfig` (xóa handler của logger alembic) → bắt qua `Logger.warning`, không qua caplog.
    monkeypatch.setattr(logging.Logger, "warning", capture)
    command.upgrade(cfg, SCHEMA_HEAD)
    monkeypatch.setattr(logging.Logger, "warning", real_warning)

    assert run("SELECT count(*) FROM claim WHERE source = 'LEGACY_HOLD'") == [(0,)]
    assert run("SELECT count(*) FROM \"user\" WHERE id = '00000000-0000-7000-8000-00000000a1c0'") == [(0,)]
    restored = run(
        "SELECT id::text, session_id::text, snapshot_id::text, removed_at, removed_by::text, removed_reason, backfilled "
        "FROM claim_evidence WHERE removed_at IS NOT NULL ORDER BY id"
    )
    assert restored == [r for r in removed_before if r[1] != S_PACK]  # phiên đóng gói: giữ dòng thêm lại
    assert run(
        f"SELECT removed_at FROM claim_evidence WHERE claim_id = '{CLAIM_OPEN}' AND session_id = '{S_PACK}'"
    ) == [(None,)]
    assert any("restore_removed_conflict" in w for w in warnings)


def test_missing_clip_failed_in_phase2_then_back(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bước 7: clip `MISSING` → `FAILED` khi lùi (CHECK Phase 2), lên lại → `MISSING` nếu vẫn `FAILED`."""
    cfg = alembic_config()
    seed_phase2_platform_data()
    seed_return_claims()
    command.upgrade(cfg, SCHEMA_HEAD)
    run(f"UPDATE clip SET status = 'MISSING' WHERE session_id IN ('{S_D}', '{S_A}')")
    monkeypatch.setenv(DETACH_ENV, "1")
    command.downgrade(cfg, "0005")
    assert run(f"SELECT DISTINCT status FROM clip WHERE session_id IN ('{S_D}', '{S_A}')") == [("FAILED",)]
    run(f"UPDATE clip SET status = 'READY' WHERE session_id = '{S_A}'")  # Phase 2 cắt lại được
    command.upgrade(cfg, SCHEMA_HEAD)
    assert dict(run(f"SELECT session_id::text, status FROM clip WHERE session_id IN ('{S_D}', '{S_A}')")) == {
        S_D: "MISSING", S_A: "READY"
    }  # fmt: skip
