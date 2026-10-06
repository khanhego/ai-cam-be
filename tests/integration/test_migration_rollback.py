# ruff: noqa: S608, E501 — SQL fixture ghép từ hằng id trong test (không có input ngoài), câu SQL dài
"""Rollback Phase 2 (T-120; 02a §3 bước 5–6, DEC-252, DEC-270, R3-5, DEC-331; TC-MG.05, TC-MG.06, AC-26).

Dữ liệu Phase 1 + Phase 2 đủ mọi bảng → `downgrade 0002` (0004 rồi 0003) → kiểm `phase2_archive`, bảng chính chỉ
còn Phase 1, cờ giữ cho clip được bảo vệ (truy vấn ứng viên J-02 của code Phase 1 không chọn chúng) → code cũ chạy
(giữ thêm 1 clip) → `upgrade head` → mọi dòng trở lại **y hệt** (so `to_jsonb` từng dòng), không `LEGACY_HOLD` giả,
mã mới không trùng, schema archive bị drop. DB riêng `<TEST_DATABASE_URL>_mig` như test 0003 / 0004.
"""

import asyncio
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.util import CommandError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from aicam.core.schema_guard import SCHEMA_HEAD
from aicam.core.settings import get_settings

from .conftest import ROOT, TEST_DATABASE_URL, _ensure_database, _reset_schema, alembic_config

pytestmark = pytest.mark.integration

_BASE = make_url(TEST_DATABASE_URL)
MIG_URL = _BASE.set(database=f"{_BASE.database}_mig").render_as_string(hide_password=False)


def i(n: int) -> str:
    return f"01940000-0000-7000-8000-{n:012x}"


U1, U2 = i(0x101), i(0x102)
ST1 = i(0x201)
SHOP = i(0x301)
O1, O2 = i(0x401), i(0x402)
OI1, OI2 = i(0x411), i(0x412)
P = {n: i(0x500 + n) for n in range(1, 7)}  # P1..P6
PT = i(0x5FF)  # kiện tạm TAM-
S = {n: i(0x600 + n) for n in range(1, 7)}  # phiên PACK của P1..P6
R1, R2, R3 = i(0x701), i(0x702), i(0x703)  # phiên RETURN
RC1, RC2, RC3 = i(0x801), i(0x802), i(0x803)
SN = {n: i(0x900 + n) for n in range(1, 5)}
C1, C2 = i(0xA01), i(0xA02)
A1, A2, A3 = i(0xB01), i(0xB02), i(0xB03)
EP1, EP2 = i(0xC01), i(0xC02)


def _clip_id(session: str, role: str) -> str:
    return session[:-4] + ("c" if role == "CAM1" else "d") + session[-3:]


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
    asyncio.run(_ensure_database(MIG_URL))
    asyncio.run(_reset_schema(MIG_URL))
    os.environ["DATABASE_URL"] = MIG_URL
    get_settings.cache_clear()
    try:
        command.upgrade(alembic_config(), "0002")
        yield
    finally:
        os.environ["DATABASE_URL"] = migrated_database_url
        get_settings.cache_clear()


def _pack(n: int, status: str, history: list[str], held_by: str | None = None) -> list[str]:
    """Kiện Pn đã đóng gói 10 ngày trước: phiên PACK + 2 clip READY; `history` = chuỗi trạng thái kho."""
    order = {1: f"'{O1}'", 2: f"'{O2}'"}.get(n, "NULL")
    out = [
        f"INSERT INTO package (id, order_id, tracking_number, warehouse_status, updated_at) "
        f"VALUES ('{P[n]}', {order}, 'SPXRB000000{n}', '{status}', now() - interval '9 days')",
        f"INSERT INTO session (id, package_id, station_id, status, started_at, ended_at, open_code, close_code) "
        f"VALUES ('{S[n]}', '{P[n]}', '{ST1}', 'COMPLETED', now() - interval '10 days 2 minutes', "
        f"now() - interval '10 days', 'SPXRB000000{n}', 'SPXRB000000{n}')",
    ]
    for role in ("CAM1", "CAM2"):
        held = held_by is not None and role == "CAM1"
        out.append(
            "INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, path, sha256, held, held_by, "
            f"held_at) VALUES ('{_clip_id(S[n], role)}', '{S[n]}', '{role}', 'READY', "
            f"now() - interval '10 days 2 minutes', now() - interval '10 days', 'clips/p{n}-{role}.mp4', "
            f"'sha{n}{role}', {str(held).lower()}, {f"'{held_by}'" if held else 'NULL'}, "
            f"{"now() - interval '5 days'" if held else 'NULL'})"
        )
    prev = None
    for k, to in enumerate(history):
        out.append(
            "INSERT INTO status_history (id, package_id, source, from_status, to_status, at) VALUES "
            f"('{i(0x5000 + n * 16 + k)}', '{P[n]}', 'WAREHOUSE', {f"'{prev}'" if prev else 'NULL'}, '{to}', "
            f"now() - interval '{20 - k} days')"
        )
        prev = to
    return out


def seed_phase1() -> None:
    """Ở 0002: 6 kiện… P4 có Cam 1 đang giữ (→ `LEGACY_HOLD` khi lên 0004); P5 không được bảo vệ."""
    stmts = [
        'INSERT INTO "user" (id, username, display_name, role, password_hash) VALUES '
        f"('{U1}', 'tst_rb_admin', 'Quản trị RB', 'ADMIN', 'x'), ('{U2}', 'tst_rb_cskh', 'Lan RB', 'CSKH', 'x')",
        f"INSERT INTO station (id, name) VALUES ('{ST1}', 'TST Station RB')",
        "INSERT INTO shop (id, platform, platform_shop_id, auth_status) "
        f"VALUES ('{SHOP}', 'SHOPEE', '990099', 'CONNECTED')",
        f'INSERT INTO "order" (id, shop_id, platform_order_sn, platform_status) VALUES '
        f"('{O1}', '{SHOP}', '2410RB00001', 'COMPLETED'), ('{O2}', '{SHOP}', '2410RB00002', 'TO_RETURN')",
        "INSERT INTO order_item (id, order_id, sku, product_name, variation, quantity) VALUES "
        f"('{OI1}', '{O1}', 'AT-DEN-L', 'Áo thun basic', 'Đen / L', 2), "
        f"('{OI2}', '{O2}', 'TUI-01', 'Túi vải', NULL, 1)",
        "UPDATE setting SET retention_clip_days = 90",
    ]
    full = ["NEW", "PACKING", "PACKED", "HANDED_OVER", "DELIVERED"]
    stmts += _pack(1, "DELIVERED", full)
    stmts += _pack(2, "HANDED_OVER", full[:4])
    stmts += _pack(3, "PACKED", full[:3])
    stmts += _pack(4, "HANDED_OVER", full[:4], held_by=U2)
    stmts += _pack(5, "DELIVERED", full)
    run_many(stmts)


def seed_phase2() -> None:
    """Ở head: 3 hồ sơ hàng hoàn (một chưa xác định có kiện tạm), 3 phiên RETURN (1 có approval, 1 có export, 1 hủy
    `NOT_A_RETURN`), 4 ảnh (1 `PACK_CLOSE`), 2 hồ sơ khiếu nại + `LEGACY_HOLD` có ghi chú người dùng / gói bằng
    chứng / cảnh báo trỏ tới, 3 cảnh báo, cột Phase 2 của station / setting / shop / phiên PACK."""
    rs = "now() - interval '1 day'"
    stmts = [
        f"UPDATE station SET kind = 'BOTH', work_mode = 'RETURN', operator_name = 'Lan' WHERE id = '{ST1}'",
        "UPDATE setting SET return_missing_days = 5, claim_deadline_days = 10, "
        "recon_start_at = now() - interval '30 days'",
        f"UPDATE shop SET last_return_cursor = now() - interval '1 hour' WHERE id = '{SHOP}'",
        f"UPDATE session SET operator_name = 'Minh', camera_clock = '{{\"CAM1\": 0.2}}' WHERE id = '{S[1]}'",
        # Admin giữ clip ở Phase 2 (API-42): lùi / lên lại vẫn là cờ giữ, không thành LEGACY_HOLD (DEC-332).
        f"UPDATE clip SET held = true, held_by = '{U1}', held_at = now() - interval '1 hour' "
        f"WHERE id = '{_clip_id(S[5], 'CAM2')}'",
        # Hồ sơ hàng hoàn
        "INSERT INTO return_case (id, order_id, kind, status, source, platform_return_sn, return_tracking_number, "
        "reason, reason_text, requested_items, received_at, conclusion, signal_keys, single_session) VALUES "
        f"('{RC1}', '{O1}', 'BUYER_RETURN', 'RECEIVED_ISSUE', 'PLATFORM', '2410RTRB1', 'SPXRTRB1', 'DAMAGED', "
        f"'Áo rách', '[{{\"sku\": \"AT-DEN-L\", \"quantity\": 2}}]', {rs}, 'DAMAGED', '{{RETURN_SN:2410RTRB1}}', true)",
        "INSERT INTO return_case (id, order_id, kind, status, source, expected_since) VALUES "
        f"('{RC2}', '{O2}', 'FAILED_DELIVERY', 'EXPECTED', 'PLATFORM', now() - interval '6 days')",
        "INSERT INTO return_case (id, kind, status, source, received_at, conclusion, merged_into_id) VALUES "
        f"('{RC3}', 'UNIDENTIFIED', 'RECEIVED_OK', 'WAREHOUSE', {rs}, 'OK', NULL)",
        "INSERT INTO package (id, tracking_number, warehouse_status, verified, is_placeholder) VALUES "
        f"('{PT}', 'TAM-' || lpad(nextval('placeholder_code_seq')::text, 6, '0'), 'RETURN_RECEIVED_OK', false, true)",
        "INSERT INTO return_case_package (return_case_id, package_id) VALUES "
        f"('{RC1}', '{P[1]}'), ('{RC2}', '{P[2]}'), ('{RC3}', '{PT}')",
        # Ở head (Phase 3) hồ sơ gắn shop của đơn (0006 backfill bản lùi / lên lại cùng giá trị).
        'UPDATE return_case rc SET shop_id = o.shop_id FROM "order" o WHERE o.id = rc.order_id',
        f"UPDATE package SET warehouse_status = 'RETURN_RECEIVED_ISSUE' WHERE id = '{P[1]}'",
        f"UPDATE package SET warehouse_status = 'RETURN_EXPECTED' WHERE id = '{P[2]}'",
    ]
    for k, (pkg, frm, to) in enumerate(
        [
            (P[1], "DELIVERED", "RETURN_EXPECTED"),
            (P[1], "RETURN_EXPECTED", "RETURN_INSPECTING"),
            (P[1], "RETURN_INSPECTING", "RETURN_RECEIVED_ISSUE"),
            (P[2], "HANDED_OVER", "RETURN_EXPECTED"),
            (PT, None, "RETURN_RECEIVED_OK"),
        ]
    ):
        stmts.append(
            "INSERT INTO status_history (id, package_id, source, from_status, to_status, at) VALUES "
            f"('{i(0x6000 + k)}', '{pkg}', 'WAREHOUSE', {f"'{frm}'" if frm else 'NULL'}, '{to}', "
            f"now() - interval '{3 - min(k, 2)} days')"
        )
    # Phiên RETURN
    for sid, pkg, case, status, extra_cols, extra_vals in (
        (R1, P[1], RC1, "COMPLETED", ", inspection_conclusion, inspection_note, inspection_saved_at, "
         "inspection_lines_mode, inspection_corrections, camera_clock",
         ", 'DAMAGED', 'Rách tay', now() - interval '1 day', 'FULL', '[]', '{\"CAM1\": 0.1}'"),
        (R2, PT, RC3, "COMPLETED", ", inspection_conclusion", ", 'OK'"),
        (R3, P[2], RC2, "CANCELLED", ", cancel_reason", ", 'NOT_A_RETURN'"),
    ):  # fmt: skip
        stmts.append(
            "INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, "
            f"return_case_id, operator_name{extra_cols}) VALUES ('{sid}', 'RETURN', '{pkg}', '{ST1}', '{status}', "
            f"now() - interval '1 day 5 minutes', now() - interval '1 day', 'SPXRTRB', '{case}', 'Lan'{extra_vals})"
        )
    for sid in (
        R1,
        R2,
        R3,
    ):  # J-01 cắt cả phiên hủy (BUG-G5-P2-2: phiên kết thúc không có clip → downgrade từ chối)
        stmts.append(
            "INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, path, sha256) VALUES "
            f"('{_clip_id(sid, 'CAM1')}', '{sid}', 'CAM1', 'READY', now() - interval '1 day 5 minutes', "
            f"now() - interval '1 day', 'clips/{sid}-cam1.mp4', 'sha-{sid}')"
        )
    stmts += [
        f"INSERT INTO session_event (id, session_id, type, payload, at) VALUES ('{i(0x7101)}', '{R1}', 'OPENED', "
        "'{}', now() - interval '1 day 5 minutes')",
        "INSERT INTO approval_request (id, station_id, session_id, tracking_number, type, status, decision, "
        f"decided_by, decided_at) VALUES ('{i(0x7201)}', '{ST1}', '{R1}', 'SPXRB0000001', 'ASSIST', 'RESOLVED', "
        f"'CONTINUE', '{U1}', now() - interval '1 day')",
        "INSERT INTO export (id, session_id, layout, status, progress, path_video, created_by) VALUES "
        f"('{i(0x7301)}', '{R2}', 'SIDE_BY_SIDE', 'READY', 100, 'exports/x/v.mp4', '{U1}')",
        "INSERT INTO inspection_line (id, session_id, order_item_id, position, product_name, variation, "
        "quantity_sent, quantity_requested, quantity_received, condition, note) VALUES "
        f"('{i(0x7401)}', '{R1}', '{OI1}', 1, 'Áo thun basic', 'Đen / L', 2, 2, 2, 'DAMAGED', 'rách')",
        # Ảnh
        "INSERT INTO snapshot (id, session_id, kind, camera_role, taken_at, path, sha256, size_bytes) VALUES "
        f"('{SN[1]}', '{R1}', 'MANUAL', 'CAM1', now() - interval '1 day', 'snapshots/1.jpg', 's1', 100), "
        f"('{SN[2]}', '{R1}', 'MANUAL', 'CAM1', now() - interval '1 day', 'snapshots/2.jpg', 's2', 100), "
        f"('{SN[3]}', '{R2}', 'MANUAL', 'CAM1', now() - interval '1 day', 'snapshots/3.jpg', 's3', 100), "
        f"('{SN[4]}', '{S[1]}', 'PACK_CLOSE', 'CAM1', now() - interval '10 days', 'snapshots/4.jpg', 's4', 100)",
        # Hồ sơ khiếu nại
        "INSERT INTO claim (id, package_id, order_id, return_case_id, type, counterparty, status, source, "
        f"owner_user_id, deadline_at, deadline_source, created_by) VALUES ('{C1}', '{P[1]}', '{O1}', '{RC1}', "
        f"'DAMAGED', 'PLATFORM', 'NEW', 'AUTO_RETURN', '{U2}', now() + interval '5 days', 'DEFAULT', NULL)",
        "INSERT INTO claim (id, package_id, type, counterparty, status, source, created_by, platform_claim_ref) "
        f"VALUES ('{C2}', '{P[3]}', 'BUYER_CLAIM', 'PLATFORM', 'SUBMITTED', 'MANUAL', '{U2}', 'SHP-123')",
        # Ở head (Phase 3) code đặt `submitted_at` khi gửi; 0006 backfill bản lùi / lên lại cùng giá trị (DEC-461).
        f"UPDATE claim SET submitted_at = updated_at WHERE id = '{C2}'",
        "INSERT INTO claim_evidence (id, claim_id, kind, session_id, snapshot_id, auto, added_by, added_at) VALUES "
        f"('{i(0xA101)}', '{C1}', 'SESSION', '{R1}', NULL, true, NULL, now()), "
        f"('{i(0xA102)}', '{C1}', 'SESSION', '{S[1]}', NULL, true, NULL, now()), "
        f"('{i(0xA103)}', '{C1}', 'SNAPSHOT', NULL, '{SN[1]}', false, '{U2}', now()), "
        f"('{i(0xA104)}', '{C2}', 'SESSION', '{S[3]}', NULL, false, '{U2}', now())",
        "INSERT INTO claim_note (id, claim_id, kind, text, author_user_id, at) VALUES "
        f"('{i(0xA201)}', '{C1}', 'NOTE', 'Đã gửi ảnh cho sàn', '{U2}', now()), "
        f"('{i(0xA202)}', '{C2}', 'STATUS_CHANGE', 'Mới → Đã gửi', '{U2}', now())",
        "INSERT INTO evidence_pack (id, claim_id, status, progress, path, sha256, size_bytes, created_by, "
        f"expires_at) VALUES ('{EP1}', '{C1}', 'READY', 100, 'exports/pack-1/ho-so.zip', 'z1', 10, '{U1}', "
        "now() + interval '1 day')",
        # Hồ sơ LEGACY_HOLD (0004 tạo cho P4): ghi chú người dùng + gói bằng chứng + cảnh báo trỏ tới
        "INSERT INTO claim_note (id, claim_id, kind, text, author_user_id, at) SELECT "
        f"'{i(0xA203)}', id, 'NOTE', 'Khách khiếu nại lại', '{U2}', now() FROM claim WHERE source = 'LEGACY_HOLD'",
        "INSERT INTO evidence_pack (id, claim_id, status, progress, missing, error, created_by) SELECT "
        f"'{EP2}', id, 'FAILED', 0, '[\"x\"]', 'disk', '{U1}' FROM claim WHERE source = 'LEGACY_HOLD'",
        # Cảnh báo đối soát
        "INSERT INTO recon_alert (id, package_id, rule, severity, status, context, context_key, detected_at, "
        f"last_seen_at) VALUES ('{A1}', '{P[2]}', 'RETURN_OVERDUE', 'HIGH', 'OPEN', '{{\"days\": 6}}', 'k1', "
        "now(), now())",
        "INSERT INTO recon_alert (id, package_id, rule, severity, status, context_key, detected_at, last_seen_at, "
        f"closed_at, resolution_action, resolution_note, resolved_by) VALUES ('{A2}', '{P[3]}', "
        f"'PACKED_NOT_HANDED_OVER', 'MEDIUM', 'RESOLVED', 'k2', now(), now(), now(), 'RESOLVE', 'đã gọi', '{U1}')",
        "INSERT INTO recon_alert (id, package_id, rule, severity, status, context_key, detected_at, last_seen_at, "
        "closed_at, resolution_action, resolved_by, claim_id) SELECT "
        f"'{A3}', '{P[4]}', 'SHIPPED_NOT_PACKED', 'LOW', 'RESOLVED', 'k3', now(), now(), now(), 'OPEN_CLAIM', "
        f"'{U1}', id FROM claim WHERE source = 'LEGACY_HOLD'",
    ]
    run_many(stmts)


# Bảng so sánh y hệt trước / sau (mọi bảng nghiệp vụ; `audit_log` chỉ thêm, so số dòng riêng).
COMPARED = (
    "user", "station", "shop", "setting", "order", "order_item", "package", "status_history", "session",
    "session_event", "clip", "approval_request", "export", "return_case", "return_case_package",
    "inspection_line", "snapshot", "claim", "claim_evidence", "claim_note", "evidence_pack", "recon_alert",
)  # fmt: skip


def dump() -> dict[str, list[Any]]:
    return {
        t: [r[0] for r in run(f'SELECT to_jsonb(x) FROM "{t}" x ORDER BY to_jsonb(x)::text')]
        for t in COMPARED
    }


def old_j02_candidates() -> set[str]:
    """Ứng viên xóa của J-02 Phase 1 (`media.service.retention_clip_candidates` nhánh feat/01-packing-mvp) với đồng
    hồ +200 ngày: mọi clip `held = false`, `READY`."""
    return {
        str(r[0])
        for r in run(
            "SELECT id FROM clip WHERE held IS false AND status = 'READY' AND end_at < now() + interval '200 days'"
        )
    }


def test_round_trip_with_phase2_data(mig_db: None) -> None:
    """TC-MG.05: up → dữ liệu Phase 2 → down 0002 → code cũ giữ thêm clip → up → khôi phục y hệt, mã không trùng."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    before = dump()
    sizes = {
        t: len(before[t])
        for t in ("return_case", "claim", "claim_note", "snapshot", "recon_alert", "session")
    }
    assert sizes == {
        "return_case": 3,
        "claim": 3,
        "claim_note": 4,
        "snapshot": 4,
        "recon_alert": 3,
        "session": 8,
    }
    audit_before = run("SELECT count(*) FROM audit_log")[0][0]
    pack_sessions = {str(r[0]) for r in run("SELECT id FROM session WHERE type = 'PACK'")}
    pack_clips = {str(r[0]) for r in run("SELECT c.id FROM clip c JOIN session s ON s.id = c.session_id "
                                         "WHERE s.type = 'PACK'")}  # fmt: skip

    command.downgrade(cfg, "0002")

    # Bảng chính: chỉ còn Phase 1, không mất phiên / clip đóng gói.
    assert run("SELECT version_num FROM alembic_version") == [("0002",)]
    assert run("SELECT to_regclass('return_case'), to_regclass('claim'), to_regclass('snapshot')") == [
        (None, None, None)
    ]
    assert {str(r[0]) for r in run("SELECT id FROM session")} == pack_sessions
    assert {str(r[0]) for r in run("SELECT id FROM clip")} == pack_clips
    assert run("SELECT count(*) FROM package WHERE tracking_number LIKE 'TAM-%'") == [(0,)]
    statuses = dict(run("SELECT tracking_number, warehouse_status FROM package ORDER BY 1"))
    assert statuses == {
        "SPXRB0000001": "DELIVERED",  # RETURN_RECEIVED_ISSUE → trạng thái cuối không phải hoàn
        "SPXRB0000002": "HANDED_OVER",
        "SPXRB0000003": "PACKED",
        "SPXRB0000004": "HANDED_OVER",
        "SPXRB0000005": "DELIVERED",
    }
    assert run("SELECT count(*) FROM status_history WHERE to_status LIKE 'RETURN%'") == [(0,)]
    assert run("SELECT conname FROM pg_constraint WHERE contype = 'c' AND NOT convalidated") == []
    # Archive đủ dòng.
    counts = {
        t: run(f"SELECT count(*) FROM phase2_archive.{t}")[0][0]
        for t in (
            "return_case", "return_case_package", "inspection_line", "snapshot", "claim", "claim_evidence",
            "claim_note", "evidence_pack", "recon_alert", "return_session", "return_session_event", "return_clip",
            "return_approval_request", "return_export", "placeholder_package", "return_status_history",
            "session_cols", "station_cols", "setting_cols", "shop_cols", "legacy_claims", "legacy_claim_evidence",
            "legacy_claim_notes", "legacy_evidence_packs", "legacy_alert_claims",
        )
    }  # fmt: skip
    assert counts == {
        "return_case": 3, "return_case_package": 3, "inspection_line": 1, "snapshot": 4, "claim": 2,
        "claim_evidence": 4, "claim_note": 2, "evidence_pack": 1, "recon_alert": 3, "return_session": 3,
        "return_session_event": 1, "return_clip": 3, "return_approval_request": 1, "return_export": 1,
        "placeholder_package": 1, "return_status_history": 5, "session_cols": 1, "station_cols": 1,
        "setting_cols": 1, "shop_cols": 1, "legacy_claims": 1, "legacy_claim_evidence": 1,
        "legacy_claim_notes": 2, "legacy_evidence_packs": 1, "legacy_alert_claims": 1,
    }  # fmt: skip
    # Clip được bảo vệ (hồ sơ mở / hàng hoàn chưa kết thúc / LEGACY_HOLD / Admin giữ) → held; J-02 code cũ chỉ
    # chọn clip không có lý do giữ (P5 Cam 1).
    assert old_j02_candidates() == {_clip_id(S[5], "CAM1")}

    # Code cũ chạy: kiện P6 đóng gói + Admin giữ Cam 1 (giữ mới → hồ sơ LEGACY_HOLD mới khi nâng cấp).
    run_many(_pack(6, "PACKED", ["NEW", "PACKING", "PACKED"], held_by=U1))

    command.upgrade(cfg, "head")

    assert run("SELECT to_regnamespace('phase2_archive')") == [(None,)]
    after = dump()
    p6 = {P[6], S[6], _clip_id(S[6], "CAM1"), _clip_id(S[6], "CAM2")}
    legacy_p6 = {str(r[0]) for r in run(f"SELECT id FROM claim WHERE package_id = '{P[6]}'")}
    assert len(legacy_p6) == 1

    def unrelated(row: dict[str, Any]) -> bool:
        keys = ("id", "package_id", "session_id", "claim_id")
        return not any(str(row.get(k)) in p6 | legacy_p6 for k in keys)

    for table in COMPARED:
        assert [r for r in after[table] if unrelated(r)] == before[table], table
    assert run("SELECT source, count(*) FROM claim GROUP BY source ORDER BY 1") == [
        ("AUTO_RETURN", 1), ("LEGACY_HOLD", 2), ("MANUAL", 1)
    ]  # fmt: skip
    assert run("SELECT count(*) FROM audit_log")[0][0] == audit_before + 1  # CLIP_PROTECTION_MIGRATED của P6
    # Mã mới không trùng mã đã cấp.
    (hh,) = run("INSERT INTO return_case (id, kind, status, source) VALUES "
                f"('{i(0x8FF)}', 'UNANNOUNCED', 'EXPECTED', 'WAREHOUSE') RETURNING code")  # fmt: skip
    assert hh[0] == "HH-000004"
    (kn,) = run(
        "INSERT INTO claim (id, package_id, type, counterparty, status, source) VALUES "
        f"('{i(0xAFF)}', '{P[5]}', 'OTHER', 'CARRIER', 'NEW', 'MANUAL') RETURNING code"
    )
    assert kn[0] not in {r[0] for r in run(f"SELECT code FROM claim WHERE id <> '{i(0xAFF)}'")}
    assert run("SELECT nextval('placeholder_code_seq')") == [(2,)]
    command.check(cfg)


def test_down_up_down_up_is_stable(mig_db: None) -> None:
    """Lùi / lên hai lần (archive cũ đã khôi phục → chép lại), lần dừng giữa ở 0003."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    before = dump()

    command.downgrade(cfg, "0002")
    command.upgrade(cfg, "0003")  # khôi phục, chưa drop archive (0004 còn đọc)
    assert run("SELECT count(*) FROM return_case") == [(3,)]
    assert run("SELECT to_regclass('phase2_archive.downgrade_held_clips') IS NOT NULL") == [(True,)]
    command.downgrade(cfg, "0002")
    command.upgrade(cfg, "head")

    assert dump() == before
    assert run("SELECT to_regnamespace('phase2_archive')") == [(None,)]


def test_downgrade_refused_with_open_return_session(mig_db: None) -> None:
    """Phiên RETURN đang mở → dừng, cả lệnh lùi (0004 + 0003) rollback, không đổi gì."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    run(f"UPDATE session SET status = 'OPEN', ended_at = NULL WHERE id = '{R2}'")
    before = dump()

    with pytest.raises(RuntimeError, match="1 phiên nhận hàng hoàn đang mở"):
        command.downgrade(cfg, "0002")

    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]
    assert dump() == before
    assert run("SELECT to_regnamespace('phase2_archive')") == [(None,)]


def test_old_image_refuses_new_database(mig_db: None, tmp_path: Path) -> None:
    """TC-MG.06 (mô phỏng): image Phase 1 chỉ có 0001 / 0002 → `alembic upgrade head` của service `migrate` lỗi
    revision lạ trên DB head (`SCHEMA_HEAD`) → `api` (depends_on migrate completed_successfully) không khởi động."""
    command.upgrade(alembic_config(), "head")
    scripts = tmp_path / "alembic"
    (scripts / "versions").mkdir(parents=True)
    shutil.copy(ROOT / "alembic" / "env.py", scripts / "env.py")
    shutil.copy(ROOT / "alembic" / "script.py.mako", scripts / "script.py.mako")
    for name in ("0001_initial.py", "0002_clip_timeline.py"):
        shutil.copy(ROOT / "alembic" / "versions" / name, scripts / "versions" / name)
    old = alembic_config()
    old.set_main_option("script_location", str(scripts))

    with pytest.raises(CommandError, match=SCHEMA_HEAD):
        command.upgrade(old, "head")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]


# ---------------------------------------------------------------- G3 Phase 2 (M-F1, M-F4, M-F5, M-F7, M-F9)

SYSTEM_HOLDER = "00000000-0000-7000-8000-00000000a1c0"


class _OtherConnection:
    """Một kết nối khác vào DB migrate (như worker / beat của image cũ còn chạy) — giữ mở trong thread riêng."""

    def __init__(self, application_name: str) -> None:
        import threading

        self._ready, self._stop = threading.Event(), threading.Event()
        self._thread = threading.Thread(target=lambda: asyncio.run(self._hold(application_name)), daemon=True)

    async def _hold(self, name: str) -> None:
        engine = create_async_engine(MIG_URL, connect_args={"server_settings": {"application_name": name}})
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
                self._ready.set()
                await asyncio.to_thread(self._stop.wait, 30)
        finally:
            await engine.dispose()

    def __enter__(self) -> "_OtherConnection":
        self._thread.start()
        assert self._ready.wait(10)
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(10)


def test_upgrade_0004_refuses_with_other_connections(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """M-F1 (c): còn kết nối khác (worker Phase 1) khi có clip giữ cần chuyển → dừng, DB vẫn ở 0002."""
    cfg = alembic_config()
    seed_phase1()  # P4 có clip đang giữ
    with _OtherConnection("celery-worker-phase1"), pytest.raises(RuntimeError, match="celery-worker-phase1"):
        command.upgrade(cfg, "head")
    assert run("SELECT version_num FROM alembic_version") == [("0002",)]
    assert run("SELECT count(*) FROM clip WHERE held") == [(1,)]
    monkeypatch.setenv("AICAM_MIGRATE_ALLOW_ACTIVE_CONNECTIONS", "1")
    with _OtherConnection("psql"):
        command.upgrade(cfg, "head")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]


def test_downgrade_refuses_uncut_return_clip(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """M-F4: clip phiên RETURN còn PENDING / FAILED → từ chối (code cũ không giữ video thô); env cho phép."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    run(f"UPDATE clip SET status = 'FAILED' WHERE id = '{_clip_id(R2, 'CAM1')}'")
    before = dump()
    with pytest.raises(RuntimeError, match="1 clip phiên nhận hàng hoàn chưa cắt được"):
        command.downgrade(cfg, "0002")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]
    assert dump() == before
    monkeypatch.setenv("AICAM_DOWNGRADE_ALLOW_UNCUT_RETURN_CLIPS", "1")
    command.downgrade(cfg, "0002")
    assert run("SELECT status FROM phase2_archive.return_clip WHERE status = 'FAILED'") == [("FAILED",)]


def test_downgrade_refuses_return_session_without_clip_rows(
    mig_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BUG-G5-P2-2: phiên RETURN đã đóng nhưng J-01 còn trong hàng đợi (chưa có dòng clip) → từ chối như clip chưa
    cắt (worker cũ sẽ bỏ qua job → mất clip); env cho phép."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    run(
        f"DELETE FROM clip WHERE session_id = (SELECT session_id FROM clip WHERE id = '{_clip_id(R2, 'CAM1')}')"
    )
    before = dump()
    with pytest.raises(RuntimeError, match="1 phiên đã kết thúc chưa được tạo clip"):
        command.downgrade(cfg, "0002")
    assert run("SELECT version_num FROM alembic_version") == [(SCHEMA_HEAD,)]
    assert dump() == before
    monkeypatch.setenv("AICAM_DOWNGRADE_ALLOW_UNCUT_RETURN_CLIPS", "1")
    command.downgrade(cfg, "0002")


def test_downgrade_holds_in_system_name_and_reports_deleted_evidence(mig_db: None) -> None:
    """M-F5: cờ giữ do downgrade đặt đứng tên người dùng hệ thống (UI cũ hiện lý do); lên lại trả người / giờ giữ cũ,
    xóa người dùng hệ thống; hồ sơ có clip bằng chứng bị xóa khi chạy bản cũ → audit + log."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    before = dump()
    command.downgrade(cfg, "0002")

    holders = run("SELECT DISTINCT held_by::text FROM clip WHERE held AND id IN "
                  "(SELECT clip_id FROM phase2_archive.downgrade_held_clips)")  # fmt: skip
    assert holders == [(SYSTEM_HOLDER,)]
    assert run(f"SELECT display_name, is_active FROM \"user\" WHERE id = '{SYSTEM_HOLDER}'") == [
        ("Hệ thống (bảo vệ bằng chứng Phase 2)", False)
    ]
    # Bản cũ (sai) xóa một clip bằng chứng của hồ sơ C1 (phiên PACK S1).
    run(f"UPDATE clip SET status = 'DELETED', deleted_at = now() WHERE id = '{_clip_id(S[1], 'CAM2')}'")

    command.upgrade(cfg, "head")

    assert run(f"SELECT count(*) FROM \"user\" WHERE id = '{SYSTEM_HOLDER}'") == [(0,)]
    assert run("SELECT count(*) FROM clip WHERE held_by = :u", {"u": SYSTEM_HOLDER}) == [(0,)]
    audit = run(
        "SELECT object_id, data->>'code' FROM audit_log WHERE action = 'EVIDENCE_CLIP_DELETED_DURING_ROLLBACK'"
    )
    assert audit == [(C1, run(f"SELECT code FROM claim WHERE id = '{C1}'")[0][0])]
    after = dump()
    assert after["user"] == before["user"]
    assert after["claim"] == before["claim"]


def test_restore_refuses_duplicate_placeholder_code(mig_db: None) -> None:
    """M-F7: mã `TAM-` của kiện tạm đã bị kiện khác dùng khi chạy bản cũ → lỗi rõ, không nửa chừng."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    code = run(f"SELECT tracking_number FROM package WHERE id = '{PT}'")[0][0]
    command.downgrade(cfg, "0002")
    run(f"INSERT INTO package (id, tracking_number, warehouse_status) VALUES ('{i(0x5FE)}', '{code}', 'NEW')")
    with pytest.raises(RuntimeError, match=f"kiện tạm.*{code}"):
        command.upgrade(cfg, "head")
    assert run("SELECT version_num FROM alembic_version") == [("0002",)]


def test_placeholder_with_pack_session_kept_cancelled(mig_db: None) -> None:
    """M-F9: kiện tạm còn phiên PACK sau khi lùi → giữ lại với `CANCELLED` (không `DELIVERED` giả); lên lại trả
    trạng thái hoàn."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    run(
        "INSERT INTO session (id, package_id, station_id, status, started_at, ended_at, open_code) VALUES "
        f"('{i(0x6FF)}', '{PT}', '{ST1}', 'CANCELLED', now() - interval '1 hour', now(), 'TAM')"
    )
    before = run(f"SELECT warehouse_status FROM package WHERE id = '{PT}'")
    command.downgrade(cfg, "0002")
    assert run(f"SELECT warehouse_status FROM package WHERE id = '{PT}'") == [("CANCELLED",)]
    command.upgrade(cfg, "head")
    assert run(f"SELECT warehouse_status FROM package WHERE id = '{PT}'") == before


def test_restore_detaches_package_changed_by_old_code(mig_db: None) -> None:
    """BB-15: bản cũ xác nhận kiện đang về (P2, hồ sơ RC2 mở) đã giao → lên lại: kiện giữ DELIVERED, rời hồ sơ,
    hồ sơ không còn kiện trong luồng hoàn → CANCELLED."""
    cfg = alembic_config()
    seed_phase1()
    command.upgrade(cfg, "head")
    seed_phase2()
    command.downgrade(cfg, "0002")
    run(f"UPDATE package SET warehouse_status = 'DELIVERED' WHERE id = '{P[2]}'")
    command.upgrade(cfg, "head")
    assert run(f"SELECT warehouse_status FROM package WHERE id = '{P[2]}'") == [("DELIVERED",)]
    assert run(f"SELECT count(*) FROM return_case_package WHERE package_id = '{P[2]}'") == [(0,)]
    assert run(f"SELECT status FROM return_case WHERE id = '{RC2}'") == [("CANCELLED",)]
    assert run(f"SELECT status FROM return_case WHERE id = '{RC1}'") == [("RECEIVED_ISSUE",)]
