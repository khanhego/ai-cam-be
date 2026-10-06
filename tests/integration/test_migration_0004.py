# ruff: noqa: S608 — SQL ghép từ hằng id trong test, không có input ngoài
"""Migration 0004 `hold_to_claims` (T-111; 02a §3, ADR-009, DEC-250, DEC-252, DEC-270, R3-5): clip đang giữ →
hồ sơ `LEGACY_HOLD`, kiểm tập con, downgrade trả cờ giữ + archive, nâng cấp lại khôi phục.

TC-MG.03, TC-MG.04, AC-26 (phần nâng cấp). DB riêng `<TEST_DATABASE_URL>_mig` như test 0003.
"""

import asyncio
import os
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from aicam.core.settings import get_settings

from .conftest import TEST_DATABASE_URL, _ensure_database, _reset_schema, alembic_config

pytestmark = pytest.mark.integration

_BASE = make_url(TEST_DATABASE_URL)
MIG_URL = _BASE.set(database=f"{_BASE.database}_mig").render_as_string(hide_password=False)

U1 = "01930000-0000-7000-8000-0000000000f1"
U2 = "01930000-0000-7000-8000-0000000000f2"
STATION = "01930000-0000-7000-8000-0000000000b1"


def _ids(letter: str) -> tuple[str, str]:
    """(package, session) của kiện `letter` (A..F)."""
    n = "ABCDEF".index(letter) + 1
    return f"01930000-0000-7000-8000-00000000a00{n}", f"01930000-0000-7000-8000-00000000d00{n}"


def _clip(letter: str, role: str) -> str:
    n = "ABCDEF".index(letter) + 1
    return f"01930000-0000-7000-8000-00000000e{n}{1 if role == 'CAM1' else 2}0"


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


def _package(letter: str, status: str = "DELIVERED", days_ago: int = 10) -> None:
    package, session = _ids(letter)
    run(
        f"INSERT INTO package (id, tracking_number, warehouse_status) "
        f"VALUES ('{package}', 'SPXTSTMG0000{letter}', '{status}')"
    )
    run(
        f"INSERT INTO session (id, package_id, station_id, status, started_at, ended_at, open_code) VALUES "
        f"('{session}', '{package}', '{STATION}', 'COMPLETED', now() - interval '{days_ago} days 1 minute', "
        f"now() - interval '{days_ago} days', 'SPXTSTMG0000{letter}')"
    )


def _clip_row(letter: str, role: str, held_by: str | None = None, held_hours_ago: int = 0) -> None:
    _, session = _ids(letter)
    held = held_by is not None
    run(
        "INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, held, held_by, held_at) "
        f"VALUES ('{_clip(letter, role)}', '{session}', '{role}', 'READY', "
        "now() - interval '10 days 1 minute', "
        f"now() - interval '10 days', {str(held).lower()}, "
        f"{f"'{held_by}'" if held_by else 'NULL'}, "
        f"{f"now() - interval '{held_hours_ago} hours'" if held else 'NULL'})"
    )


def _seed_phase1() -> None:
    """3 clip giữ trên 2 kiện (PRE-11): A có Cam 1 (Lan) + Cam 2 (Minh), B có Cam 1 (Lan); C không giữ."""
    run(
        'INSERT INTO "user" (id, username, display_name, role, password_hash) VALUES '
        f"('{U1}', 'tst_mg_lan', 'Lan CSKH', 'CSKH', 'x'), "
        f"('{U2}', 'tst_mg_minh', 'Minh QL', 'SUPERVISOR', 'x')"
    )
    run(f"INSERT INTO station (id, name) VALUES ('{STATION}', 'TST Station MG')")
    for letter in "ABC":
        _package(letter)
    _clip_row("A", "CAM1", U1, held_hours_ago=5)
    _clip_row("A", "CAM2", U2, held_hours_ago=2)
    _clip_row("B", "CAM1", U1, held_hours_ago=1)
    _clip_row("C", "CAM1")


def _clips() -> dict[str, tuple[bool, str | None, Any]]:
    return {str(r[0]): (r[1], str(r[2]) if r[2] else None, r[3]) for r in run(
        "SELECT id, held, held_by, held_at FROM clip"
    )}  # fmt: skip


def test_upgrade_converts_holds_to_legacy_claims(mig_db: None) -> None:
    """TC-MG.03: 2 hồ sơ `LEGACY_HOLD` (hạn +30 ngày, ghi chú "Chuyển từ cờ giữ của …"); mọi clip giữ trước ∈
    tập được bảo vệ sau; `held = false` nhưng giữ `held_by` / `held_at`; audit; `alembic check` sạch."""
    _seed_phase1()
    before = _clips()

    command.upgrade(alembic_config(), "head")

    claims = run(
        "SELECT package_id, type, counterparty, status, source, deadline_source, created_by, code, "
        "deadline_at - now() BETWEEN interval '29 days 23 hours' AND interval '30 days 1 minute' "
        "FROM claim ORDER BY package_id"
    )
    package_a, session_a = _ids("A")
    package_b, session_b = _ids("B")
    assert [(*map(str, c[:7]), c[8]) for c in claims] == [
        (package_a, "OTHER", "PLATFORM", "NEW", "LEGACY_HOLD", "DEFAULT", U1, True),
        (package_b, "OTHER", "PLATFORM", "NEW", "LEGACY_HOLD", "DEFAULT", U1, True),
    ]
    assert sorted(c[7] for c in claims) == ["KN-000001", "KN-000002"]
    evidence = run(
        "SELECT c.package_id, ce.session_id, ce.kind, ce.auto FROM claim_evidence ce "
        "JOIN claim c ON c.id = ce.claim_id ORDER BY c.package_id"
    )
    assert [tuple(map(str, e)) for e in evidence] == [
        (package_a, session_a, "SESSION", "False"),
        (package_b, session_b, "SESSION", "False"),
    ]
    notes = run(
        "SELECT c.package_id, n.kind, n.text FROM claim_note n JOIN claim c ON c.id = n.claim_id "
        "ORDER BY c.package_id, n.text"
    )
    assert [(str(p), k) for p, k, _ in notes] == [
        (package_a, "SYSTEM"),
        (package_a, "SYSTEM"),
        (package_b, "SYSTEM"),
    ]
    texts = [t for _, _, t in notes]
    assert texts[0].startswith("Chuyển từ cờ giữ của Lan CSKH lúc ")
    assert texts[1].startswith("Chuyển từ cờ giữ của Minh QL lúc ")
    # Cờ giữ tắt, người / giờ giữ còn (cho downgrade).
    after = _clips()
    assert all(not held for held, _, _ in after.values())
    assert {k: v[1:] for k, v in after.items()} == {k: v[1:] for k, v in before.items()}
    # Tập con (DEC-250): mọi clip giữ trước thuộc phiên làm bằng chứng của hồ sơ chưa đóng.
    held_before = {k for k, (held, _, _) in before.items() if held}
    protected = {
        str(r[0])
        for r in run(
            "SELECT c.id FROM clip c JOIN claim_evidence ce ON ce.session_id = c.session_id "
            "JOIN claim cl ON cl.id = ce.claim_id WHERE cl.status <> 'CLOSED'"
        )
    }
    assert held_before <= protected
    audit = run("SELECT object_type, data->>'code' FROM audit_log WHERE action = 'CLIP_PROTECTION_MIGRATED'")
    assert sorted(a[1] for a in audit) == ["KN-000001", "KN-000002"]
    assert run("SELECT to_regnamespace('phase2_archive')") == [(None,)]
    command.check(alembic_config())


def test_upgrade_without_holds_is_noop(mig_db: None) -> None:
    command.upgrade(alembic_config(), "head")
    assert run("SELECT count(*) FROM claim") == [(0,)]
    assert run("SELECT nextval('claim_code_seq')") == [(1,)]


def _seed_phase2_after_upgrade() -> None:
    """Sau nâng cấp: C có hồ sơ khiếu nại MANUAL mở; D thuộc hồ sơ hàng hoàn đang về (clip PACK chưa giữ);
    E có hồ sơ đã đóng 200 ngày trước (hết bảo vệ)."""
    package_c, session_c = _ids("C")
    run(
        "INSERT INTO claim (id, package_id, type, counterparty, status, source, version) VALUES "
        f"('01930000-0000-7000-8000-00000000c001', '{package_c}', 'BUYER_CLAIM', 'PLATFORM', 'NEW', "
        "'MANUAL', 1)"
    )
    run(
        "INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_at) VALUES "
        f"('01930000-0000-7000-8000-00000000ce01', '01930000-0000-7000-8000-00000000c001', 'SESSION', "
        f"'{session_c}', true, now())"
    )
    _package("D", status="RETURN_EXPECTED")
    _clip_row("D", "CAM1")
    package_d, _ = _ids("D")
    run(
        "INSERT INTO return_case (id, kind, status, source, order_id) VALUES "
        "('01930000-0000-7000-8000-00000000cd01', 'BUYER_RETURN', 'EXPECTED', 'PLATFORM', NULL)"
    )
    run(
        "INSERT INTO return_case_package (return_case_id, package_id) VALUES "
        f"('01930000-0000-7000-8000-00000000cd01', '{package_d}')"
    )
    _package("E")
    _clip_row("E", "CAM1")
    package_e, session_e = _ids("E")
    run(
        "INSERT INTO claim (id, package_id, type, counterparty, status, source, version, closed_at) VALUES "
        f"('01930000-0000-7000-8000-00000000c002', '{package_e}', 'OTHER', 'PLATFORM', 'CLOSED', "
        "'MANUAL', 1, "
        "now() - interval '200 days')"
    )
    run(
        "INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_at) VALUES "
        f"('01930000-0000-7000-8000-00000000ce02', '01930000-0000-7000-8000-00000000c002', 'SESSION', "
        f"'{session_e}', true, now())"
    )


def test_downgrade_then_upgrade_round_trip(mig_db: None) -> None:
    """TC-MG.04 + R3-5 / DEC-270: downgrade đặt `held` cho mọi clip đang được bảo vệ (hồ sơ khiếu nại mở,
    hồ sơ hàng hoàn chưa kết thúc), ghi `downgrade_held_clips`, chuyển `LEGACY_HOLD` + ghi chú vào
    `phase2_archive`; nâng cấp lại khôi phục đúng hồ sơ cũ (không tạo `LEGACY_HOLD` giả), trả cờ giữ, drop
    schema; giữ mới trong lúc chạy code cũ → hồ sơ mới; mã `KN-` không trùng."""
    _seed_phase1()
    cfg = alembic_config()
    command.upgrade(cfg, "head")
    legacy = {str(r[0]): r[1] for r in run("SELECT id, code FROM claim WHERE source = 'LEGACY_HOLD'")}
    _seed_phase2_after_upgrade()
    upgraded = _clips()

    command.downgrade(cfg, "0003")

    clips = _clips()
    protected = {
        _clip("A", "CAM1"),
        _clip("A", "CAM2"),
        _clip("B", "CAM1"),
        _clip("C", "CAM1"),
        _clip("D", "CAM1"),
    }
    assert {k for k, (held, _, _) in clips.items() if held} == protected
    assert clips[_clip("E", "CAM1")][0] is False  # hồ sơ đóng 200 ngày trước: hết bảo vệ
    assert clips[_clip("A", "CAM2")][1:] == upgraded[_clip("A", "CAM2")][1:]  # giữ nguyên người / giờ giữ cũ
    assert clips[_clip("C", "CAM1")][2] is not None
    assert {str(r[0]) for r in run("SELECT clip_id FROM phase2_archive.downgrade_held_clips")} == protected
    assert {str(r[0]) for r in run("SELECT id FROM phase2_archive.legacy_claims")} == set(legacy)
    assert run("SELECT count(*) FROM phase2_archive.legacy_claim_evidence") == [(2,)]
    assert run("SELECT count(*) FROM phase2_archive.legacy_claim_notes") == [(3,)]
    assert run("SELECT source, count(*) FROM claim GROUP BY source") == [("MANUAL", 2)]

    # Code cũ (chỉ biết cờ giữ) chạy một thời gian: Admin giữ thêm clip của F.
    _package("F")
    _clip_row("F", "CAM1", U2, held_hours_ago=0)

    command.upgrade(cfg, "head")

    restored = {str(r[0]): r[1] for r in run("SELECT id, code FROM claim WHERE source = 'LEGACY_HOLD'")}
    assert {k: v for k, v in restored.items() if k in legacy} == legacy  # đúng hồ sơ cũ, đúng mã
    assert len(restored) == 3  # + hồ sơ mới cho kiện F (giữ mới trong lúc chạy code cũ)
    package_f, _ = _ids("F")
    assert run(f"SELECT count(*) FROM claim WHERE source = 'LEGACY_HOLD' AND package_id = '{package_f}'") == [
        (1,)
    ]
    assert run(
        "SELECT count(*) FROM claim_note n JOIN claim c ON c.id = n.claim_id WHERE c.source = 'LEGACY_HOLD'"
    ) == [(4,)]
    final = _clips()
    assert all(not held for held, _, _ in final.values())
    for clip_id in protected:  # người / giờ giữ trước downgrade được trả lại
        assert final[clip_id][1:] == upgraded[clip_id][1:]
    assert run("SELECT to_regnamespace('phase2_archive')") == [(None,)]
    codes = [r[0] for r in run("SELECT code FROM claim ORDER BY code")]
    (fresh,) = run(
        "INSERT INTO claim (id, package_id, type, counterparty, status, source, version) VALUES "
        f"('01930000-0000-7000-8000-00000000c003', '{package_f}', 'OTHER', 'PLATFORM', 'NEW', 'MANUAL', 1) "
        "RETURNING code"
    )
    assert fresh[0] not in codes
    command.check(cfg)


def test_legacy_deadline_is_thirty_days(mig_db: None) -> None:
    """02 §6.3 #5: hạn hồ sơ `LEGACY_HOLD` = lúc nâng cấp + 30 ngày (nhắc xem lại, `DEFAULT`)."""
    _seed_phase1()
    command.upgrade(alembic_config(), "head")
    (row,) = run("SELECT min(deadline_at - created_at), max(deadline_at - created_at) FROM claim")
    assert row[0] == row[1] == timedelta(days=30)
