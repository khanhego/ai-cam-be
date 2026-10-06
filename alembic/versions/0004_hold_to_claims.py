# ruff: noqa: S608 — SQL ghép từ hằng của migration (tên bảng / cột), không có input ngoài
"""hold_to_claims — clip đang "giữ" → hồ sơ khiếu nại `LEGACY_HOLD` (02a §3 Migration 0004, ADR-009, T-111)

Upgrade (một transaction — DEC-250):
1. Nâng cấp lại sau downgrade (R3-5, DEC-270): khôi phục hồ sơ `LEGACY_HOLD` + bằng chứng + ghi chú từ
   `phase2_archive.legacy_claims*`; clip trong `phase2_archive.downgrade_held_clips` mà cờ giữ chưa bị đổi từ lúc
   downgrade → `held = false` (trả `held_by` / `held_at` cũ). Không tạo `LEGACY_HOLD` mới cho các clip đó.
   Clip đã giữ sẵn trước downgrade (`downgrade_preheld_clips`, cờ chưa đổi) → giữ nguyên `held` (DEC-332).
2. Clip `held` còn lại (`held_before`) → nhóm theo kiện → mỗi kiện một hồ sơ `LEGACY_HOLD` (`OTHER`, Sàn, `NEW`,
   hạn = lúc nâng cấp + 30 ngày `DEFAULT`, người tạo = người giữ đầu tiên) + bằng chứng `SESSION` từng phiên +
   ghi chú hệ thống "Chuyển từ cờ giữ của … lúc …" + audit `CLIP_PROTECTION_MIGRATED`. INSERT theo lô 500.
3. `held = false` (giữ `held_by`, `held_at` cho downgrade).
4. Kiểm tập con: `held_before` ⊆ clip của phiên làm bằng chứng của hồ sơ `LEGACY_HOLD` chưa đóng; sai → raise
   (cả transaction lùi). Log số đếm + chênh lệch.
5. Drop `phase2_archive` (cuối upgrade 0004 — R3-5).

Downgrade (DEC-252, DEC-270): `CREATE SCHEMA IF NOT EXISTS phase2_archive`; đặt `held = true` cho **mọi** clip đang
được bảo vệ theo ADR-009 (hồ sơ khiếu nại chưa đóng / đóng chưa quá hạn giữ, hồ sơ hàng hoàn chưa kết thúc, +7 ngày
sau khi nhận, "Chỉ hoàn tiền" 30 ngày) để J-02 của code cũ không xóa; ghi danh sách vào
`phase2_archive.downgrade_held_clips`; chép hồ sơ `LEGACY_HOLD` + bằng chứng + ghi chú + gói bằng chứng + liên kết
cảnh báo đối soát (T-120) sang `phase2_archive` rồi xóa khỏi bảng chính. Hồ sơ khác giữ nguyên (downgrade 0003 chép).

Revision ID: 0004
Revises: 0003
"""

import json
import logging
import os
import uuid
from collections import defaultdict
from typing import Any, Sequence, Union
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from alembic import op

from aicam.core.ids import uuid7

revision: str = "0004"
down_revision: Union[str, Sequence[str], None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

BATCH = 500
LEGACY_DEADLINE_DAYS = 30
ARCHIVE = "phase2_archive"
CLAIM_COLUMNS = (
    "id, code, package_id, order_id, return_case_id, type, counterparty, status, source, owner_user_id, "
    "deadline_at, deadline_source, platform_claim_ref, recovered_amount, close_reason, due_soon_notified_at, "
    "created_by, created_at, updated_at, closed_at, version"
)
EVIDENCE_COLUMNS = "id, claim_id, kind, session_id, snapshot_id, auto, added_by, added_at"
NOTE_COLUMNS = "id, claim_id, kind, text, author_user_id, at"
PACK_COLUMNS = "id, claim_id, status, progress, path, sha256, size_bytes, missing, error, created_by, created_at, expires_at"
# Người dùng hệ thống đứng tên cờ giữ do downgrade đặt (G3 M-F5, DEC-338): UI Phase 1 hiện "Giữ bởi <tên>" thay vì
# trống. Không đăng nhập được (`is_active = false`, hash không hợp lệ); upgrade lại xóa khi không còn tham chiếu.
SYSTEM_HOLDER_ID = "00000000-0000-7000-8000-00000000a1c0"
SYSTEM_HOLDER_NAME = "Hệ thống (bảo vệ bằng chứng Phase 2)"
ALLOW_ACTIVE_ENV = "AICAM_MIGRATE_ALLOW_ACTIVE_CONNECTIONS"

# Phiên được bảo vệ theo ADR-009 (cùng điều kiện `media.protection.protected_sessions_sql`, viết lại bằng SQL:
# migration không import code nghiệp vụ). `:floor` = RETENTION_CLIP_MIN_DAYS.
PROTECTED_SESSIONS_SQL = """
WITH cfg AS (
    SELECT GREATEST(retention_clip_days, :floor) AS days FROM setting WHERE id = 1
), cases AS (
    SELECT rc.id, rcp.package_id
    FROM return_case rc JOIN return_case_package rcp ON rcp.return_case_id = rc.id
    WHERE rc.status IN ('EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'MISSING')
       OR (rc.status IN ('RECEIVED_OK', 'RECEIVED_ISSUE')
           AND COALESCE(rc.received_at, rc.updated_at) > now() - interval '7 days')
       OR (rc.status = 'NO_PARCEL' AND COALESCE(rc.reported_at, rc.created_at) > now() - interval '30 days')
)
SELECT ce.session_id
FROM claim_evidence ce JOIN claim c ON c.id = ce.claim_id
WHERE ce.kind = 'SESSION'
  AND (c.status <> 'CLOSED' OR c.closed_at >= now() - (SELECT days FROM cfg) * interval '1 day')
UNION
SELECT s.id FROM session s
WHERE s.package_id IN (SELECT package_id FROM cases)
  AND (s.type = 'RETURN'
       OR (s.type = 'PACK' AND s.status = 'COMPLETED' AND NOT EXISTS (
           SELECT 1 FROM session s2
           WHERE s2.package_id = s.package_id AND s2.type = 'PACK' AND s2.status = 'COMPLETED'
             AND (s2.ended_at > s.ended_at OR (s2.ended_at = s.ended_at AND s2.id > s.id)))))
UNION
SELECT s.id FROM session s WHERE s.return_case_id IN (SELECT id FROM cases)
"""


def _floor() -> int:
    return int(os.environ.get("RETENTION_CLIP_MIN_DAYS", "60"))


def _tz() -> ZoneInfo:
    return ZoneInfo(os.environ.get("TZ_DISPLAY", "Asia/Ho_Chi_Minh"))


def _exists(bind: sa.Connection, name: str) -> bool:
    return bind.execute(sa.text("SELECT to_regclass(:n)"), {"n": f"{ARCHIVE}.{name}"}).scalar() is not None


def _chunks(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    return [rows[i : i + BATCH] for i in range(0, len(rows), BATCH)]


def _insert(bind: sa.Connection, sql: str, rows: list[dict[str, Any]]) -> None:
    for chunk in _chunks(rows):
        bind.execute(sa.text(sql), chunk)


# ---------------------------------------------------------------- upgrade


def _restore_from_archive(bind: sa.Connection) -> tuple[int, int]:
    """Nâng cấp lại sau downgrade: hồ sơ `LEGACY_HOLD` cũ + trả cờ giữ do downgrade đặt (DEC-270, R3-5)."""
    restored = 0
    if _exists(bind, "legacy_claims"):
        restored = bind.execute(
            sa.text(
                f"INSERT INTO claim ({CLAIM_COLUMNS}) SELECT {CLAIM_COLUMNS} FROM {ARCHIVE}.legacy_claims a "
                "WHERE NOT EXISTS (SELECT 1 FROM claim c WHERE c.id = a.id)"
            )
        ).rowcount
        if _exists(bind, "legacy_claim_evidence"):
            bind.execute(
                sa.text(
                    f"INSERT INTO claim_evidence ({EVIDENCE_COLUMNS}) SELECT {EVIDENCE_COLUMNS} "
                    f"FROM {ARCHIVE}.legacy_claim_evidence a "
                    "WHERE NOT EXISTS (SELECT 1 FROM claim_evidence e WHERE e.id = a.id)"
                )
            )
        if _exists(bind, "legacy_claim_notes"):
            bind.execute(
                sa.text(
                    f"INSERT INTO claim_note ({NOTE_COLUMNS}) SELECT {NOTE_COLUMNS} FROM {ARCHIVE}.legacy_claim_notes a "
                    "WHERE NOT EXISTS (SELECT 1 FROM claim_note n WHERE n.id = a.id)"
                )
            )
        if _exists(bind, "legacy_evidence_packs"):  # gói bằng chứng (FK CASCADE) — T-120, DEC-331
            bind.execute(
                sa.text(
                    f"INSERT INTO evidence_pack ({PACK_COLUMNS}) SELECT {PACK_COLUMNS} "
                    f"FROM {ARCHIVE}.legacy_evidence_packs a "
                    "WHERE NOT EXISTS (SELECT 1 FROM evidence_pack e WHERE e.id = a.id)"
                )
            )
        if _exists(bind, "legacy_alert_claims"):  # cảnh báo đối soát trỏ hồ sơ (FK SET NULL)
            bind.execute(
                sa.text(
                    f"UPDATE recon_alert r SET claim_id = a.claim_id FROM {ARCHIVE}.legacy_alert_claims a "
                    "WHERE r.id = a.alert_id AND r.claim_id IS NULL "
                    "AND EXISTS (SELECT 1 FROM claim c WHERE c.id = a.claim_id)"
                )
            )
        # Mã `KN-` khôi phục giữ nguyên → sequence không được cấp lại mã đã dùng.
        bind.execute(
            sa.text(
                "SELECT setval('claim_code_seq', m) FROM ("
                "  SELECT max(substring(code FROM 4)::bigint) AS m FROM claim WHERE code ~ '^KN-[0-9]+$'"
                ") x, claim_code_seq s "
                "WHERE x.m IS NOT NULL AND x.m > CASE WHEN s.is_called THEN s.last_value ELSE s.last_value - 1 END"
            )
        )
    released = 0
    if _exists(bind, "downgrade_held_clips"):
        # Chỉ clip mà cờ giữ chưa bị đổi từ lúc downgrade (người dùng code cũ giữ / bỏ giữ → coi là giữ mới).
        released = bind.execute(
            sa.text(
                f"UPDATE clip c SET held = false, held_by = d.prev_held_by, held_at = d.prev_held_at "
                f"FROM {ARCHIVE}.downgrade_held_clips d "
                "WHERE c.id = d.clip_id AND c.held "
                "AND c.held_by IS NOT DISTINCT FROM d.new_held_by AND c.held_at IS NOT DISTINCT FROM d.new_held_at"
            )
        ).rowcount
        unprotected = bind.execute(
            sa.text(
                f"SELECT count(*) FROM {ARCHIVE}.downgrade_held_clips d JOIN clip c ON c.id = d.clip_id "
                f"WHERE NOT c.held AND c.session_id NOT IN ({PROTECTED_SESSIONS_SQL})"
            ),
            {"floor": _floor()},
        ).scalar()
        if unprotected:
            # Bảo vệ đã hết hạn trong lúc chạy code cũ (vd. +7 ngày sau khi nhận) → theo retention thường.
            log.info("0004: %s clip của downgrade không còn được bảo vệ (đã hết lý do giữ)", unprotected)
    return restored, released


def _kept_holds(bind: sa.Connection) -> str:
    """Nâng cấp lại: clip đã `held` **trước** downgrade (Admin giữ ở Phase 2 — API-42) mà cờ / người / giờ giữ chưa
    đổi khi chạy code cũ → giữ nguyên `held = true`, không tạo `LEGACY_HOLD` (DEC-332). Trả điều kiện SQL loại trừ."""
    if not _exists(bind, "downgrade_preheld_clips"):
        return "true"
    return (
        f"NOT EXISTS (SELECT 1 FROM {ARCHIVE}.downgrade_preheld_clips k WHERE k.clip_id = c.id "
        "AND c.held_by IS NOT DISTINCT FROM k.held_by AND c.held_at IS NOT DISTINCT FROM k.held_at)"
    )


def _migrate_held(bind: sa.Connection) -> list[uuid.UUID]:
    """Clip `held` → hồ sơ `LEGACY_HOLD` (một hồ sơ / kiện). Trả `held_before`."""
    keep = _kept_holds(bind)
    rows = bind.execute(
        sa.text(
            "SELECT c.id, c.session_id, c.camera_role, c.held_by, c.held_at, s.package_id, p.order_id, "
            "u.display_name "
            "FROM clip c JOIN session s ON s.id = c.session_id JOIN package p ON p.id = s.package_id "
            'LEFT JOIN "user" u ON u.id = c.held_by '
            f"WHERE c.held AND c.status <> 'DELETED' AND {keep} "
            "ORDER BY s.package_id, c.held_at NULLS LAST, c.id"
        )
    ).all()
    if rows:
        _guard_no_other_clients(bind)
    by_package: dict[uuid.UUID, list[Any]] = defaultdict(list)
    for row in rows:
        by_package[row.package_id].append(row)
    claims: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    tz = _tz()
    for package_id, clips in by_package.items():
        claim_id = uuid7()
        first = clips[0]
        claims.append(
            {
                "id": claim_id,
                "package_id": package_id,
                "order_id": first.order_id,
                "created_by": first.held_by,
            }
        )
        sessions: dict[uuid.UUID, Any] = {}
        for clip in clips:
            sessions.setdefault(clip.session_id, clip)
        for session_id, clip in sessions.items():
            evidence.append(
                {"id": uuid7(), "claim_id": claim_id, "session_id": session_id, "added_by": clip.held_by}
            )
        holders: dict[tuple[Any, Any], Any] = {}
        for clip in clips:
            holders.setdefault((clip.held_by, clip.held_at), clip)
        for clip in holders.values():
            who = clip.display_name or "người dùng không còn"
            when = clip.held_at.astimezone(tz).strftime("%H:%M %d/%m/%Y") if clip.held_at else "không rõ giờ"
            notes.append(
                {"id": uuid7(), "claim_id": claim_id, "text": f"Chuyển từ cờ giữ của {who} lúc {when}"}
            )
        audits.append(
            {
                "claim_id": claim_id,
                "data": json.dumps(
                    {
                        "package_id": str(package_id),
                        "clip_ids": [str(c.id) for c in clips],
                        "session_ids": [str(s) for s in sessions],
                        "held_by": sorted({str(c.held_by) for c in clips if c.held_by}),
                    }
                ),
            }
        )
    _insert(
        bind,
        "INSERT INTO claim (id, package_id, order_id, type, counterparty, status, source, deadline_at, "
        "deadline_source, created_by, created_at, updated_at, version) VALUES (:id, :package_id, :order_id, "
        f"'OTHER', 'PLATFORM', 'NEW', 'LEGACY_HOLD', now() + interval '{LEGACY_DEADLINE_DAYS} days', 'DEFAULT', "
        ":created_by, now(), now(), 1)",
        claims,
    )
    _insert(
        bind,
        "INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_by, added_at) "
        "VALUES (:id, :claim_id, 'SESSION', :session_id, false, :added_by, now())",
        evidence,
    )
    _insert(
        bind,
        "INSERT INTO claim_note (id, claim_id, kind, text, author_user_id, at) "
        "VALUES (:id, :claim_id, 'SYSTEM', :text, NULL, now())",
        notes,
    )
    _insert(  # audit_log chỉ INSERT → mã hồ sơ (sequence cấp lúc INSERT claim) lấy bằng SELECT
        bind,
        "INSERT INTO audit_log (user_id, action, object_type, object_id, at, data) "
        "SELECT NULL, 'CLIP_PROTECTION_MIGRATED', 'CLAIM', c.id::text, now(), "
        "CAST(:data AS jsonb) || jsonb_build_object('code', c.code) FROM claim c WHERE c.id = :claim_id",
        audits,
    )
    bind.execute(sa.text(f"UPDATE clip c SET held = false WHERE c.held AND {keep}"))
    log.info("0004: %s clip giữ → %s hồ sơ LEGACY_HOLD (%s phiên)", len(rows), len(claims), len(evidence))
    return [row.id for row in rows]


def _check_subset(bind: sa.Connection, held_before: list[uuid.UUID]) -> None:
    """DEC-250: mọi clip giữ trước ∈ tập được bảo vệ sau (theo phiên — tập sau thường lớn hơn)."""
    protected_after = set(
        bind.execute(
            sa.text(
                "SELECT c.id FROM clip c "
                "JOIN claim_evidence ce ON ce.session_id = c.session_id AND ce.kind = 'SESSION' "
                "JOIN claim cl ON cl.id = ce.claim_id "
                "WHERE cl.source = 'LEGACY_HOLD' AND cl.status <> 'CLOSED' AND c.status <> 'DELETED'"
            )
        ).scalars()
    )
    missing = set(held_before) - protected_after
    log.info(
        "0004: held_before=%s protected_after=%s chênh=%s",
        len(held_before),
        len(protected_after),
        len(protected_after) - len(set(held_before)),
    )
    if missing:
        raise RuntimeError(
            f"0004: {len(missing)} clip đang giữ không thuộc tập được bảo vệ sau nâng cấp "
            f"({', '.join(sorted(str(m) for m in missing)[:5])}) — dừng, không đổi dữ liệu (DEC-250)."
        )


def _guard_no_other_clients(bind: sa.Connection) -> None:
    """G3 M-F1 (c), DEC-336: bước 3 bỏ cờ `held` — J-02 của image Phase 1 còn chạy (worker / beat chưa dừng) chỉ biết
    cờ này → xóa clip vừa chuyển thành bằng chứng `LEGACY_HOLD`. Còn kết nối khác vào DB → dừng (cả transaction
    lùi). Ops chắc chắn không còn tiến trình ứng dụng (vd. psql để xem) thì đặt AICAM_MIGRATE_ALLOW_ACTIVE_CONNECTIONS=1."""
    if os.environ.get(ALLOW_ACTIVE_ENV) == "1":
        return
    others = (
        bind.execute(
            sa.text(
                "SELECT coalesce(nullif(application_name, ''), '?') || ' (' || coalesce(host(client_addr), 'local') "
                "|| ')' FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() "
                "AND backend_type = 'client backend' ORDER BY 1"
            )
        )
        .scalars()
        .all()
    )
    if others:
        raise RuntimeError(
            f"0004: còn {len(others)} kết nối khác vào DB ({', '.join(others[:5])}) trong khi có clip đang giữ cần "
            "chuyển thành hồ sơ LEGACY_HOLD — J-02 của bản cũ có thể xóa chúng. Dừng mọi service "
            "(`dc stop api vision worker worker-sync worker-export beat`, docs/ops.md §7.1) rồi chạy lại; không có gì "
            f"bị thay đổi. Chắc chắn không còn tiến trình ứng dụng thì đặt {ALLOW_ACTIVE_ENV}=1."
        )


def _report_deleted_evidence(bind: sa.Connection) -> int:
    """Nâng cấp lại sau downgrade (G3 M-F5): hồ sơ chưa đóng có clip bằng chứng bị xóa trong lúc chạy bản cũ → log
    cảnh báo + audit `EVIDENCE_CLIP_DELETED_DURING_ROLLBACK` từng hồ sơ để CSKH biết bằng chứng nào đã mất."""
    if not _exists(bind, "meta"):
        return 0
    rows = bind.execute(
        sa.text(
            "SELECT cl.id, cl.code, array_agg(DISTINCT c.id::text) AS clip_ids FROM claim cl "
            "JOIN claim_evidence ce ON ce.claim_id = cl.id AND ce.kind = 'SESSION' "
            "JOIN clip c ON c.session_id = ce.session_id "
            "WHERE cl.status <> 'CLOSED' AND c.status = 'DELETED' AND c.deleted_at >= ("
            f"  SELECT (value #>> '{{}}')::timestamptz FROM {ARCHIVE}.meta WHERE key = 'downgraded_at') "
            "GROUP BY cl.id, cl.code ORDER BY cl.code"
        )
    ).all()
    for row in rows:
        bind.execute(
            sa.text(
                "INSERT INTO audit_log (user_id, action, object_type, object_id, at, data) VALUES (NULL, "
                "'EVIDENCE_CLIP_DELETED_DURING_ROLLBACK', 'CLAIM', :id, now(), CAST(:data AS jsonb))"
            ),
            {"id": str(row.id), "data": json.dumps({"code": row.code, "clip_ids": sorted(row.clip_ids)})},
        )
    if rows:
        log.warning(
            "0004: %s hồ sơ có clip bằng chứng bị xóa khi chạy bản cũ: %s",
            len(rows),
            ", ".join(r.code for r in rows),
        )
    return len(rows)


def _drop_system_holder(bind: sa.Connection) -> None:
    bind.execute(
        sa.text("UPDATE clip SET held_by = NULL WHERE held_by = CAST(:u AS uuid) AND NOT held"),
        {"u": SYSTEM_HOLDER_ID},
    )
    left = bind.execute(
        sa.text("SELECT count(*) FROM clip WHERE held_by = CAST(:u AS uuid)"), {"u": SYSTEM_HOLDER_ID}
    ).scalar()
    if left:
        log.warning("0004: %s clip vẫn đứng tên người dùng hệ thống giữ — giữ lại người dùng này", left)
        return
    with bind.begin_nested() as sp:
        try:
            bind.execute(sa.text('DELETE FROM "user" WHERE id = CAST(:u AS uuid)'), {"u": SYSTEM_HOLDER_ID})
        except sa.exc.IntegrityError:
            sp.rollback()
            log.warning("0004: người dùng hệ thống giữ clip còn được tham chiếu — giữ lại")


def upgrade() -> None:
    bind = op.get_bind()
    restored, released = _restore_from_archive(bind)
    if restored or released:
        log.info("0004: khôi phục %s hồ sơ LEGACY_HOLD, trả %s cờ giữ do downgrade đặt", restored, released)
    _report_deleted_evidence(bind)
    _drop_system_holder(bind)
    held_before = _migrate_held(bind)
    _check_subset(bind, held_before)
    op.execute(f"DROP SCHEMA IF EXISTS {ARCHIVE} CASCADE")


# ---------------------------------------------------------------- downgrade


def downgrade() -> None:
    bind = op.get_bind()
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {ARCHIVE}")
    op.execute(
        f"CREATE TABLE IF NOT EXISTS {ARCHIVE}.downgrade_held_clips ("
        "clip_id uuid PRIMARY KEY, prev_held_by uuid, prev_held_at timestamptz, "
        "new_held_by uuid, new_held_at timestamptz)"
    )
    # 0. Clip đã giữ sẵn (API-42 ở Phase 2): nhớ để nâng cấp lại giữ nguyên, không đổi thành LEGACY_HOLD (DEC-332).
    op.execute(
        f"CREATE TABLE IF NOT EXISTS {ARCHIVE}.downgrade_preheld_clips ("
        "clip_id uuid PRIMARY KEY, held_by uuid, held_at timestamptz)"
    )
    op.execute(
        f"INSERT INTO {ARCHIVE}.downgrade_preheld_clips SELECT id, held_by, held_at FROM clip "
        "WHERE held AND status <> 'DELETED' ON CONFLICT (clip_id) DO NOTHING"
    )
    # 1. Mọi clip đang được bảo vệ theo ADR-009 → `held = true` (code cũ chỉ biết cờ giữ), đứng tên người dùng hệ
    #    thống để UI cũ hiện lý do (G3 M-F5); người / giờ giữ cũ lưu ở `downgrade_held_clips`, upgrade trả lại.
    bind.execute(
        sa.text(
            'INSERT INTO "user" (id, username, display_name, role, password_hash, is_active) VALUES '
            "(CAST(:u AS uuid), 'system_phase2_hold', :name, 'SUPERVISOR', '!', false) ON CONFLICT (id) DO NOTHING"
        ),
        {"u": SYSTEM_HOLDER_ID, "name": SYSTEM_HOLDER_NAME},
    )
    held = bind.execute(
        sa.text(
            f"WITH target AS ("
            f"  SELECT c.id, c.held_by, c.held_at FROM clip c "
            f"  WHERE NOT c.held AND c.status <> 'DELETED' AND c.session_id IN ({PROTECTED_SESSIONS_SQL})"
            f"), upd AS ("
            f"  UPDATE clip c SET held = true, held_by = CAST(:holder AS uuid), held_at = now() FROM target t "
            f"  WHERE c.id = t.id RETURNING c.id, t.held_by AS prev_by, t.held_at AS prev_at, c.held_by, c.held_at"
            f") INSERT INTO {ARCHIVE}.downgrade_held_clips "
            f"SELECT id, prev_by, prev_at, held_by, held_at FROM upd ON CONFLICT (clip_id) DO NOTHING"
        ),
        {"floor": _floor(), "holder": SYSTEM_HOLDER_ID},
    ).rowcount
    # 2. Hồ sơ LEGACY_HOLD + bằng chứng + ghi chú → archive rồi xóa (DEC-270). Hồ sơ khác giữ nguyên.
    for table, source, columns, where in (
        ("legacy_claims", "claim", CLAIM_COLUMNS, "source = 'LEGACY_HOLD'"),
        (
            "legacy_claim_evidence",
            "claim_evidence",
            EVIDENCE_COLUMNS,
            "claim_id IN (SELECT id FROM claim WHERE source = 'LEGACY_HOLD')",
        ),
        (
            "legacy_claim_notes",
            "claim_note",
            NOTE_COLUMNS,
            "claim_id IN (SELECT id FROM claim WHERE source = 'LEGACY_HOLD')",
        ),
    ):
        op.execute(f"CREATE TABLE IF NOT EXISTS {ARCHIVE}.{table} (LIKE {source})")
        bind.execute(
            sa.text(
                f"INSERT INTO {ARCHIVE}.{table} ({columns}) SELECT {columns} FROM {source} s WHERE {where} "
                f"AND NOT EXISTS (SELECT 1 FROM {ARCHIVE}.{table} a WHERE a.id = s.id)"
            )
        )
    # Gói bằng chứng (CASCADE) + liên kết cảnh báo đối soát (SET NULL) của hồ sơ LEGACY_HOLD — không mất khi xóa.
    op.execute(f"CREATE TABLE IF NOT EXISTS {ARCHIVE}.legacy_evidence_packs (LIKE evidence_pack)")
    bind.execute(
        sa.text(
            f"INSERT INTO {ARCHIVE}.legacy_evidence_packs ({PACK_COLUMNS}) SELECT {PACK_COLUMNS} FROM evidence_pack s "
            "WHERE claim_id IN (SELECT id FROM claim WHERE source = 'LEGACY_HOLD') "
            f"AND NOT EXISTS (SELECT 1 FROM {ARCHIVE}.legacy_evidence_packs a WHERE a.id = s.id)"
        )
    )
    op.execute(
        f"CREATE TABLE IF NOT EXISTS {ARCHIVE}.legacy_alert_claims (alert_id uuid PRIMARY KEY, claim_id uuid NOT NULL)"
    )
    bind.execute(
        sa.text(
            f"INSERT INTO {ARCHIVE}.legacy_alert_claims (alert_id, claim_id) SELECT r.id, r.claim_id FROM recon_alert r "
            "JOIN claim c ON c.id = r.claim_id WHERE c.source = 'LEGACY_HOLD' ON CONFLICT (alert_id) DO NOTHING"
        )
    )
    archived = bind.execute(sa.text("DELETE FROM claim WHERE source = 'LEGACY_HOLD'")).rowcount
    log.info(
        "0004 downgrade: đặt held cho %s clip được bảo vệ; chuyển %s hồ sơ LEGACY_HOLD vào %s",
        held,
        archived,
        ARCHIVE,
    )
