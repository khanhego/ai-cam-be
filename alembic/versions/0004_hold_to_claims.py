# ruff: noqa: S608 — SQL ghép từ hằng của migration (tên bảng / cột), không có input ngoài
"""hold_to_claims — clip đang "giữ" → hồ sơ khiếu nại `LEGACY_HOLD` (02a §3 Migration 0004, ADR-009, T-111)

Upgrade (một transaction — DEC-250):
1. Nâng cấp lại sau downgrade (R3-5, DEC-270): khôi phục hồ sơ `LEGACY_HOLD` + bằng chứng + ghi chú từ
   `phase2_archive.legacy_claims*`; clip trong `phase2_archive.downgrade_held_clips` mà cờ giữ chưa bị đổi từ lúc
   downgrade → `held = false` (trả `held_by` / `held_at` cũ). Không tạo `LEGACY_HOLD` mới cho các clip đó.
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
`phase2_archive.downgrade_held_clips`; chép hồ sơ `LEGACY_HOLD` + bằng chứng + ghi chú sang `phase2_archive` rồi
xóa khỏi bảng chính. Hồ sơ khác giữ nguyên.

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


def _migrate_held(bind: sa.Connection) -> list[uuid.UUID]:
    """Clip `held` → hồ sơ `LEGACY_HOLD` (một hồ sơ / kiện). Trả `held_before`."""
    rows = bind.execute(
        sa.text(
            "SELECT c.id, c.session_id, c.camera_role, c.held_by, c.held_at, s.package_id, p.order_id, "
            "u.display_name "
            "FROM clip c JOIN session s ON s.id = c.session_id JOIN package p ON p.id = s.package_id "
            'LEFT JOIN "user" u ON u.id = c.held_by '
            "WHERE c.held AND c.status <> 'DELETED' ORDER BY s.package_id, c.held_at NULLS LAST, c.id"
        )
    ).all()
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
    bind.execute(sa.text("UPDATE clip SET held = false WHERE held"))
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


def upgrade() -> None:
    bind = op.get_bind()
    restored, released = _restore_from_archive(bind)
    if restored or released:
        log.info("0004: khôi phục %s hồ sơ LEGACY_HOLD, trả %s cờ giữ do downgrade đặt", restored, released)
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
    # 1. Mọi clip đang được bảo vệ theo ADR-009 → `held = true` (code cũ chỉ biết cờ giữ).
    held = bind.execute(
        sa.text(
            f"WITH target AS ("
            f"  SELECT c.id, c.held_by, c.held_at FROM clip c "
            f"  WHERE NOT c.held AND c.status <> 'DELETED' AND c.session_id IN ({PROTECTED_SESSIONS_SQL})"
            f"), upd AS ("
            f"  UPDATE clip c SET held = true, held_at = COALESCE(c.held_at, now()) FROM target t "
            f"  WHERE c.id = t.id RETURNING c.id, t.held_by AS prev_by, t.held_at AS prev_at, c.held_by, c.held_at"
            f") INSERT INTO {ARCHIVE}.downgrade_held_clips "
            f"SELECT id, prev_by, prev_at, held_by, held_at FROM upd ON CONFLICT (clip_id) DO NOTHING"
        ),
        {"floor": _floor()},
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
    archived = bind.execute(sa.text("DELETE FROM claim WHERE source = 'LEGACY_HOLD'")).rowcount
    log.info(
        "0004 downgrade: đặt held cho %s clip được bảo vệ; chuyển %s hồ sơ LEGACY_HOLD vào %s",
        held,
        archived,
        ARCHIVE,
    )
