"""Đối soát (02a §2 `reconciliation`, §4 API-120..123, §5 BR-10..14, 19, 20, 26, §7 J-14; DEC-226, 255, 262).

- `run_rules` (J-14): bước 1 BR-12 chuyển kiện quá hạn (`SKIP LOCKED`), bước 2 bảy quy tắc set-based
  (`rules.py`), bước 3 diff với cảnh báo mở (tạo / cập nhật / tự đóng), bước 4 WS.
- API-120 danh sách, API-121 đánh dấu đã xử lý, API-123 chạy ngay; API-122 / 131 đóng cảnh báo qua
`close_alert`.
"""

import secrets
import time as time_mod
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import case, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.errors import AppError
from aicam.core.ids import uuid7
from aicam.core.redis import get_redis
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package
from aicam.modules.reconciliation import rules
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.reconciliation.schemas import (
    AlertPackage,
    OpenSummary,
    ReconAlertOut,
    ReconAlertPage,
    Resolution,
    ResolvedBy,
    SummaryOut,
)
from aicam.modules.returns.models import ReturnCase
from aicam.modules.settings.models import Setting
from aicam.modules.users.queries import get_user_ref

log = structlog.get_logger()

RUN_TASK = "reconciliation.run_rules"
RUN_LOCK_KEY = "recon:run"  # J-14 sở hữu (SET NX, giữ suốt lần chạy — R-20)
RUN_LOCK_TTL_S = 600
QUEUED_KEY = "recon:queued"  # gộp các lần kích hoạt sau J-04 / J-06 / J-13 trong 30 giây
SOON_DELAY_S = 30.0
MISSING_LABEL = "Đối soát"
MISSING_BATCH = 200
CHUNK = 1000
_RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"""

RULE_BR = {
    "SHIPPED_NOT_PACKED": "BR-10",
    "CANCELLED_AFTER_PACK": "BR-11",
    "RETURN_OVERDUE": "BR-12",
    "RETURN_UNANNOUNCED": "BR-13",
    "PACKED_NOT_HANDED_OVER": "BR-14",
    "RETURN_DONE_NOT_RECEIVED": "BR-19",
    "UNVERIFIED_STALE": "BR-20",
}


async def lock_alert(session: AsyncSession, alert_id: uuid.UUID) -> ReconAlert | None:
    result: ReconAlert | None = await session.scalar(
        select(ReconAlert)
        .where(ReconAlert.id == alert_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result


def close_alert(
    alert: ReconAlert,
    *,
    action: str,
    note: str | None,
    by: uuid.UUID | None,
    to_status: str | None = None,
    claim_id: uuid.UUID | None = None,
) -> None:
    """`OPEN` → `RESOLVED` (người xử lý). Người gọi đã khóa dòng và kiểm `status == OPEN`."""
    alert.status = "RESOLVED"
    alert.closed_at = clock.now()
    alert.resolution_action = action
    alert.resolution_note = note
    alert.resolved_by = by
    alert.to_status = to_status
    alert.claim_id = claim_id


async def summary(session: AsyncSession) -> OpenSummary:
    rows = (
        await session.execute(
            select(ReconAlert.severity, func.count())
            .where(ReconAlert.status == "OPEN")
            .group_by(ReconAlert.severity)
        )
    ).all()
    return OpenSummary(**{severity: count for severity, count in rows})


async def alert_out(
    session: AsyncSession, alert: ReconAlert, manual_targets: dict[str, tuple[str, ...]]
) -> ReconAlertOut:
    package = await session.get(Package, alert.package_id)
    if package is None:  # FK CASCADE: không xảy ra
        raise RuntimeError(f"Cảnh báo trỏ tới kiện không tồn tại: {alert.package_id}")
    order = await session.get(Order, package.order_id) if package.order_id else None
    resolution = None
    if alert.resolution_action is not None:
        user = await get_user_ref(session, alert.resolved_by) if alert.resolved_by else None
        resolution = Resolution(
            action=alert.resolution_action,
            note=alert.resolution_note,
            by=ResolvedBy(id=user.id, display_name=user.display_name) if user else None,
            at=alert.closed_at,
            to_status=alert.to_status,
            claim_id=alert.claim_id,
        )
    return ReconAlertOut(
        id=alert.id,
        rule=alert.rule,
        br=RULE_BR[alert.rule],
        severity=alert.severity,
        status=alert.status,
        package=AlertPackage(
            id=package.id,
            tracking_number=package.tracking_number,
            warehouse_status=package.warehouse_status,
            platform_status=order.platform_status if order else None,
        ),
        context=alert.context,
        detected_at=alert.detected_at,
        closed_at=alert.closed_at,
        resolution=resolution,
        allowed_status_targets=list(manual_targets.get(package.warehouse_status, ())),
    )


# ---------------------------------------------------------------- API-120 (FR-06.03)

SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
MAX_RANGE_DAYS = 92


def _day_start(day: date, tz: str) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))


def _validate_range(date_from: date | None, date_to: date | None) -> None:
    if date_from and date_to:
        if date_from > date_to:
            raise AppError(
                "VALIDATION_ERROR",
                "Dữ liệu không hợp lệ.",
                422,
                {"fields": {"date_to": "Ngày đến trước ngày từ"}},
            )
        if (date_to - date_from).days > MAX_RANGE_DAYS:
            raise AppError(
                "VALIDATION_ERROR",
                "Dữ liệu không hợp lệ.",
                422,
                {"fields": {"date_to": f"Khoảng ngày tối đa {MAX_RANGE_DAYS} ngày"}},
            )


async def list_alerts(
    session: AsyncSession,
    *,
    tz: str,
    status: str | None,
    severity: str | None,
    rule: str | None,
    package_id: uuid.UUID | None,
    date_from: date | None,
    date_to: date | None,
    page: int,
    page_size: int,
) -> ReconAlertPage:
    """API-120: lọc theo trạng thái / mức / quy tắc / kiện / ngày phát hiện (giờ VN); sắp mức (HIGH trước) rồi
    `detected_at` cũ trước; `summary` = số cảnh báo mở theo mức (một truy vấn `GROUP BY`)."""
    _validate_range(date_from, date_to)
    conds: list[Any] = []
    if status:
        conds.append(ReconAlert.status == status)
    if severity:
        conds.append(ReconAlert.severity == severity)
    if rule:
        conds.append(ReconAlert.rule == rule)
    if package_id:
        conds.append(ReconAlert.package_id == package_id)
    if date_from:
        conds.append(ReconAlert.detected_at >= _day_start(date_from, tz))
    if date_to:
        conds.append(ReconAlert.detected_at < _day_start(date_to + timedelta(days=1), tz))
    total = int(await session.scalar(select(func.count()).select_from(ReconAlert).where(*conds)) or 0)
    order = case(SEVERITY_ORDER, value=ReconAlert.severity, else_=3)
    rows = (
        await session.scalars(
            select(ReconAlert)
            .where(*conds)
            .order_by(order, ReconAlert.detected_at, ReconAlert.id)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    items = [await alert_out(session, a, orders.MANUAL_TRANSITIONS) for a in rows]
    return ReconAlertPage(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        summary=SummaryOut(open=await summary(session)),
    )


# ---------------------------------------------------------------- API-121 (FR-06.03)


def publish_updated(session: AsyncSession, open_summary: OpenSummary, tz: str) -> None:
    """WS-02 `recon.updated` + `report.updated` sau commit (D15, badge, D2)."""
    from aicam.realtime import publish

    data = {"summary": {"open": open_summary.model_dump()}}
    day = clock.now().astimezone(ZoneInfo(tz)).date().isoformat()

    async def _send() -> None:
        await publish.to_dashboard("recon.updated", data)
        await publish.to_dashboard("report.updated", {"date": day})

    after_commit(session, _send)


async def resolve(
    session: AsyncSession, alert_id: uuid.UUID, note: str, *, actor: uuid.UUID, ip: str | None, tz: str
) -> ReconAlertOut:
    """API-121: `OPEN` → `RESOLVED` (`RESOLVE`), ghi chú 1–500; đã đóng → `409 ALREADY_RESOLVED` kèm người /
    lúc đóng. Khóa dòng cảnh báo (`FOR UPDATE`) — hai người cùng xử lý: người sau nhận 409 (TC-06.17)."""
    text_note = " ".join(note.split())
    if not 1 <= len(text_note) <= 500:
        raise AppError(
            "VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"note": "Nhập ghi chú 1–500 ký tự"}}
        )
    alert = await lock_alert(session, alert_id)
    if alert is None:
        raise AppError("NOT_FOUND", "Không tìm thấy cảnh báo.", 404)
    if alert.status != "OPEN":
        user = await get_user_ref(session, alert.resolved_by) if alert.resolved_by else None
        raise AppError(
            "ALREADY_RESOLVED",
            "Cảnh báo này đã được xử lý." if alert.status == "RESOLVED" else "Cảnh báo này đã tự hết.",
            409,
            {
                "status": alert.status,
                "closed_at": clock.iso_z(alert.closed_at) if alert.closed_at else None,
                "resolved_by": {"id": str(user.id), "display_name": user.display_name} if user else None,
            },
        )
    close_alert(alert, action="RESOLVE", note=text_note, by=actor)
    audit.record(
        session,
        "RECON_RESOLVE",
        user_id=actor,
        object_type="RECON_ALERT",
        object_id=alert.id,
        ip=ip,
        data={"rule": alert.rule, "package_id": str(alert.package_id), "note": text_note},
    )
    await session.flush()
    out = await alert_out(session, alert, orders.MANUAL_TRANSITIONS)
    publish_updated(session, await summary(session), tz)
    await commit(session)
    return out


# ---------------------------------------------------------------- API-123 + kích hoạt J-14


async def request_run() -> None:
    """API-123: J-14 đang giữ khóa `recon:run` → `409 RECON_IN_PROGRESS`; không → đẩy J-14 (không tự lấy
    khóa — R-20, DEC-262)."""
    from aicam.modules.media import jobs

    if await get_redis().exists(RUN_LOCK_KEY):
        raise AppError("RECON_IN_PROGRESS", "Đối soát đang chạy.", 409)
    await jobs.send(RUN_TASK, [], "default")


def request_run_soon(session: AsyncSession) -> None:
    """Sau J-04 / J-06 / J-13 có thay đổi: J-14 sau 30 giây; nhiều lần trong 30 giây gộp một (khóa
    `recon:queued`)."""
    from aicam.modules.media import jobs

    async def _send() -> None:
        if await get_redis().set(QUEUED_KEY, "1", nx=True, ex=int(SOON_DELAY_S)):
            await jobs.send(RUN_TASK, [], "default", SOON_DELAY_S)

    after_commit(session, _send)


# ---------------------------------------------------------------- J-14 `run_rules` (FR-06.02, 06.06)


async def _lock_cases_skip_locked(
    session: AsyncSession, case_ids: list[uuid.UUID]
) -> list[ReturnCase] | None:
    if not case_ids:
        return []
    rows = (
        await session.scalars(
            select(ReturnCase)
            .where(ReturnCase.id.in_(case_ids))
            .order_by(ReturnCase.id)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).all()
    return list(rows) if len(rows) == len(case_ids) else None


async def mark_missing(session: AsyncSession, now: datetime, days: int) -> tuple[int, int]:
    """Bước 1 (BR-12, DEC-255): kiện `RETURN_EXPECTED` có `status_changed_at < now − N ngày` →
    `RETURN_MISSING`
    (WAREHOUSE, "Đối soát"); hồ sơ tính lại (BR-24 — `MISSING` chỉ khi chưa kiện nào nhận, không đè
    `PARTIALLY_RECEIVED`). Khóa hồ sơ rồi kiện (DEC-266) bằng `SKIP LOCKED`: dòng đang bị API / job khác giữ →
    bỏ qua lượt này (không chờ, không deadlock). Trả (số kiện, số hồ sơ đổi)."""
    # Import muộn: `returns` → `claims` → `reconciliation` (claims đóng cảnh báo khi tạo hồ sơ từ cảnh báo).
    from aicam.modules.returns import service as returns

    cutoff = now - timedelta(days=days)
    candidates = (
        await session.scalars(
            select(Package.id).where(
                Package.warehouse_status == "RETURN_EXPECTED", Package.status_changed_at < cutoff
            )
        )
    ).all()
    moved = cases_changed = 0
    for i, package_id in enumerate(candidates, 1):
        cases = await _lock_cases_skip_locked(
            session, await returns.open_case_ids_of_package(session, package_id)
        )
        if cases is None:
            continue
        package = await session.scalar(
            select(Package)
            .where(
                Package.id == package_id,
                Package.warehouse_status == "RETURN_EXPECTED",
                Package.status_changed_at < cutoff,
            )
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
        if package is None:
            continue
        await orders.transition(
            session, package, "RETURN_MISSING", source="WAREHOUSE", actor_label=MISSING_LABEL
        )
        moved += 1
        await session.flush()
        for return_case in cases:
            if await returns.recompute(session, return_case):
                returns.notify_updated(session, return_case)
                cases_changed += 1
        if i % MISSING_BATCH == 0:
            await commit(session)
    await commit(session)
    return moved, cases_changed


@dataclass
class ApplyResult:
    created: int = 0
    updated: int = 0
    closed: int = 0
    suppressed: int = 0


async def apply_hits(session: AsyncSession, hits: list[rules.Hit], now: datetime) -> ApplyResult:
    """Bước 3 (BR-26): diff tập vi phạm với cảnh báo mở — mới → INSERT (bỏ qua nếu có `RESOLVED` cùng
    (kiện, quy tắc, `context_key`)) `ON CONFLICT DO NOTHING` trên unique mở; còn → `last_seen_at`, `context`;
    hết → `AUTO_RESOLVED`."""
    out = ApplyResult()
    by_key: dict[tuple[uuid.UUID, str], rules.Hit] = {}
    for found in hits:
        by_key.setdefault((found.package_id, found.rule), found)
    open_rows = (
        await session.execute(
            select(ReconAlert.id, ReconAlert.package_id, ReconAlert.rule).where(ReconAlert.status == "OPEN")
        )
    ).all()
    open_keys: set[tuple[uuid.UUID, str]] = set()
    updates: list[dict[str, Any]] = []
    to_close: list[uuid.UUID] = []
    for alert_id, package_id, rule in open_rows:
        key = (package_id, rule)
        open_keys.add(key)
        hit = by_key.get(key)
        if hit is None:
            to_close.append(alert_id)
        else:
            updates.append(
                {"id": alert_id, "last_seen_at": now, "context": hit.context, "context_key": hit.context_key}
            )
    if updates:
        await session.execute(update(ReconAlert), updates)
        out.updated = len(updates)
    for i in range(0, len(to_close), CHUNK):
        result = await session.execute(
            update(ReconAlert)
            .where(ReconAlert.id.in_(to_close[i : i + CHUNK]), ReconAlert.status == "OPEN")
            .values(status="AUTO_RESOLVED", closed_at=now)
            .execution_options(synchronize_session=False)
        )
        out.closed += int(getattr(result, "rowcount", 0) or 0)
    new = [h for key, h in by_key.items() if key not in open_keys]
    suppressed: set[tuple[uuid.UUID, str, str]] = set()
    for i in range(0, len(new), CHUNK):
        chunk = new[i : i + CHUNK]
        rows = (
            await session.execute(
                select(ReconAlert.package_id, ReconAlert.rule, ReconAlert.context_key).where(
                    ReconAlert.status == "RESOLVED",
                    ReconAlert.package_id.in_({h.package_id for h in chunk}),
                )
            )
        ).all()
        suppressed.update((pid, rule, key) for pid, rule, key in rows)
    values = []
    for hit in new:
        if (hit.package_id, hit.rule, hit.context_key) in suppressed:
            out.suppressed += 1
            continue
        values.append(
            {
                "id": uuid7(),
                "package_id": hit.package_id,
                "rule": hit.rule,
                "severity": rules.RULE_SEVERITY[hit.rule],
                "status": "OPEN",
                "context": hit.context,
                "context_key": hit.context_key,
                "detected_at": now,
                "last_seen_at": now,
            }
        )
    for i in range(0, len(values), CHUNK):
        result = await session.execute(
            insert(ReconAlert)
            .values(values[i : i + CHUNK])
            .on_conflict_do_nothing(
                index_elements=["package_id", "rule"], index_where=text("status = 'OPEN'")
            )
        )
        out.created += int(getattr(result, "rowcount", 0) or 0)
    return out


async def run_rules(session: AsyncSession, settings: Settings) -> dict[str, Any]:
    """J-14 (30 phút; sau J-04 / J-06 / J-13 có thay đổi; API-123). **Sở hữu** khóa `recon:run` (SET NX 600
    giây, giữ suốt lần chạy); không lấy được → bỏ lượt (R-20). `RECON_ENABLED=false` → không làm gì."""
    if not settings.recon_enabled:
        return {"skipped": "disabled"}
    token = secrets.token_hex(8)
    if not await get_redis().set(RUN_LOCK_KEY, token, nx=True, ex=RUN_LOCK_TTL_S):
        return {"skipped": "locked"}
    began = time_mod.monotonic()
    try:
        row = await session.get(Setting, 1)
        now = clock.now()
        params = rules.Params(
            now=now,
            recon_start_at=row.recon_start_at if row else now,
            return_missing_days=row.return_missing_days if row else 7,
            handover_warn_hours=row.handover_warn_hours if row else 24,
        )
        moved, cases_changed = await mark_missing(session, now, params.return_missing_days)
        hits: list[rules.Hit] = []
        for rule in rules.RULES:
            hits.extend(await rule(session, params))
        applied = await apply_hits(session, hits, now)
        changed = bool(moved or applied.created or applied.closed)
        open_summary = await summary(session)
        if changed:
            publish_updated(session, open_summary, settings.tz_display)
        await commit(session)
    except Exception:
        await rollback(session)
        log.exception("recon_run_failed")  # metric `aicam_recon_run_failed_total` (02a §10)
        raise
    finally:
        await get_redis().eval(_RELEASE_IF_OWNER, 1, RUN_LOCK_KEY, token)  # type: ignore[misc]
    out = {
        "missing": moved,
        "cases_changed": cases_changed,
        "hits": len(hits),
        "created": applied.created,
        "updated": applied.updated,
        "auto_resolved": applied.closed,
        "suppressed": applied.suppressed,
    }
    # metric `aicam_recon_run_seconds`, `aicam_recon_alerts_open{severity}` (02a §10) — log có cấu trúc
    log.info(
        "recon_run", seconds=round(time_mod.monotonic() - began, 3), open=open_summary.model_dump(), **out
    )
    return out
