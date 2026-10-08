"""J-26 — điều kiện N01..N09 → `notify_event` (02a §7.5, DEC-443, DEC-473).

Mỗi điều kiện có cửa sổ nhìn lại ≤ 24 giờ nên dọn `notify_event` sau 30 ngày không phát lại (DEC-473).
`dedupe_key` đúng bảng 02a §7.5 — `notify_event` unique (`code`, `dedupe_key`) là chỗ bỏ trùng BR-36 (1).
`data` chỉ chứa trường whitelist của `render.py` (mã kiện / hồ sơ, sàn, shop, station, giờ — FR-06.09).

Module chỉ **đọc** module khác (02 §4.2: `notify → reports, approvals, stations, platforms, claims, returns,
reconciliation, backup, sessions`).
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.backup import service as backup_service
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.claims.models import Claim
from aicam.modules.notify.service import in_quiet
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.reports import service as reports
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.returns.queries import refund_pending_filter, response_due_sql
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.queries import dropped_return_filter
from aicam.modules.settings import service as settings_service
from aicam.modules.stations.models import Camera, Station

log = structlog.get_logger()

LOOKBACK = timedelta(hours=24)
CAMERA_OFFLINE_AFTER = timedelta(seconds=60)  # N01 "mất tín hiệu > 60 giây"
REFUND_REMIND = timedelta(hours=12)  # N04 "còn ≤ 12 giờ"
CLAIM_SOON = timedelta(hours=48)  # N05 "hạn trong 48 giờ"
SHOP_ERROR_AFTER = timedelta(minutes=30)  # N06 "đồng bộ lỗi liên tục > 30 phút"
APPROVAL_WAIT = timedelta(minutes=3)  # N09
DISK_MEDIUM, DISK_HIGH = 80, 90  # N07


@dataclass
class Draft:
    code: str
    severity: str
    dedupe_key: str
    data: dict[str, Any] = field(default_factory=dict)


def _iso(at: datetime | None) -> str | None:
    return clock.iso_z(at) if at else None


def _local_date(at: datetime, tz: str) -> str:
    return at.astimezone(ZoneInfo(tz)).date().isoformat()


Check = Callable[[AsyncSession, Settings, datetime], Awaitable[list[Draft]]]


# ---------------------------------------------------------------- N01 camera


async def n01_cameras(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    """Camera `OFFLINE` > 60 giây (≤ 24 giờ), station đang bật, **ngoài giờ yên lặng** (DEC-444)."""
    cfg = await settings_service.get(db)
    if in_quiet(cfg, now, settings.tz_display):
        return []
    rows = (
        await db.execute(
            select(Camera.id, Camera.role, Camera.last_seen_at, Station.name)
            .join(Station, Station.id == Camera.station_id)
            .where(
                Station.is_active.is_(True),
                Camera.status == "OFFLINE",
                Camera.last_seen_at < now - CAMERA_OFFLINE_AFTER,
                Camera.last_seen_at > now - LOOKBACK,
            )
        )
    ).all()
    return [
        Draft(
            "N01",
            "HIGH",
            f"cam:{cid}:{_iso(seen)}",
            {"camera_id": str(cid), "station": station, "role": role, "since": _iso(seen)},
        )
        for cid, role, seen, station in rows
    ]


# ---------------------------------------------------------------- N02 lệch mức Cao


def _shop_cols() -> tuple[Any, Any]:
    return Shop.platform, Shop.name


async def n02_recon_high(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    rows = (
        await db.execute(
            select(
                ReconAlert.id, ReconAlert.detected_at, ReconAlert.rule, Package.tracking_number, *_shop_cols()
            )
            .join(Package, Package.id == ReconAlert.package_id)
            .outerjoin(Order, Order.id == Package.order_id)
            .outerjoin(Shop, Shop.id == Order.shop_id)
            .where(
                ReconAlert.severity == "HIGH",
                ReconAlert.status == "OPEN",
                ReconAlert.detected_at > now - LOOKBACK,
            )
        )
    ).all()
    return [
        Draft(
            "N02",
            "HIGH",
            f"alert:{aid}",
            {"tracking": tracking, "platform": platform, "shop": shop, "since": _iso(detected), "rule": rule},
        )
        for aid, detected, rule, tracking, platform, shop in rows
    ]


# ---------------------------------------------------------------- N03 phiên hoàn hủy / bỏ dở, kiện hoàn lạ


async def n03_returns(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    """Phiên RETURN `CANCELLED` / `ABANDONED` trừ phiên bị loại theo BR-39 (`dropped_return_filter` —
    lý do hiệu lực quét nhầm / không phải hàng hoàn, hoặc đã đánh dấu quét nhầm); hồ sơ `UNIDENTIFIED` mới."""
    out: list[Draft] = []
    sessions = (
        await db.execute(
            select(
                PackSession.id,
                PackSession.status,
                PackSession.ended_at,
                Package.tracking_number,
                Station.name,
            )
            .join(Package, Package.id == PackSession.package_id)
            .join(Station, Station.id == PackSession.station_id)
            .where(dropped_return_filter(), PackSession.ended_at > now - LOOKBACK)
        )
    ).all()
    for sid, status, ended, tracking, station in sessions:
        out.append(Draft("N03", "MEDIUM", f"sess:{sid}", {"kind": "SESSION", "tracking": tracking,
                                                          "station": station, "status": status,
                                                          "at": _iso(ended)}))  # fmt: skip
    placeholder = aliased(Package)
    cases = (
        await db.execute(
            select(
                ReturnCase.id,
                ReturnCase.created_at,
                func.coalesce(func.min(placeholder.tracking_number), ReturnCase.return_tracking_number),
            )
            .outerjoin(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
            .outerjoin(placeholder, placeholder.id == ReturnCasePackage.package_id)
            .where(
                ReturnCase.kind == "UNIDENTIFIED",
                ReturnCase.status != "CANCELLED",
                ReturnCase.created_at > now - LOOKBACK,
            )
            .group_by(ReturnCase.id)
        )
    ).all()
    for cid, created, tracking in cases:
        out.append(Draft("N03", "MEDIUM", f"case:{cid}", {"kind": "CASE", "tracking": tracking,
                                                          "at": _iso(created)}))  # fmt: skip
    return out


# ---------------------------------------------------------------- N04 Chỉ hoàn tiền (BR-40)


async def n04_refund_only(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    hours = (await settings_service.get(db)).refund_only_default_hours
    due = response_due_sql(hours).label("due")
    reported = func.coalesce(ReturnCase.reported_at, ReturnCase.created_at)
    rows = (
        await db.execute(
            select(ReturnCase.id, ReturnCase.code, reported, due, *_shop_cols())
            .outerjoin(Shop, Shop.id == ReturnCase.shop_id)
            .where(refund_pending_filter())
        )
    ).all()
    out: list[Draft] = []
    for rid, code, reported_at, due_at, platform, shop in rows:
        data = {"case_code": code, "platform": platform, "shop": shop, "due": _iso(due_at)}
        if reported_at is not None and reported_at > now - LOOKBACK:
            out.append(Draft("N04", "HIGH", f"refund:{rid}:new", {**data, "stage": "NEW"}))
        if due_at is not None and now - LOOKBACK < due_at <= now + REFUND_REMIND:
            out.append(Draft("N04", "HIGH", f"refund:{rid}:12h", {**data, "stage": "DUE_12H"}))
    return out


# ---------------------------------------------------------------- N05 hồ sơ khiếu nại sắp / quá hạn (BR-42)


async def n05_claims(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    rows = (
        await db.execute(
            select(Claim.id, Claim.code, Claim.type, Claim.deadline_at).where(
                Claim.status == "NEW",
                Claim.deadline_at > now - LOOKBACK,
                Claim.deadline_at <= now + CLAIM_SOON,
            )
        )
    ).all()
    out: list[Draft] = []
    for cid, code, kind, deadline in rows:
        if deadline is None:
            continue
        stage = "OVERDUE" if deadline < now else "SOON"
        key = f"claim:{cid}:{stage.lower()}:{_iso(deadline)}"
        out.append(Draft("N05", "HIGH", key, {"claim_code": code, "claim_type": kind, "due": _iso(deadline),
                                              "stage": stage}))  # fmt: skip
    return out


# ---------------------------------------------------------------- N06 shop hết hạn / đồng bộ lỗi


async def n06_shops(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    """`EXPIRED` (mốc = `last_error.at`) / lỗi liên tục từ `error_since` > 30 phút (DEC-467). Shop đã ngắt
    bỏ qua."""
    shops = (await db.scalars(select(Shop).where(Shop.auth_status.in_(("CONNECTED", "EXPIRED"))))).all()
    out: list[Draft] = []
    for shop in shops:
        err = shop.last_error or {}
        base = {"shop": shop.name or shop.platform_shop_id, "platform": shop.platform}
        if shop.auth_status == "EXPIRED":
            at = err.get("at") if isinstance(err.get("at"), str) else None
            at_dt = datetime.fromisoformat(at.replace("Z", "+00:00")) if at else shop.auth_expires_at
            if at_dt is None or at_dt > now - LOOKBACK:
                out.append(Draft("N06", "HIGH", f"shop:{shop.id}:expired:{at or _iso(at_dt)}",
                                 {**base, "stage": "EXPIRED", "since": at or _iso(at_dt)}))  # fmt: skip
            continue
        since = shop.error_since
        if shop.last_error and since is not None and now - LOOKBACK < since < now - SHOP_ERROR_AFTER:
            out.append(Draft("N06", "HIGH", f"shop:{shop.id}:err:{_iso(since)}",
                             {**base, "stage": "ERROR", "since": _iso(since),
                              "error_code": err.get("code")}))  # fmt: skip
    return out


# ---------------------------------------------------------------- N07 ổ đĩa


async def n07_disk(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    disk = reports.disk_usage(settings)
    if not disk:
        return []
    percent = int(disk["percent"])
    day = _local_date(now, settings.tz_display)
    if percent >= DISK_HIGH:
        return [Draft("N07", "HIGH", f"disk:90:{day}", {"percent": percent})]
    if percent >= DISK_MEDIUM:
        return [Draft("N07", "MEDIUM", f"disk:80:{day}", {"percent": percent})]
    return []


# ---------------------------------------------------------------- N08 sao lưu (5 lý do)


async def _second_failed_run(db: AsyncSession) -> tuple[int, Any]:
    """Chuỗi lượt DB `FAILED` liền nhất (mới nhất trước) và id **lượt lỗi thứ 2** của chuỗi (ổn định khi chuỗi
    dài thêm — dedupe `backup:db2:{run_id}`, DEC-500)."""
    rows = (
        await db.execute(
            select(BackupRun.id, BackupRun.status)
            .where(BackupRun.kind == "DB", BackupRun.status.in_(("SUCCESS", "FAILED")))
            .order_by(BackupRun.started_at.desc())
            .limit(50)
        )
    ).all()
    streak = []
    for run_id, status in rows:
        if status != "FAILED":
            break
        streak.append(run_id)
    if len(streak) < 2:
        return len(streak), None
    return len(streak), streak[-2]  # theo thời gian: lượt lỗi thứ nhất = streak[-1], thứ hai = streak[-2]


async def n08_backup(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    """DB > 26 giờ (`DB_LATE`), 2 lượt DB liền lỗi (`DB_FAILED_TWICE`), tệp chờ > 24 giờ (`EVIDENCE_LATE`),
    lệch mã băm (`HASH_MISMATCH`), không thấy tệp tại kho (`SOURCE_MISSING`) — cùng điều kiện mục D2
    `BACKUP_STALE` (chỉ khi sao lưu `ON` / `KEY_CHANGED` — DEC-657)."""
    cfg = await settings_service.get(db)
    if backup_service.state(cfg, settings) not in (backup_service.ON, backup_service.KEY_CHANGED):
        return []
    out: list[Draft] = []
    stats = await backup_service.db_stats(db)
    late, hours = backup_service.db_late(cfg, stats, now)
    if late:
        ref = stats.last_success_at or cfg.backup_confirmed_at
        out.append(
            Draft("N08", "HIGH", f"backup:db:{_iso(ref)}", {"reason": "DB_LATE", "hours": int(hours or 0)})
        )
    failures, second = await _second_failed_run(db)
    if second is not None:
        out.append(
            Draft("N08", "HIGH", f"backup:db2:{second}", {"reason": "DB_FAILED_TWICE", "count": failures})
        )
    ev = await backup_service.evidence_stats(db, now)
    if ev.late_count:
        day = _local_date(now, settings.tz_display)
        out.append(
            Draft("N08", "HIGH", f"backup:ev:{day}", {"reason": "EVIDENCE_LATE", "count": ev.late_count})
        )
    objs = (
        await db.execute(
            select(BackupObject.id, BackupObject.status, BackupObject.kind).where(
                BackupObject.updated_at > now - LOOKBACK,
                (BackupObject.status == "HASH_MISMATCH")
                | ((BackupObject.status == "FAILED") & (BackupObject.last_error == "SOURCE_MISSING")),
            )
        )
    ).all()
    for oid, status, kind in objs:
        if status == "HASH_MISMATCH":
            out.append(Draft("N08", "HIGH", f"backup:hash:{oid}", {"reason": "HASH_MISMATCH", "kind": kind}))
        else:
            out.append(
                Draft("N08", "HIGH", f"backup:srcmiss:{oid}", {"reason": "SOURCE_MISSING", "kind": kind})
            )
    return out


# ---------------------------------------------------------------- N09 yêu cầu duyệt chờ lâu


async def n09_approvals(db: AsyncSession, settings: Settings, now: datetime) -> list[Draft]:
    rows = (
        await db.execute(
            select(ApprovalRequest.id, ApprovalRequest.type, ApprovalRequest.created_at, Station.name)
            .join(Station, Station.id == ApprovalRequest.station_id)
            .where(
                ApprovalRequest.status == "PENDING",
                ApprovalRequest.created_at < now - APPROVAL_WAIT,
                ApprovalRequest.created_at > now - LOOKBACK,
            )
        )
    ).all()
    return [
        Draft(
            "N09",
            "MEDIUM",
            f"appr:{aid}",
            {"station": station, "approval_type": kind, "since": _iso(created)},
        )
        for aid, kind, created, station in rows
    ]


CHECKS: tuple[tuple[str, Check], ...] = (
    ("N01", n01_cameras),
    ("N02", n02_recon_high),
    ("N03", n03_returns),
    ("N04", n04_refund_only),
    ("N05", n05_claims),
    ("N06", n06_shops),
    ("N07", n07_disk),
    ("N08", n08_backup),
    ("N09", n09_approvals),
)


async def collect(db: AsyncSession, settings: Settings, now: datetime) -> tuple[list[Draft], list[str]]:
    """Chạy mọi điều kiện; một điều kiện lỗi (vd bảng đang khóa) không chặn điều kiện khác (log, lượt sau)."""
    drafts: list[Draft] = []
    failed: list[str] = []
    for code, check in CHECKS:
        try:
            async with db.begin_nested():
                drafts.extend(await check(db, settings, now))
        except Exception as exc:  # cô lập từng điều kiện
            log.warning("notify_condition_failed", code=code, error=type(exc).__name__)
            failed.append(code)
    return drafts, failed
