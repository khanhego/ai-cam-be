"""Nhánh `work_mode = RETURN` của API-11 (02a §4.1, 02 §6.2 API-11; BR-07, BR-23, BR-24, BR-28; DEC-266).

Hai bước như Phase 1 `_lookup_platform`:
1. `prepare()` — **ngoài** khóa station: `returns.resolve_code`, tra sàn ≤ 2 giây khi không thấy, upsert đơn,
   gộp hồ sơ chưa xác định; rồi lấy `order:{sn}` của đơn đích (DEC-266: đơn trước station).
2. `handle()` — **trong** khóa station: kiểm lại dưới khóa (khóa hồ sơ → kiện), mở / cảnh báo / đóng.
"""

import asyncio
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAdapter, PlatformError, PlatformOrder
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession
from aicam.modules.sessions.schemas import AlertOut, ClosedSessionOut
from aicam.modules.stations.models import Camera, Station

log = structlog.get_logger()

WAREHOUSE_LABELS = {
    "NEW": "Mới",
    "PACKING": "Đang đóng gói",
    "PACKED": "Đã đóng gói",
    "HANDED_OVER": "Đã bàn giao",
    "DELIVERED": "Đã giao",
    "CANCELLED": "Đã hủy",
    "CANCELLED_AFTER_PACK": "Hủy sau khi đóng",
}
CONCLUSION_LABELS = {
    "OK": "Nguyên vẹn",
    "DAMAGED": "Hư hỏng",
    "MISSING_ITEM": "Thiếu hàng",
    "WRONG_ITEM": "Sai hàng / bị tráo",
    "EMPTY_BOX": "Hộp rỗng",
    "OTHER": "Khác",
}


def _alert(alert_code: str, message: str, /, **data: object) -> AlertOut:
    return AlertOut(code=alert_code, message=message, data=data)


def is_valid_code(code: str, settings: Settings) -> bool:
    """Mã vận đơn (Phase 1) hoặc mã đơn sàn `ORDER_SN_REGEX` (02 API-11 `INVALID_CODE`)."""
    return bool(re.fullmatch(settings.scan_code_regex, code) or re.fullmatch(settings.order_sn_regex, code))


# ---------------------------------------------------------------- tra sàn (ngoài khóa)


async def platform_find(
    session: AsyncSession, code: str, adapter: PlatformAdapter, settings: Settings
) -> Order | None:
    """Tra sàn ≤ `PLATFORM_LOOKUP_TIMEOUT_S` theo mã vận đơn hoặc mã đơn; có → upsert đơn (savepoint)."""
    try:
        target = await platforms.lookup_target(session, adapter, settings)
        if target is None:
            return None

        async def _find() -> PlatformOrder | None:
            if re.fullmatch(settings.order_sn_regex, code):
                found = await adapter.get_order(target.creds, code)
                if found is not None:
                    return found
            return await adapter.find_by_tracking(target.creds, code)

        data = await asyncio.wait_for(_find(), timeout=settings.platform_lookup_timeout_s)
    except (TimeoutError, PlatformError) as exc:
        log.info("return_platform_lookup_failed", code=code, error=type(exc).__name__)
        return None
    except SQLAlchemyError:
        raise
    except Exception:  # tra sàn lỗi bất ngờ không làm quét 500 (như Phase 1 G3-P2-6)
        log.exception("return_platform_lookup_error", code=code)
        return None
    if data is None:
        return None
    try:
        async with session.begin_nested():
            result = await orders.upsert_platform_order(session, data, shop_id=target.shop_id)
            await returns.merge_unidentified_by_code(session, result.order)
    except IntegrityError:
        return None
    return result.order


async def lookup_with_platform(
    session: AsyncSession, code: str, adapter: PlatformAdapter, settings: Settings
) -> tuple[returns.Resolution, bool]:
    """`resolve_code`; không thấy + mã ≥ 8 ký tự → tra sàn rồi tra lại. Trả (kết quả, đã tra sàn)."""
    found = await returns.resolve_code(session, code)
    if found.status != "NOT_FOUND" or len(code) < 8:
        return found, False
    order = await platform_find(session, code, adapter, settings)
    await session.flush()
    if order is None:
        return found, True
    return await returns.resolve_code(session, code), True


@dataclass
class Prepared:
    resolution: returns.Resolution | None = None
    platform_checked: bool = False


async def prepare(
    session: AsyncSession, station: Station, code: str, adapter: PlatformAdapter, settings: Settings
) -> Prepared:
    """Bước ngoài khóa station. Station đang có phiên: chỉ khóa đơn chờ gộp của hồ sơ (R3-2)."""
    active: PackSession | None = await session.scalar(
        select(PackSession).where(
            PackSession.station_id == station.id, PackSession.status.in_(ACTIVE_STATUSES)
        )
    )
    if active is not None:
        if active.return_case_id is not None:
            case = await session.get(ReturnCase, active.return_case_id)
            await _lock_order_of(session, case.pending_merge_order_id if case else None)
            await _lock_order_of(session, case.order_id if case else None)
        return Prepared()
    if not is_valid_code(code, settings):
        return Prepared()
    resolution, checked = await lookup_with_platform(session, code, adapter, settings)
    if resolution.order is not None:
        await orders.lock_orders(session, [resolution.order.platform_order_sn])
    return Prepared(resolution, checked)


async def lock_case_orders(session: AsyncSession, case_id: uuid.UUID) -> None:
    """Khóa `order:{sn}` của hồ sơ + đơn chờ gộp (R3-2) — gọi **trước** khóa station (DEC-266)."""
    case = await session.get(ReturnCase, case_id)
    if case is None:
        return
    await _lock_order_of(session, case.pending_merge_order_id)
    await _lock_order_of(session, case.order_id)


async def _lock_order_of(session: AsyncSession, order_id: uuid.UUID | None) -> None:
    if order_id is None:
        return
    order = await session.get(Order, order_id)
    if order is not None:
        await orders.lock_orders(session, [order.platform_order_sn])


# ---------------------------------------------------------------- kiểm mở được (02a §4.1 check_openable)


def _hhmm(at: datetime | None, tz: str) -> str:
    return at.astimezone(ZoneInfo(tz)).strftime("%H:%M") if at else ""


async def _station_name(session: AsyncSession, station_id: uuid.UUID) -> str | None:
    station = await session.get(Station, station_id)
    return station.name if station else None


async def check_openable(
    session: AsyncSession, package: Package, case: ReturnCase | None, order: Order | None, code: str, tz: str
) -> AlertOut | None:
    """Trả ALERT nếu kiện không mở được phiên hoàn; None = mở được. Dùng chung API-11 / API-104 / API-105."""
    if package.is_placeholder:
        return _alert(
            "RETURN_NOT_FOUND", f"Không có đơn nào khớp mã {code}.", code=code, can_open_unidentified=True
        )
    active = await session.scalar(
        select(PackSession).where(
            PackSession.package_id == package.id, PackSession.status.in_(ACTIVE_STATUSES)
        )
    )
    if (
        active is None
        and case is not None
        and case.status == "INSPECTING"
        and await returns.is_single_session(session, case)
    ):  # hồ sơ một phiên đang kiểm ở kiện khác của hồ sơ
        active = await session.scalar(
            select(PackSession).where(
                PackSession.return_case_id == case.id, PackSession.status.in_(ACTIVE_STATUSES)
            )
        )
    if active is not None or package.warehouse_status == "RETURN_INSPECTING":
        name = await _station_name(session, active.station_id) if active else None
        return _alert(
            "RETURN_IN_PROGRESS_ELSEWHERE",
            f"{code} đang được kiểm tại {name}." if name else f"{code} đang được kiểm ở station khác.",
            station_name=name,
        )
    if package.warehouse_status in returns.RECEIVED_STATUSES:
        done: PackSession | None = await session.scalar(
            select(PackSession)
            .where(
                PackSession.package_id == package.id,
                PackSession.type == "RETURN",
                PackSession.status == "COMPLETED",
            )
            .order_by(PackSession.ended_at.desc())
            .limit(1)
        )
        name = await _station_name(session, done.station_id) if done else None
        conclusion = done.inspection_conclusion if done else None
        label = CONCLUSION_LABELS.get(conclusion or "", "")
        when = f" lúc {_hhmm(done.ended_at, tz)}" if done and done.ended_at else ""
        where = f" tại {name}" if name else ""
        return _alert(
            "RETURN_ALREADY_RECEIVED",
            f"{code} đã nhận{when}{where}" + (f" — {label}." if label else "."),
            received_at=clock.iso_z(done.ended_at) if done and done.ended_at else None,
            station_name=name,
            conclusion=conclusion,
            can_record_other=True,
        )
    has_open_case = case is not None and case.status in OPEN_CASE_STATUSES
    if package.warehouse_status in returns.OPENABLE_STATUSES or (
        package.warehouse_status == "NEW"
        and (
            has_open_case
            or (order is not None and (order.platform_status or "") in returns.SHIPPED_PLATFORM_STATUSES)
        )
    ):
        return None
    label = WAREHOUSE_LABELS.get(package.warehouse_status, package.warehouse_status)
    return _alert(
        "NOT_SHIPPED",
        f"{code} đang ở trạng thái {label} trong kho. Đây không phải hàng hoàn. "
        "Nếu kiện thực sự đã gửi đi, báo quản lý điều chỉnh trạng thái.",
        warehouse_status=package.warehouse_status,
    )


# ---------------------------------------------------------------- mở phiên (02a §4.1 open_return_session)


async def effective_pack_session(session: AsyncSession, package_id: uuid.UUID) -> PackSession | None:
    """Phiên PACK hiệu lực của kiện: phiên `COMPLETED` mới nhất (phiên bị thay đã `SUPERSEDED`)."""
    result: PackSession | None = await session.scalar(
        select(PackSession)
        .where(
            PackSession.package_id == package_id,
            PackSession.type == "PACK",
            PackSession.status == "COMPLETED",
        )
        .order_by(PackSession.ended_at.desc())
        .limit(1)
    )
    return result


async def _case_for_open(
    session: AsyncSession, station: Station, package: Package, case: ReturnCase | None, order: Order | None
) -> ReturnCase:
    if case is not None:
        await returns.link_package(session, case, package.id)
        return case
    if order is not None:
        result = await returns.attach_or_create(
            session,
            order,
            returns.Signal(returns.SIGNAL_WAREHOUSE_SCAN, package_ids=(package.id,)),
            actor_label=station.name,
        )
        if result.case is not None:
            await returns.link_package(session, result.case, package.id)
            return result.case
    # Kiện có mã thật nhưng chưa gắn đơn (chưa xác minh — BR-04): hồ sơ "Chưa xác định" trên chính kiện đó.
    created = ReturnCase(
        order_id=None, kind="UNIDENTIFIED", status="EXPECTED", source="WAREHOUSE", signal_keys=[],
        requested_items=[], single_session=True,
    )  # fmt: skip
    session.add(created)
    await session.flush()
    await returns.link_package(session, created, package.id)
    return created


async def open_return_session(
    session: AsyncSession,
    station: Station,
    package: Package,
    case: ReturnCase | None,
    order: Order | None,
    code: str,
    *,
    extra_flags: tuple[str, ...] = (),
    note: str | None = None,
) -> PackSession:
    """Mở phiên RETURN (người gọi giữ: `order:{sn}` → station → hồ sơ → kiện, đã `check_openable`)."""
    from aicam.modules.sessions import inspection
    from aicam.modules.sessions.events import record_event

    case = await _case_for_open(session, station, package, case, order)
    if case.single_session is None:  # chốt lúc mở phiên đầu (R3-10)
        case.single_session = await returns.is_single_session(session, case)
    flags: list[str] = list(extra_flags)
    if case.kind == "UNANNOUNCED" and case.platform_return_sn is None:
        flags.append("UNANNOUNCED")
    if case.kind == "UNIDENTIFIED":
        flags.append("UNIDENTIFIED")
    if await effective_pack_session(session, package.id) is None:
        flags.append("NO_PACK_CLIP")
    pack = PackSession(
        type="RETURN",
        package_id=package.id,
        station_id=station.id,
        return_case_id=case.id,
        status="OPEN",
        started_at=clock.now(),
        open_code=code,
        package_status_before=package.warehouse_status,
        operator_name=station.operator_name,
        flags=list(dict.fromkeys(flags)),
        note=note,
    )
    session.add(pack)
    await orders.transition(
        session, package, "RETURN_INSPECTING", source="WAREHOUSE", actor_label=station.name
    )
    await session.flush()
    await inspection.init_lines(session, pack, case, single=bool(case.single_session))
    await returns.recompute(session, case)
    record_event(session, pack, "SCAN_OPEN", code=code, return_case_id=str(case.id))
    await session.flush()
    returns.notify_updated(session, case)
    return pack


# ---------------------------------------------------------------- API-11 trong khóa station


async def _open_from_resolution(
    session: AsyncSession, station: Station, code: str, resolution: returns.Resolution, settings: Settings
) -> tuple[str, AlertOut | None]:
    if resolution.status == "NOT_FOUND" or (resolution.package is None and resolution.status != "MULTIPLE"):
        return "ALERT", _alert(
            "RETURN_NOT_FOUND",
            f"Không có đơn nào khớp mã {code}, sàn không trả lời.",
            code=code,
            can_open_unidentified=True,
        )
    if resolution.status == "MULTIPLE" or resolution.package is None:
        sn = resolution.order.platform_order_sn if resolution.order else code
        count = len(await returns.packages_of_order(session, resolution.order.id)) if resolution.order else 0
        return "ALERT", _alert(
            "RETURN_MULTIPLE_PACKAGES",
            f"Đơn {sn} có {count} kiện. Chọn đúng kiện đang cầm.",
            platform_order_sn=sn,
        )
    package_id = resolution.package.id
    case_ids = (
        [resolution.case.id]
        if resolution.case
        else await returns.open_case_ids_of_package(session, package_id)
    )
    cases = await returns.lock_cases(session, case_ids)
    case = cases[0] if cases else None
    locked = await returns.lock_packages(session, [package_id])
    if not locked:
        return "ALERT", _alert("RETURN_NOT_FOUND", f"Không có đơn nào khớp mã {code}.", code=code,
                               can_open_unidentified=True)  # fmt: skip
    package = locked[0]
    order = await session.get(Order, package.order_id) if package.order_id else None
    alert = await check_openable(session, package, case, order, code, settings.tz_display)
    if alert is not None:
        return "ALERT", alert
    try:
        async with session.begin_nested():
            await open_return_session(session, station, package, case, order, code)
    except IntegrityError:  # station khác vừa mở cùng kiện (partial unique) — 02a §6
        return "ALERT", _alert(
            "RETURN_IN_PROGRESS_ELSEWHERE", f"{code} đang được kiểm ở station khác.", station_name=None
        )
    return "SESSION_OPENED", None


async def _in_session(
    session: AsyncSession, station: Station, pack: PackSession, code: str, settings: Settings
) -> tuple[str, AlertOut | None, ClosedSessionOut | None]:
    """Đang kiểm: mã thuộc hồ sơ (BR-23) + đã có kết luận (BR-07) → đóng; khác → cảnh báo, giữ phiên."""
    case = await returns.lock_case(session, pack.return_case_id) if pack.return_case_id else None
    if case is None:  # dữ liệu lỗi: phiên RETURN mất hồ sơ — không đóng bừa
        log.error("return_session_without_case", session_id=str(pack.id))
        return "IGNORED", None, None
    codes = await returns.accepted_codes(session, case, pack.open_code)
    if code not in codes:
        return (
            "ALERT",
            _alert(
                "RETURN_CODE_DIFFERENT",
                f"Mã {code} không thuộc kiện đang kiểm. Quét lại mã trên kiện này để hoàn tất.",
                code=code,
                expected_codes=codes,
            ),
            None,
        )
    if pack.inspection_conclusion is None:
        return "ALERT", _alert("INSPECTION_REQUIRED", "Chọn kết luận trước khi quét đóng."), None
    closed = await close_return_session(session, pack, case, code=code, actor_label=station.name)
    return "SESSION_COMPLETED", None, closed


async def handle(
    session: AsyncSession,
    station: Station,
    pack: PackSession | None,
    code: str,
    prepared: Prepared | None,
    settings: Settings,
) -> tuple[str, AlertOut | None, ClosedSessionOut | None]:
    """Trong khóa station, station đã đọc lại (`work_mode = RETURN`), không có yêu cầu duyệt chờ."""
    if pack is not None:
        if pack.type != "RETURN" or pack.status != "OPEN":
            return "IGNORED", None, None
        return await _in_session(session, station, pack, code, settings)
    outcome, alert = await _open(session, station, code, prepared, settings)
    return outcome, alert, None


async def _open(
    session: AsyncSession, station: Station, code: str, prepared: Prepared | None, settings: Settings
) -> tuple[str, AlertOut | None]:
    if not is_valid_code(code, settings):
        return "ALERT", _alert(
            "INVALID_CODE", "Mã vừa quét không phải mã vận đơn / mã đơn. Quét lại mã trên kiện."
        )
    if not station.operator_name:
        return "ALERT", _alert("OPERATOR_REQUIRED", "Nhập tên người kiểm trước khi nhận hàng hoàn.")
    resolution = prepared.resolution if prepared and prepared.resolution else None
    if resolution is None:  # chế độ vừa đổi giữa lúc quét: tra trong khóa, không tra sàn
        resolution = await returns.resolve_code(session, code)
    return await _open_from_resolution(session, station, code, resolution, settings)


# ---------------------------------------------------------------- đóng / hủy / bỏ dở (T-108)


async def camera_clock(session: AsyncSession, station_id: uuid.UUID) -> list[dict[str, object]]:
    """Độ lệch giờ camera lúc đóng phiên cho `info.json` (DEC-261); `checked_at` null (J-09 không lưu mốc)."""
    cameras = (
        await session.scalars(select(Camera).where(Camera.station_id == station_id).order_by(Camera.role))
    ).all()
    return [
        {"camera_role": c.role, "clock_offset_ms": c.clock_offset_ms, "checked_at": None} for c in cameras
    ]


async def close_return_session(
    session: AsyncSession,
    pack: PackSession,
    case: ReturnCase,
    *,
    code: str | None,
    actor_label: str,
    auto: bool = False,
) -> ClosedSessionOut:
    """Đóng phiên RETURN đã có kết luận (API-11 quét mã cùng hồ sơ; J-07 tự hoàn tất `code = None`).

    Người gọi giữ: station → hồ sơ (FOR UPDATE). Khóa kiện của hồ sơ (id tăng), kiện của phiên
    `→ RETURN_RECEIVED_*`; hồ sơ một phiên chuyển kiện khác theo BR-24 / DEC-271; hồ sơ chờ gộp (R3-2) gộp
    ngay; `recompute`; J-01 sau commit. Hồ sơ khiếu nại tự tạo (BR-08) nối ở T-110 (`claim_code` null tới đó).
    """
    from aicam.modules.media import jobs as media_jobs
    from aicam.modules.sessions.events import record_event, set_flag

    conclusion = pack.inspection_conclusion or "OTHER"
    package_ids = [p.id for p in await returns.packages_of_case(session, case.id)]
    locked = {p.id: p for p in await returns.lock_packages(session, {*package_ids, pack.package_id})}
    package = locked[pack.package_id]
    pack.camera_clock = await camera_clock(session, pack.station_id)
    pack.status = "COMPLETED"
    pack.ended_at = clock.now()
    pack.close_code = code
    if auto:
        set_flag(pack, "AUTO_CLOSED")
    await orders.transition(
        session, package, returns.received_status(conclusion), source="WAREHOUSE", actor_label=actor_label
    )
    if case.single_session:
        await returns.apply_close_to_packages(
            session, case, package.id, conclusion, source="WAREHOUSE", actor_label=actor_label
        )
    await session.flush()
    if case.pending_merge_order_id is not None and case.order_id is None:
        order = await session.get(Order, case.pending_merge_order_id)
        if order is not None:
            await returns.merge_unidentified(session, case, order, pack.open_code, actor_label=actor_label)
            await session.refresh(pack, ["package_id", "return_case_id"])  # UPDATE hàng loạt khi gộp
    destination = await session.get(ReturnCase, case.merged_into_id) if case.merged_into_id else case
    target = destination or case
    await returns.recompute(session, target)
    record_event(
        session, pack, "AUTO_CLOSED" if auto else "COMPLETED", close_code=code, conclusion=conclusion
    )
    media_jobs.enqueue_build_clips(session, pack.id, pack.ended_at)
    returns.notify_updated(session, target)
    await session.flush()
    refreshed = await session.get(Package, pack.package_id)
    log.info("return_session_closed", session_id=str(pack.id), conclusion=conclusion, auto=auto,
             return_case_id=str(target.id))  # fmt: skip
    return ClosedSessionOut(
        id=pack.id,
        type="RETURN",
        tracking_number=pack.open_code,
        flags=list(pack.flags),
        conclusion=conclusion,
        claim_code=None,
        package_status=refreshed.warehouse_status if refreshed else package.warehouse_status,
        return_case_status=target.status,
    )


async def end_return_session(
    session: AsyncSession,
    pack: PackSession,
    *,
    status: str,
    reason: str | None,
    note: str | None,
    actor_label: str,
) -> None:
    """Hủy (API-12 / API-21 CANCEL_SESSION) / bỏ dở (J-07) phiên RETURN: kiện về trạng thái lúc mở phiên;
    hồ sơ do chính phiên này tạo (về trước khi sàn báo / chưa xác định), chưa có phiên nào khác → `CANCELLED`;
    còn lại tính lại (BR-24). Clip vẫn cắt. Người gọi giữ khóa station."""
    from aicam.modules.media import jobs as media_jobs
    from aicam.modules.sessions.events import record_event

    case = await returns.lock_case(session, pack.return_case_id) if pack.return_case_id else None
    locked = await returns.lock_packages(session, [pack.package_id])
    pack.status = status
    pack.ended_at = clock.now()
    pack.cancel_reason = reason
    pack.note = note
    if locked:
        await orders.transition(
            session,
            locked[0],
            pack.package_status_before or "NEW",
            source="WAREHOUSE",
            actor_label=actor_label,
        )
    await session.flush()
    if case is not None:
        others = [
            s for s in await returns.return_sessions_of_case(session, case.id)
            if s.id != pack.id and s.status not in ("CANCELLED", "ABANDONED")
        ]  # fmt: skip
        created_here = case.source == "WAREHOUSE" and case.kind in ("UNANNOUNCED", "UNIDENTIFIED")
        if created_here and not others and case.platform_return_sn is None:
            case.status = "CANCELLED"
        else:
            await returns.recompute(session, case)
        returns.notify_updated(session, case)
    record_event(session, pack, status, reason=reason, note=note)
    media_jobs.enqueue_build_clips(session, pack.id, pack.ended_at)
