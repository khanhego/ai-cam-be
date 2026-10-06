"""Phiên đóng gói: trạng thái station (API-10) và xử lý quét (API-11) — 02a §4.1, BR-01..06, BR-18."""

import asyncio
import re
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import all_, delete, func, literal, select, text, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.settings import Settings, get_settings
from aicam.modules.approvals.queries import last_resolved_at, pending_for_station, set_tray_match
from aicam.modules.approvals.views import approval_item
from aicam.modules.claims import service as claims
from aicam.modules.media import jobs as media_jobs
from aicam.modules.media.queries import clips_of_session
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import RETURN_STATUSES, Order, Package
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAdapter, PlatformError
from aicam.modules.returns import service as returns
from aicam.modules.sessions import return_scan, return_state
from aicam.modules.sessions.events import record_event as record_event
from aicam.modules.sessions.events import set_flag as set_flag
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession, ScanDedup
from aicam.modules.sessions.schemas import (
    AlertOut,
    ApprovalBrief,
    CameraState,
    ClosedSessionOut,
    InspectionIn,
    InspectionSavedOut,
    ItemOut,
    MismatchOut,
    OrderBrief,
    PackageBrief,
    RecentClip,
    RecentOut,
    RecentSession,
    ReturnSessionIn,
    ScanOut,
    SessionOut,
    SnapshotCreated,
    SnapshotCreatedOut,
    StationStateOut,
    StationStateRef,
    TrayOut,
)
from aicam.modules.sessions.tray import Tray, read_tray
from aicam.modules.settings import service as settings_service
from aicam.modules.stations import service as stations
from aicam.modules.stations.models import Station

log = structlog.get_logger()

_STATE_BY_STATUS = {"OPEN": "PACKING", "MISMATCH": "MISMATCH", "WAITING_APPROVAL": "WAITING_APPROVAL"}
# API-12 (02 §6.2): lý do hủy theo loại phiên.
_CANCEL_REASONS = {
    "PACK": ("OUT_OF_STOCK", "WRONG_SCAN", "OTHER"),
    "RETURN": ("WRONG_SCAN", "NOT_A_RETURN", "OTHER"),
}


# ---------------------------------------------------------------- truy vấn


async def active_session(
    session: AsyncSession, station_id: uuid.UUID, *, refresh: bool = False
) -> PackSession | None:
    """`refresh=True` sau khi lấy lock: đọc lại từ DB, bỏ bản cũ trong identity map (review M1 #6)."""
    query = select(PackSession).where(
        PackSession.station_id == station_id, PackSession.status.in_(ACTIVE_STATUSES)
    )
    if refresh:
        query = query.execution_options(populate_existing=True)
    result: PackSession | None = await session.scalar(query)
    return result


async def active_session_of_package(session: AsyncSession, package_id: uuid.UUID) -> PackSession | None:
    result: PackSession | None = await session.scalar(
        select(PackSession).where(
            PackSession.package_id == package_id, PackSession.status.in_(ACTIVE_STATUSES)
        )
    )
    return result


async def last_completed(session: AsyncSession, package_id: uuid.UUID) -> PackSession | None:
    result: PackSession | None = await session.scalar(
        select(PackSession)
        .where(PackSession.package_id == package_id, PackSession.status == "COMPLETED")
        .order_by(PackSession.ended_at.desc())
        .limit(1)
    )
    return result


def vn_day_start(day: date, tz: str) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))


async def today_count(session: AsyncSession, station_id: uuid.UUID, tz: str) -> int:
    start = vn_day_start(clock.now().astimezone(ZoneInfo(tz)).date(), tz)
    return (
        await session.scalar(
            select(func.count()).where(
                PackSession.station_id == station_id,
                PackSession.type == "PACK",
                PackSession.status == "COMPLETED",
                PackSession.ended_at >= start,
            )
        )
        or 0
    )


# ---------------------------------------------------------------- API-10


async def _lock_package(session: AsyncSession, package_id: uuid.UUID) -> Package:
    """Khóa kiện `FOR UPDATE` + đọc lại (thứ tự DEC-266: station → hồ sơ → kiện)."""
    package: Package | None = await session.scalar(
        select(Package)
        .where(Package.id == package_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if package is None:
        raise RuntimeError(f"Phiên trỏ tới kiện không tồn tại: {package_id}")
    return package


async def _require_package(session: AsyncSession, package_id: uuid.UUID) -> Package:
    package = await session.get(Package, package_id)
    if package is None:  # FK RESTRICT nên không xảy ra; lỗi dữ liệu thì báo rõ
        raise RuntimeError(f"Phiên trỏ tới kiện không tồn tại: {package_id}")
    return package


async def _package_brief(session: AsyncSession, package: Package) -> PackageBrief:
    order_brief = None
    items: list[ItemOut] = []
    if package.order_id:
        order = await orders.get_order(session, package.order_id)
        if order is not None:
            order_brief = OrderBrief(
                platform="SHOPEE", platform_order_sn=order.platform_order_sn, buyer_note=order.buyer_note
            )
            items = [
                ItemOut(
                    order_item_id=i.id,
                    product_name=i.product_name,
                    variation=i.variation,
                    quantity=i.quantity,
                    image_url=i.image_url,
                )
                for i in await orders.items_of(session, order.id)
            ]
    return PackageBrief(
        id=package.id, tracking_number=package.tracking_number, order=order_brief, items=items
    )


async def build_state(session: AsyncSession, station: Station, settings: Settings) -> StationStateOut:
    current = await active_session(session, station.id)
    pending = await pending_for_station(session, station.id)
    tray = await read_tray(get_redis(), station.id, current.open_code if current else None)
    cameras = [
        CameraState(role=c.role, status=c.status) for c in await stations.cameras_of(session, station.id)
    ]
    session_out = None
    if current is not None:
        package = await _require_package(session, current.package_id)
        cfg = await settings_service.get(session)
        is_return = current.type == "RETURN"
        warn_m, abandon_m = (
            (cfg.return_warn_minutes, cfg.return_abandon_minutes)
            if is_return
            else (cfg.session_warn_minutes, cfg.session_abandon_minutes)
        )
        session_out = SessionOut(
            id=current.id,
            type=current.type,
            status=current.status,
            started_at=current.started_at,
            flags=list(current.flags),
            operator_name=current.operator_name,
            package=await _package_brief(session, package),
            mismatch=MismatchOut(**current.mismatch) if current.mismatch else None,
            warn_at=(base := await timer_base(session, current)) + timedelta(minutes=warn_m),
            abandon_at=base + timedelta(minutes=abandon_m),
        )
        if is_return:
            await return_state.fill(session, session_out, current, settings, station.account_user_id)
    if pending is not None:
        state = "WAITING_APPROVAL"
    elif current is not None:
        state = (
            "INSPECTING"
            if current.type == "RETURN" and current.status == "OPEN"
            else _STATE_BY_STATUS[current.status]
        )
    else:
        state = "READY"
    return_total, return_issue = await return_state.today_return_counts(
        session, station.id, settings.tz_display
    )
    return StationStateOut(
        station=StationStateRef(
            id=station.id,
            name=station.name,
            kind=station.kind,
            work_mode=station.work_mode,
            operator_name=station.operator_name,
        ),
        state=state,
        cameras=cameras,
        tray=TrayOut(codes=list(tray.codes), match=tray.match, updated_at=tray.updated_at),
        session=session_out,
        approval_request=(
            ApprovalBrief(
                id=pending.id,
                type=pending.type,
                tracking_number=pending.tracking_number,
                created_at=pending.created_at,
            )
            if pending
            else None
        ),
        today_count=await today_count(session, station.id, settings.tz_display),
        today_return_count=return_total,
        today_return_issue_count=return_issue,
        server_time=clock.now(),
    )


async def require_station(
    session: AsyncSession, station_id: uuid.UUID | None, user_id: uuid.UUID | None = None
) -> Station:
    """Token còn hạn nhưng station đã gỡ khỏi tài khoản → 403 ngay, không chờ token hết hạn (review #13)."""
    station = await stations.get_station(session, station_id) if station_id else None
    if station is not None and user_id is not None and station.account_user_id != user_id:
        station = None
    if station is None:
        raise AppError("FORBIDDEN", "Tài khoản không gắn station.", 403)
    if not station.is_active:
        raise AppError("STATION_INACTIVE", "Station này đang tắt. Liên hệ Admin.", 409)
    return station


# ---------------------------------------------------------------- API-11


def _alert(code: str, message: str, **data: Any) -> AlertOut:
    return AlertOut(code=code, message=message, data=data)


async def lock_station(session: AsyncSession, station_id: uuid.UUID) -> None:
    """Tuần tự hóa mọi thao tác trên một station (DEC-11). Nhả khi transaction kết thúc."""
    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"station:{station_id}"})


async def _lookup_platform(
    session: AsyncSession, code: str, adapter: PlatformAdapter, settings: Settings
) -> Package | None:
    """BR-04: tra sàn tối đa `PLATFORM_LOOKUP_TIMEOUT_S` (2 giây); có → ghi đơn; không / quá hạn / lỗi → None.

    Gọi ngoài lock station; ghi đơn trong savepoint (station khác có thể vừa ghi cùng đơn — review M1 #5).
    """
    try:
        target = await platforms.lookup_target(session, adapter, settings)
        if target is None:
            return None
        found = await asyncio.wait_for(
            adapter.find_by_tracking(target.creds, code), timeout=settings.platform_lookup_timeout_s
        )
    except (TimeoutError, PlatformError) as exc:
        log.info("platform_lookup_failed", code=code, error=type(exc).__name__)
        return None
    except SQLAlchemyError:
        raise  # lỗi DB: transaction quét đã hỏng, không giả như "không tìm thấy"
    except Exception:  # tra sàn lỗi bất ngờ không được làm quét 500 (BR-04 → UNVERIFIED, G3-P2-6)
        log.exception("platform_lookup_error", code=code)
        return None
    if found is None:
        return None
    try:
        # Station khác vừa ghi cùng đơn (tra ngoài lock): bỏ qua, bước mở phiên đọc lại từ DB (review #5).
        async with session.begin_nested():
            result = await orders.upsert_platform_order(session, found, shop_id=target.shop_id)
    except IntegrityError:
        return None
    return next((p for p in result.packages if p.tracking_number == code), None)


def _hhmm(at: datetime, tz: str) -> str:
    return at.astimezone(ZoneInfo(tz)).strftime("%H:%M")


async def _open_session(
    session: AsyncSession, station: Station, code: str, settings: Settings
) -> tuple[str, AlertOut | None]:
    """Hai station tranh cùng kiện / cùng mã mới: unique index chặn → ALERT thay vì 500 (review #5)."""
    try:
        async with session.begin_nested():
            return await _open_session_unsafe(session, station, code, settings)
    except IntegrityError:
        return "ALERT", _alert("PACKED_ELSEWHERE_IN_PROGRESS", f"{code} đang được đóng gói ở station khác.")


async def _open_session_unsafe(
    session: AsyncSession, station: Station, code: str, settings: Settings
) -> tuple[str, AlertOut | None]:
    # Khóa kiện (DEC-266, sau station): API-122 chỉnh tay cùng lúc không bị bên quét ghi đè (DEC-303 d).
    package = await orders.find_package(session, code, for_update=True)
    if package is None:
        package = await orders.create_unverified_package(session, code)
    if package.warehouse_status in RETURN_STATUSES:  # EX-R16, DEC-247: kiện hoàn ở bàn đóng gói
        return "ALERT", _alert(
            "ALREADY_HANDED_OVER", f"{code} là kiện hàng hoàn — nhận ở bàn nhận hoàn.", is_return=True
        )
    if await orders.is_cancelled(session, package):
        return "ALERT", _alert("ORDER_CANCELLED", f"{code} đã bị hủy trên Shopee. Không đóng gói.")
    if package.warehouse_status == "PACKED":
        done = await last_completed(session, package.id)
        station_name = None
        if done is not None:
            other = await stations.get_station(session, done.station_id)
            station_name = other.name if other else None
        when = f" lúc {_hhmm(done.ended_at, settings.tz_display)}" if done and done.ended_at else ""
        return "ALERT", _alert(
            "ALREADY_PACKED",
            f"{code} đã đóng gói{when}" + (f" tại {station_name}." if station_name else "."),
            packed_at=clock.iso_z(done.ended_at) if done and done.ended_at else None,
            station_name=station_name,
            can_request_repack=True,
        )
    if package.warehouse_status in ("HANDED_OVER", "DELIVERED"):
        return "ALERT", _alert(
            "ALREADY_HANDED_OVER", f"{code} đã bàn giao cho đơn vị vận chuyển. Không đóng gói lại."
        )
    if package.warehouse_status == "PACKING" or await active_session_of_package(session, package.id):
        return "ALERT", _alert("PACKED_ELSEWHERE_IN_PROGRESS", f"{code} đang được đóng gói ở station khác.")
    pack = PackSession(
        package_id=package.id,
        station_id=station.id,
        status="OPEN",
        started_at=clock.now(),
        open_code=code,
        package_status_before=package.warehouse_status,
        flags=[] if package.verified else ["UNVERIFIED"],
    )
    # Phiếu đã nằm trên khay và khớp trước khi quét: vision không phát sự kiện mới, nên ghi nhận ngay (BR-18).
    tray = await read_tray(get_redis(), station.id, code)
    pack.cam2_seen_match = tray.match == "MATCH"
    session.add(pack)
    await orders.transition(session, package, "PACKING", source="WAREHOUSE", actor_label=station.name)
    await session.flush()
    record_event(session, pack, "SCAN_OPEN", code=code)
    # Khay đã có phiếu khác trước khi quét: vision không phát sự kiện mới nên xét ngay (BR-06, DEC-111).
    if apply_tray(session, pack, tray) == "MISMATCH":
        return "MISMATCH", None
    return "SESSION_OPENED", None


async def complete_session(
    session: AsyncSession, pack: PackSession, *, tray: Tray, close_code: str | None, actor_label: str
) -> ClosedSessionOut:
    """Đóng phiên hợp lệ: BR-18 cờ Cam 2, kiện PACKED, phiên đóng gói lại thay phiên cũ (BR-03).

    BR-21: phiên có cờ `ORDER_CANCELLED` (đơn hủy trên sàn khi đang đóng) → kiện `CANCELLED_AFTER_PACK`.
    Trả `closed_session` (FR-03.14) để station báo phiếu còn trên khay / Cam 2 không xác minh."""
    if not pack.cam2_seen_match or tray.match == "UNAVAILABLE":
        set_flag(pack, "CAM2_UNVERIFIED")
    if tray.match == "MATCH":
        set_flag(pack, "LABEL_ON_TRAY")
    pack.status = "COMPLETED"
    pack.ended_at = clock.now()
    pack.close_code = close_code
    pack.mismatch = None
    pack.camera_clock = await return_scan.camera_clock(session, pack.station_id)  # DEC-261 (PACK + RETURN)
    package = await _lock_package(session, pack.package_id)
    target = "CANCELLED_AFTER_PACK" if "ORDER_CANCELLED" in pack.flags else "PACKED"
    await orders.transition(session, package, target, source="WAREHOUSE", actor_label=actor_label)
    if pack.supersedes_session_id:
        old = await session.get(PackSession, pack.supersedes_session_id)
        if old is not None and old.status == "COMPLETED":
            old.status = "SUPERSEDED"
    record_event(session, pack, "COMPLETED", close_code=close_code)
    media_jobs.enqueue_build_clips(session, pack.id, pack.ended_at)  # J-01 sau commit
    # T-121: ảnh lúc đóng gói lấy ngay từ khung Cam 1 vision giữ; không có → J-17 trích từ clip (DEC-227).
    from aicam.modules.media import snapshots

    await snapshots.capture_pack_close_from_cache(session, pack, get_settings())
    return ClosedSessionOut(
        id=pack.id,
        type="PACK",
        tracking_number=package.tracking_number,
        flags=list(pack.flags),
        conclusion=None,
        claim_code=None,
        package_status=package.warehouse_status,
    )


def mark_mismatch(pack: PackSession, *, source: str, actual: str) -> None:
    pack.status = "MISMATCH"
    pack.mismatch = {"source": source, "expected": pack.open_code, "actual": actual}
    set_flag(pack, "HAD_MISMATCH")


def _tray_actual(tray: Tray, open_code: str) -> str:
    """Mã Cam 2 thấy khác mã phiên (nhiều mã → nối bằng dấu phẩy)."""
    return ", ".join(c for c in tray.codes if c != open_code)


async def _continue_session(
    session: AsyncSession, station: Station, pack: PackSession, code: str
) -> tuple[str, AlertOut | None, ClosedSessionOut | None]:
    tray = await read_tray(get_redis(), station.id, pack.open_code)
    if tray.match == "MATCH":
        pack.cam2_seen_match = True
    # Thứ tự cứng (BR-06, review #2): khay có mã khác luôn xét trước khi so mã quét.
    if tray.blocks_close:
        mark_mismatch(pack, source="CAM2", actual=_tray_actual(tray, pack.open_code))
        record_event(session, pack, "MISMATCH", source="CAM2", scanned=code, tray=list(tray.codes))
        return "MISMATCH", None, None
    if code == pack.open_code:
        closed = await complete_session(session, pack, tray=tray, close_code=code, actor_label=station.name)
        return "SESSION_COMPLETED", None, closed
    mark_mismatch(pack, source="SCAN", actual=code)
    record_event(session, pack, "MISMATCH", source="SCAN", scanned=code)
    return "MISMATCH", None, None


async def _reload_station(session: AsyncSession, station_id: uuid.UUID) -> Station:
    station: Station | None = await session.scalar(
        select(Station).where(Station.id == station_id).execution_options(populate_existing=True)
    )
    if station is None:  # require_station đã kiểm
        raise AppError("FORBIDDEN", "Tài khoản không gắn station.", 403)
    return station


async def _dedup(session: AsyncSession, client_scan_id: uuid.UUID, station_id: uuid.UUID) -> ScanDedup | None:
    result: ScanDedup | None = await session.scalar(
        select(ScanDedup)
        .where(ScanDedup.client_scan_id == client_scan_id, ScanDedup.station_id == station_id)
        .execution_options(populate_existing=True)
    )
    return result


async def _replay(
    session: AsyncSession, station: Station, previous: ScanDedup, settings: Settings
) -> ScanOut:
    state = await build_state(session, station, settings)
    await commit(session)
    return ScanOut(
        outcome=previous.response["outcome"],
        alert=previous.response.get("alert"),
        state=state,
        closed_session=previous.response.get("closed_session"),
    )


async def scan(
    session: AsyncSession,
    station: Station,
    *,
    code: str,
    client_scan_id: uuid.UUID,
    adapter: PlatformAdapter,
    settings: Settings,
) -> ScanOut:
    code = code.strip().upper()

    # Retry cùng client_scan_id: trả nguyên kết quả cũ, state mới (DEC-29).
    previous = await _dedup(session, client_scan_id, station.id)
    if previous is not None:
        return await _replay(session, station, previous, settings)

    valid = re.fullmatch(settings.scan_code_regex, code) is not None
    prepared: return_scan.Prepared | None = None
    if station.work_mode == "RETURN":
        # Bàn hoàn (02a §4.1, R3-4): tra mã + tra sàn + khóa `order:{sn}` ngoài khóa station.
        prepared = await return_scan.prepare(session, station, code, adapter, settings)
        await session.flush()
    elif (
        valid
        and await active_session(session, station.id) is None
        and await orders.find_package(session, code) is None
    ):
        # Tra sàn ngoài khóa station (02a §4.1): chỉ khi mã hợp lệ, station rảnh và mã chưa có.
        await _lookup_platform(session, code, adapter, settings)
        await session.flush()

    await lock_station(session, station.id)
    # Retry chạy chồng với lần gửi đầu: kiểm lại dưới lock (review #4).
    previous = await _dedup(session, client_scan_id, station.id)
    if previous is not None:
        return await _replay(session, station, previous, settings)
    # Đổi chế độ giữa lúc quét → xử lý theo chế độ đọc lại dưới khóa (R-24).
    station = await _reload_station(session, station.id)
    pack = await active_session(session, station.id, refresh=True)
    closed: ClosedSessionOut | None = None
    if await pending_for_station(session, station.id) is not None or (
        pack is not None and pack.status == "WAITING_APPROVAL"
    ):
        outcome, alert = "IGNORED", None  # xét trước định dạng mã (review #20)
    elif station.work_mode == "RETURN":
        outcome, alert, closed = await return_scan.handle(session, station, pack, code, prepared, settings)
    elif not valid:
        outcome, alert = (
            "ALERT",
            _alert("INVALID_CODE", "Mã vừa quét không phải mã vận đơn. Quét lại mã trên phiếu."),
        )
    elif pack is None:
        outcome, alert = await _open_session(session, station, code, settings)
    else:
        outcome, alert, closed = await _continue_session(session, station, pack, code)

    session.add(
        ScanDedup(
            client_scan_id=client_scan_id,
            station_id=station.id,
            response={
                "outcome": outcome,
                "alert": alert.model_dump() if alert else None,
                "closed_session": closed.model_dump(mode="json") if closed else None,
            },
        )
    )
    await session.flush()
    state = await build_state(session, station, settings)
    notify_after_commit(session, station.id, state)
    await commit(session)
    log.info(
        "scan", station_id=str(station.id), code=code, outcome=outcome, alert=alert.code if alert else None,
        work_mode=station.work_mode,
    )  # fmt: skip
    return ScanOut(outcome=outcome, alert=alert, state=state, closed_session=closed)


# ---------------------------------------------------------------- API-105 (T-119)


def _invalid(field: str, message: str) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {field: message}})


async def open_return_by_request(
    session: AsyncSession,
    station: Station,
    body: ReturnSessionIn,
    *,
    actor: uuid.UUID,
    ip: str | None,
    settings: Settings,
) -> ScanOut:
    """API-105 (FR-04.07, 04.13; 02 §6.2, §6.3 #9, §6.4 #2, §6.5 #1): mở phiên hoàn từ kết quả tìm thủ công
    (`package_id`) hoặc mở phiên chưa xác định (`unidentified_code`, `force_new`). Cùng `client_scan_id` /
    thứ tự khóa như API-11 (DEC-266, R3-4): tra mã + khóa `order:{sn}` ngoài khóa station, kiểm lại trong
    khóa."""
    code = (body.unidentified_code or "").strip().upper() or None
    if (body.package_id is None) == (code is None):
        raise _invalid("package_id", "Chọn đúng một: kiện từ kết quả tìm hoặc mã chưa xác định")
    if code is not None and not return_scan.is_valid_code(code, settings):
        raise _invalid("unidentified_code", "Mã không đúng định dạng mã vận đơn / mã đơn")
    note = " ".join((body.note or "").split()) or None
    if body.force_new:
        if code is None:
            raise _invalid("unidentified_code", "Nhập mã đã quét trên kiện")
        if note is None or not 5 <= len(note) <= 200:
            raise _invalid("note", "Nhập ghi chú 5–200 ký tự")
    if station.work_mode != "RETURN":
        raise AppError("WRONG_WORK_MODE", "Station không ở chế độ nhận hàng hoàn.", 409)
    previous = await _dedup(session, body.client_scan_id, station.id)
    if previous is not None:
        return await _replay(session, station, previous, settings)

    # Ngoài khóa station: tìm kiện / tra mã (không tra sàn — FE đã thấy RETURN_NOT_FOUND), khóa đơn trước
    # station.
    resolution: returns.Resolution
    if body.package_id is not None:
        package = await session.get(Package, body.package_id)
        if package is None:
            raise AppError("NOT_FOUND", "Không tìm thấy kiện.", 404)
        order = await session.get(Order, package.order_id) if package.order_id else None
        resolution = returns.Resolution("FOUND", package, None, order)
        code = package.tracking_number
    else:
        assert code is not None  # noqa: S101 — kiểm ở trên
        # G3 SM-F8: hai station cùng mở "chưa xác định" cho một mã → tuần tự theo mã (khóa đầu tiên của
        # transaction), bên sau tra lại thấy hồ sơ bên trước vừa tạo.
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"return_code:{code}"}
        )
        resolution = await returns.resolve_code(session, code)
    if resolution.order is not None:
        await orders.lock_orders(session, [resolution.order.platform_order_sn])

    await lock_station(session, station.id)
    previous = await _dedup(session, body.client_scan_id, station.id)
    if previous is not None:
        return await _replay(session, station, previous, settings)
    station = await _reload_station(session, station.id)
    if station.work_mode != "RETURN":
        raise AppError("WRONG_WORK_MODE", "Station không ở chế độ nhận hàng hoàn.", 409)
    if await active_session(session, station.id, refresh=True) is not None or await pending_for_station(
        session, station.id
    ):
        raise AppError("SESSION_ACTIVE", "Station đang có phiên mở. Đóng phiên trước.", 409)

    alert: AlertOut | None
    if not station.operator_name:
        outcome, alert = "ALERT", _alert("OPERATOR_REQUIRED", "Nhập tên người kiểm trước khi nhận hàng hoàn.")
    elif body.force_new:
        outcome, alert = await return_scan.open_force_new(
            session, station, code, note or "", resolution, actor=actor, ip=ip, tz=settings.tz_display
        )
    elif body.package_id is None and resolution.status == "NOT_FOUND":
        outcome, alert = await return_scan.open_unidentified(session, station, code)
    else:
        outcome, alert = await return_scan.open_from_resolution(session, station, code, resolution, settings)

    session.add(
        ScanDedup(
            client_scan_id=body.client_scan_id,
            station_id=station.id,
            response={
                "outcome": outcome,
                "alert": alert.model_dump() if alert else None,
                "closed_session": None,
            },
        )
    )
    await session.flush()
    state = await build_state(session, station, settings)
    notify_after_commit(session, station.id, state)
    await commit(session)
    log.info("return_session_request", station_id=str(station.id), code=code, outcome=outcome,
             alert=alert.code if alert else None, force_new=body.force_new)  # fmt: skip
    return ScanOut(outcome=outcome, alert=alert, state=state)


# ---------------------------------------------------------------- Cam 2 (T-12)


def apply_tray(session: AsyncSession, pack: PackSession, tray: Tray) -> str | None:
    """Áp BR-06 cho phiên theo khay hiện tại (02a §4.1). Trả trạng thái mới nếu đổi.

    - Lần đầu `MATCH` trong phiên → `cam2_seen_match` (BR-18).
    - `OPEN` + khay `DIFFERENT`/`MULTIPLE` → `MISMATCH` nguồn `CAM2`, cờ `HAD_MISMATCH`.
    - `MISMATCH` nguồn `CAM2` + khay `MATCH`/`NOT_SEEN` → `OPEN`; phiếu sai đổi → cập nhật `actual`.
    - `MISMATCH` nguồn `SCAN`, `WAITING_APPROVAL` → giữ (chỉ quét đúng mã / quyết định duyệt mới đổi).
    """
    if pack.type == "RETURN":
        # BR-06 không áp phiên hoàn (DEC-203, DEC-246): chỉ ghi mã Cam 2 thấy, không đổi trạng thái.
        seen = ", ".join(tray.codes) or None
        if seen and seen != pack.cam2_code:
            pack.cam2_code = seen
            record_event(session, pack, "CAM2_DETECT", tray=list(tray.codes))
        return None
    if tray.match == "MATCH":
        pack.cam2_seen_match = True
    cam2_mismatch = pack.status == "MISMATCH" and (pack.mismatch or {}).get("source") == "CAM2"
    if pack.status == "OPEN" and tray.blocks_close:
        mark_mismatch(pack, source="CAM2", actual=_tray_actual(tray, pack.open_code))
        record_event(session, pack, "MISMATCH", source="CAM2", tray=list(tray.codes))
        return "MISMATCH"
    if cam2_mismatch and tray.match in ("MATCH", "NOT_SEEN"):
        pack.status = "OPEN"
        pack.mismatch = None
        record_event(session, pack, "MISMATCH_CLEARED", source="CAM2", tray=list(tray.codes))
        return "OPEN"
    if cam2_mismatch and tray.blocks_close and pack.mismatch:
        actual = _tray_actual(tray, pack.open_code)
        if pack.mismatch.get("actual") != actual:
            pack.mismatch = {**pack.mismatch, "actual": actual}
    return None


async def on_tray_changed(session: AsyncSession, station_id: uuid.UUID, settings: Settings) -> str | None:
    """Vision báo khay đổi (BR-06, FR-03.06, 03.07): áp `apply_tray` cho phiên đang mở của station.

    Đang chờ duyệt MISMATCH / ASSIST → cập nhật `context.tray_match` của yêu cầu (D13 khóa / mở nút
    "Đóng phiên có ghi chú") và báo `approval.updated` (DEC-112).
    Cùng khóa station với quét / J-07; đẩy `station.state` sau commit. Trả trạng thái phiên mới nếu đổi.
    """
    station = await stations.get_station(session, station_id)
    if station is None:
        await rollback(session)
        return None
    await lock_station(session, station_id)
    pack = await active_session(session, station_id, refresh=True)
    pending = await pending_for_station(session, station_id)
    new_status: str | None = None
    tray_match: str | None = None
    if pack is not None:
        tray = await read_tray(get_redis(), station_id, pack.open_code)
        tray_match = tray.match
        new_status = apply_tray(session, pack, tray)
    approval_changed = pending is not None and set_tray_match(pending, tray_match)
    await session.flush()
    state = await build_state(session, station, settings)
    if new_status is not None:
        notify_after_commit(session, station_id, state)  # + report.updated (số phiên từng lệch mã)
        log.info("tray_session_changed", station_id=str(station_id), status=new_status)
    else:
        _publish_state_after_commit(session, station_id, state)
    if approval_changed and pending is not None:
        item = await approval_item(session, pending)
        after_commit(session, lambda: _to_approvers("approval.updated", item.model_dump(mode="json")))
    await commit(session)
    return new_status


# ---------------------------------------------------------------- API-12, API-15, J-07


async def end_without_packing(
    session: AsyncSession,
    pack: PackSession,
    *,
    status: str,
    reason: str | None,
    note: str | None,
    actor_label: str,
) -> None:
    """Hủy / bỏ dở: kiện về trạng thái lúc mở phiên (NEW, hoặc PACKED nếu là phiên đóng gói lại — BR-03)."""
    if pack.type == "RETURN":
        await return_scan.end_return_session(
            session, pack, status=status, reason=reason, note=note, actor_label=actor_label
        )
        return
    pack.status = status
    pack.ended_at = clock.now()
    pack.cancel_reason = reason
    pack.note = note
    pack.mismatch = None
    package = await _lock_package(session, pack.package_id)
    await orders.transition(
        session, package, pack.package_status_before or "NEW", source="WAREHOUSE", actor_label=actor_label
    )
    if "ORDER_CANCELLED" in pack.flags:  # đơn đã hủy trên sàn khi đang đóng (BR-21): không để kiện "Mới"
        await orders.apply_platform_cancel(session, package)
    record_event(session, pack, status, reason=reason, note=note)
    media_jobs.enqueue_build_clips(session, pack.id, pack.ended_at)  # clip vẫn cắt cho phiên hủy / bỏ dở


async def cancel(
    session: AsyncSession,
    station: Station,
    session_id: uuid.UUID,
    *,
    reason: str,
    note: str | None,
    settings: Settings,
) -> StationStateOut:
    if reason == "OTHER" and not (note and note.strip()):
        raise AppError(
            "VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"note": "Nhập lý do khi chọn Khác"}}
        )
    await lock_station(session, station.id)
    pack = await session.scalar(
        select(PackSession).where(PackSession.id == session_id).execution_options(populate_existing=True)
    )
    if pack is None or pack.station_id != station.id or pack.status not in ("OPEN", "MISMATCH"):
        raise AppError("SESSION_NOT_OPEN", "Phiên không còn mở.", 409)
    if reason not in _CANCEL_REASONS[pack.type]:
        raise AppError(
            "VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422,
            {"fields": {"reason": "Lý do không áp dụng cho loại phiên này"}},
        )  # fmt: skip
    await end_without_packing(
        session, pack, status="CANCELLED", reason=reason, note=note and note.strip(), actor_label=station.name
    )
    await session.flush()
    state = await build_state(session, station, settings)
    notify_after_commit(session, station.id, state)
    await commit(session)
    return state


async def save_inspection(
    session: AsyncSession,
    station: Station,
    session_id: uuid.UUID,
    body: InspectionIn,
    settings: Settings,
) -> InspectionSavedOut:
    """API-102 (FR-04.03, 04.09, BR-22): lưu nháp kết luận + dòng (ghi đè). Khóa station → phiên."""
    from aicam.modules.sessions import inspection

    await lock_station(session, station.id)
    pack = await session.scalar(
        select(PackSession)
        .where(PackSession.id == session_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if pack is None or pack.station_id != station.id or pack.status != "OPEN":
        raise AppError("SESSION_NOT_OPEN", "Phiên không còn mở.", 409)
    if pack.type != "RETURN":
        raise AppError("NOT_RETURN_SESSION", "Phiên không phải phiên mở hoàn.", 409)
    current = await inspection.lines_of(session, pack.id)
    lines = [
        inspection.LineInput(i.order_item_id, i.quantity_received, i.condition, i.note) for i in body.lines
    ]
    note = body.note.strip() if body.note else None
    inspection.validate(
        lines_mode=pack.inspection_lines_mode or "FULL", conclusion=body.conclusion, note=note,
        current=current, lines=lines,
    )  # fmt: skip
    inspection.replace_lines(current, lines)
    pack.inspection_conclusion = body.conclusion
    pack.inspection_note = note or None
    pack.inspection_saved_at = clock.now()
    await session.flush()
    out = inspection.inspection_out(pack, current)
    state = await build_state(session, station, settings)
    _publish_state_after_commit(session, station.id, state)
    await commit(session)
    return InspectionSavedOut(inspection=out)


async def take_snapshot(
    session: AsyncSession, station: Station, session_id: uuid.UUID, settings: Settings
) -> SnapshotCreatedOut:
    """API-103 (FR-04.04): chụp Cam 1 cho phiên RETURN đang mở; đẩy `station.state` sau commit."""
    from aicam.modules.media import snapshots

    shot = await snapshots.take(session, station.id, session_id, settings, lock=lock_station)
    state = await build_state(session, station, settings)
    _publish_state_after_commit(session, station.id, state)
    await commit(session)
    uid = station.account_user_id
    return SnapshotCreatedOut(
        snapshot=SnapshotCreated(
            id=shot.id, kind="MANUAL", camera_role="CAM1", taken_at=shot.taken_at, sha256=shot.sha256 or "",
            url=snapshots.url_for(settings, shot.id, uid) if uid else "",
        )
    )  # fmt: skip


async def recent(session: AsyncSession, station: Station, settings: Settings, limit: int = 5) -> RecentOut:
    """API-15: 5 phiên gần nhất của station trong ngày (giờ VN) — ma trận quyền 01 §5.1."""
    start = vn_day_start(clock.now().astimezone(ZoneInfo(settings.tz_display)).date(), settings.tz_display)
    rows = (
        await session.execute(
            select(PackSession, Package.tracking_number)
            .join(Package, Package.id == PackSession.package_id)
            .where(PackSession.station_id == station.id, PackSession.started_at >= start)
            .order_by(PackSession.started_at.desc())
            .limit(limit)
        )
    ).all()
    items = []
    for pack, tracking in rows:
        clips = await clips_of_session(session, pack.id)
        items.append(
            RecentSession(
                id=pack.id,
                type=pack.type,
                conclusion=pack.inspection_conclusion,
                claim_code=await claims.code_for_session(session, pack.id) if pack.type == "RETURN" else None,
                tracking_number=pack.open_code if pack.type == "RETURN" else tracking,
                status=pack.status,
                flags=list(pack.flags),
                started_at=pack.started_at,
                ended_at=pack.ended_at,
                clips=[RecentClip(id=c.id, camera_role=c.camera_role, status=c.status) for c in clips],
            )
        )
    return RecentOut(items=items)


async def timer_base(session: AsyncSession, pack: PackSession) -> datetime:
    """Mốc tính quá giờ (BR-16): lúc mở phiên, hoặc lúc yêu cầu duyệt gần nhất kết thúc nếu muộn hơn.

    Thời gian chờ quản lý không tính vào thời gian đóng gói: phiên vừa được "Cho tiếp tục" sau 40 phút chờ
    không bị cảnh báo / bỏ dở ngay (RB-14 → DEC-60).
    """
    resolved = await last_resolved_at(session, pack.id)
    return max(pack.started_at, resolved) if resolved else pack.started_at


AUTO_CLOSE_BLOCKED = "AUTO_CLOSE_BLOCKED"


async def _saved_inspection_complete(session: AsyncSession, pack: PackSession) -> bool:
    """Kết luận + dòng kiểm đã lưu qua được kiểm tra của API-102 (BR-22: ghi chú khi Khác, nhất quán
    Nguyên vẹn)."""
    from aicam.modules.sessions import inspection

    current = await inspection.lines_of(session, pack.id)
    try:
        inspection.validate(
            lines_mode=pack.inspection_lines_mode or "FULL", conclusion=pack.inspection_conclusion,
            note=pack.inspection_note, current=current,
            lines=[
                inspection.LineInput(x.order_item_id, x.quantity_received, x.condition, x.note)
                for x in current
            ],
        )  # fmt: skip
    except AppError:
        return False
    return True


async def check_timeouts(session: AsyncSession, settings: Settings) -> dict[str, int]:
    """J-07 (BR-16): mở quá `warn` phút → cảnh báo một lần; quá `abandon` phút → ABANDONED.

    Phiên đang chờ duyệt không bị bỏ dở (02a BR-16). Mỗi phiên: lock station → đọc lại `FOR UPDATE`
    → kiểm lại → commit riêng.
    Không ghi đè lần quét đóng vừa xảy ra; một phiên lỗi không hỏng cả lô (review M1 #2).
    """
    from aicam.realtime import publish

    cfg = await settings_service.get(session)
    limits = {
        "PACK": (timedelta(minutes=cfg.session_warn_minutes), timedelta(minutes=cfg.session_abandon_minutes)),
        "RETURN": (timedelta(minutes=cfg.return_warn_minutes), timedelta(minutes=cfg.return_abandon_minutes)),
    }
    minutes = {"PACK": cfg.session_warn_minutes, "RETURN": cfg.return_warn_minutes}
    candidates = (
        await session.execute(
            select(PackSession.id, PackSession.station_id, PackSession.return_case_id).where(
                PackSession.status.in_(("OPEN", "MISMATCH"))
            )
        )
    ).all()
    await session.commit()
    counts = {"warned": 0, "abandoned": 0}
    auto_closed = 0
    for session_id, station_id, case_id in candidates:
        try:
            if case_id is not None:  # DEC-266: `order:{sn}` (hồ sơ chờ gộp) trước station
                await return_scan.lock_case_orders(session, case_id)
            await lock_station(session, station_id)
            pack = await session.scalar(
                select(PackSession)
                .where(PackSession.id == session_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if pack is None or pack.status not in ("OPEN", "MISMATCH"):
                await rollback(session)
                continue
            warn_after, abandon_after = limits[pack.type]
            age = clock.now() - await timer_base(session, pack)
            if (
                age >= abandon_after
                and pack.type == "RETURN"
                and pack.inspection_conclusion is not None
                and not await _saved_inspection_complete(session, pack)
            ):
                # G3 J-07 (DEC-340): kết luận đã lưu nhưng chưa đủ (vd. "Khác" thiếu ghi chú) → không tự hoàn
                # tất
                # bằng dữ liệu dở: giữ phiên (cờ AUTO_CLOSE_BLOCKED, báo station một lần), D2 đếm ở "phiên
                # hoàn
                # bỏ dở" để quản lý xử lý.
                if AUTO_CLOSE_BLOCKED in pack.flags:
                    await rollback(session)
                    continue
                set_flag(pack, AUTO_CLOSE_BLOCKED)
                await commit(session)
                counts["blocked"] = counts.get("blocked", 0) + 1
                await publish.to_station(
                    station_id,
                    "alert",
                    {"code": "SESSION_WARN", "session_id": str(session_id), "minutes": minutes[pack.type]},
                )
            elif age >= abandon_after and pack.type == "RETURN" and pack.inspection_conclusion is not None:
                # EX-R15, DEC-253: kết luận đã lưu → tự hoàn tất như quét đóng (cờ AUTO_CLOSED).
                station = await stations.get_station(session, station_id)
                case = await returns.lock_case(session, pack.return_case_id) if pack.return_case_id else None
                if case is None:
                    await rollback(session)
                    continue
                closed = await return_scan.close_return_session(
                    session,
                    pack,
                    case,
                    code=None,
                    actor_label=station.name if station else "Hệ thống",
                    auto=True,
                )
                notify_after_commit(session, station_id, None)
                await commit(session)
                auto_closed += 1
                await publish_state(session, station_id, settings)
                await publish.to_station(
                    station_id,
                    "alert",
                    {
                        "code": "SESSION_AUTO_CLOSED",
                        "session_id": str(session_id),
                        "tracking_number": pack.open_code,
                        "closed_session": closed.model_dump(mode="json"),
                    },
                )
            elif age >= abandon_after:
                station = await stations.get_station(session, station_id)
                await end_without_packing(
                    session, pack, status="ABANDONED", reason=None, note=None,
                    actor_label=station.name if station else "Hệ thống",
                )  # fmt: skip
                alert = {
                    "code": "SESSION_ABANDONED",
                    "session_id": str(session_id),
                    "tracking_number": pack.open_code,
                }
                await commit(session)
                counts["abandoned"] += 1
                await publish_state(session, station_id, settings)
                await publish.to_station(station_id, "alert", alert)
            elif age >= warn_after and not pack.warn_notified:
                pack.warn_notified = True
                await commit(session)
                counts["warned"] += 1
                await publish.to_station(
                    station_id,
                    "alert",
                    {
                        "code": "SESSION_WARN",
                        "session_id": str(session_id),
                        "minutes": minutes[pack.type],
                    },
                )
            else:
                await rollback(session)
        except Exception:
            await rollback(session)
            log.exception("check_timeouts_failed", session_id=str(session_id))
    if auto_closed:
        log.info("return_sessions_auto_closed", count=auto_closed)
    return counts


# ---------------------------------------------------------------- BR-21 đơn hủy khi đang đóng (T-117)


async def flag_order_cancelled(session: AsyncSession, package_id: uuid.UUID, settings: Settings) -> str:
    """Task riêng sau J-04 / J-06 thấy đơn hủy khi kiện `PACKING` (BR-21, DEC-266, R3-8).

    Khóa `order:{sn}` → station của phiên → kiện. Phiên còn hoạt động → cờ `ORDER_CANCELLED` + WS-01
    `alert ORDER_CANCELLED_DURING_SESSION` + `station.state`; phiên đã đóng trước khi task chạy (kiện
    `PACKED`) → `PACKED → CANCELLED_AFTER_PACK`. Trả kết quả (log / test)."""
    for _ in range(2):  # phiên vừa mở giữa bước đọc và bước khóa → làm lại với đúng station
        result = await _flag_order_cancelled_once(session, package_id, settings)
        if result != "retry":
            return result
    return "noop"


async def _flag_order_cancelled_once(session: AsyncSession, package_id: uuid.UUID, settings: Settings) -> str:
    from aicam.realtime import publish

    package = await session.get(Package, package_id)
    if package is None:
        await rollback(session)
        return "missing"
    if package.order_id is not None:
        order = await session.get(Order, package.order_id)
        if order is not None:
            await orders.lock_orders(session, [order.platform_order_sn])
    active = await active_session_of_package(session, package_id)
    if active is not None:
        await lock_station(session, active.station_id)
    pack = await session.scalar(
        select(PackSession)
        .where(PackSession.package_id == package_id, PackSession.status.in_(ACTIVE_STATUSES))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if pack is not None and (active is None or pack.station_id != active.station_id):
        await rollback(session)
        return "retry"
    package = await _lock_package(session, package_id)
    if pack is not None:
        if pack.type != "PACK" or "ORDER_CANCELLED" in pack.flags:
            await rollback(session)
            return "noop"
        set_flag(pack, "ORDER_CANCELLED")
        record_event(session, pack, "ORDER_CANCELLED")
        await session.flush()
        station = await stations.get_station(session, pack.station_id)
        state = await build_state(session, station, settings) if station else None
        notify_after_commit(session, pack.station_id, state)
        alert = {"code": "ORDER_CANCELLED_DURING_SESSION", "session_id": str(pack.id),
                 "tracking_number": package.tracking_number}  # fmt: skip
        station_id = pack.station_id

        async def _alert_station() -> None:
            await publish.to_station(station_id, "alert", alert)

        after_commit(session, _alert_station)
        await commit(session)
        log.info("order_cancelled_during_session", session_id=str(pack.id), package_id=str(package_id))
        return "flagged"
    if package.warehouse_status == "PACKED":  # phiên đã đóng trước khi task chạy (R3-8)
        await orders.transition(
            session, package, "CANCELLED_AFTER_PACK", source="PLATFORM", actor_label="Sàn"
        )
        today = clock.now().astimezone(ZoneInfo(settings.tz_display)).date().isoformat()

        async def _report() -> None:
            await publish.to_dashboard("report.updated", {"date": today})

        after_commit(session, _report)
        await commit(session)
        return "cancelled_after_pack"
    await rollback(session)
    return "noop"


# ---------------------------------------------------------------- cờ phiên (T-14)


async def add_flag(session: AsyncSession, session_id: uuid.UUID, flag: str) -> None:
    """Thêm cờ nguyên tử (không ghi đè cờ do luồng khác vừa thêm)."""
    await session.execute(
        update(PackSession)
        .where(PackSession.id == session_id, literal(flag) != all_(PackSession.flags))
        .values(flags=func.array_append(PackSession.flags, flag))
        .execution_options(synchronize_session=False)
    )


async def mark_camera_lost(
    session: AsyncSession, station_id: uuid.UUID, camera_role: str
) -> uuid.UUID | None:
    """Camera mất tín hiệu giữa phiên → cờ VIDEO_INCOMPLETE cho phiên đang mở (EX-P7, review M1 #15).

    Cùng khóa station với quét / J-07. Trả id phiên bị gắn cờ (None nếu station rảnh). Caller commit.
    """
    await lock_station(session, station_id)
    pack = await active_session(session, station_id, refresh=True)
    if pack is None:
        return None
    set_flag(pack, "VIDEO_INCOMPLETE")
    record_event(session, pack, "CAMERA_OFFLINE", camera_role=camera_role)
    return pack.id


# ---------------------------------------------------------------- realtime (T-11)


async def publish_state(session: AsyncSession, station_id: uuid.UUID, settings: Settings) -> None:
    """Đẩy `station.state` mới nhất xuống station (WS-01).

    Dùng sau thay đổi không do chính station gây ra (camera, J-07, duyệt). Gọi sau commit.
    """
    from aicam.realtime import publish

    station = await stations.get_station(session, station_id)
    if station is None:
        return
    state = await build_state(session, station, settings)
    await publish.to_station(station_id, "station.state", state.model_dump(mode="json"))


def notify_after_commit(session: AsyncSession, station_id: uuid.UUID, state: StationStateOut | None) -> None:
    """Sau commit: state xuống station + dashboard tính lại số liệu (WS-02 `report.updated`)."""
    from aicam.realtime import publish

    async def _send() -> None:
        if state is not None:
            await publish.to_station(station_id, "station.state", state.model_dump(mode="json"))
        today = clock.now().astimezone(ZoneInfo(get_settings().tz_display)).date()
        await publish.to_dashboard("report.updated", {"date": today.isoformat()})

    after_commit(session, _send)


async def _to_approvers(event_type: str, data: Any) -> None:
    from aicam.realtime import publish

    await publish.to_approvers(event_type, data)


def _publish_state_after_commit(session: AsyncSession, station_id: uuid.UUID, state: StationStateOut) -> None:
    from aicam.realtime import publish

    async def _send() -> None:
        await publish.to_station(station_id, "station.state", state.model_dump(mode="json"))

    after_commit(session, _send)


async def purge_scan_dedup(session: AsyncSession, older_than: timedelta) -> int:
    """J-11: bỏ bản ghi chống quét trùng quá hạn giữ (02 §8: `client_scan_id` giữ 10 phút)."""
    result = await session.execute(delete(ScanDedup).where(ScanDedup.created_at < clock.now() - older_than))
    return int(result.rowcount or 0)  # type: ignore[attr-defined]
