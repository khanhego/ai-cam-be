"""Phiên đóng gói: trạng thái station (API-10) và xử lý quét (API-11) — 02a §4.1, BR-01..06, BR-18."""

import asyncio
import re
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import all_, func, literal, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.settings import Settings, get_settings
from aicam.modules.approvals.queries import pending_for_station
from aicam.modules.media import jobs as media_jobs
from aicam.modules.media.queries import clips_of_session
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Package
from aicam.modules.platforms.base import PlatformAdapter, PlatformError
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession, ScanDedup, SessionEvent
from aicam.modules.sessions.schemas import (
    AlertOut,
    ApprovalBrief,
    CameraState,
    ItemOut,
    MismatchOut,
    OrderBrief,
    PackageBrief,
    RecentClip,
    RecentOut,
    RecentSession,
    ScanOut,
    SessionOut,
    StationRef,
    StationStateOut,
    TrayOut,
)
from aicam.modules.sessions.tray import Tray, read_tray
from aicam.modules.settings import service as settings_service
from aicam.modules.stations import service as stations
from aicam.modules.stations.models import Station

log = structlog.get_logger()

_STATE_BY_STATUS = {"OPEN": "PACKING", "MISMATCH": "MISMATCH", "WAITING_APPROVAL": "WAITING_APPROVAL"}


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
                PackSession.status == "COMPLETED",
                PackSession.ended_at >= start,
            )
        )
        or 0
    )


# ---------------------------------------------------------------- API-10


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
        session_out = SessionOut(
            id=current.id,
            status=current.status,
            started_at=current.started_at,
            flags=list(current.flags),
            package=await _package_brief(session, package),
            mismatch=MismatchOut(**current.mismatch) if current.mismatch else None,
            warn_at=current.started_at + timedelta(minutes=cfg.session_warn_minutes),
            abandon_at=current.started_at + timedelta(minutes=cfg.session_abandon_minutes),
        )
    if pending is not None:
        state = "WAITING_APPROVAL"
    elif current is not None:
        state = _STATE_BY_STATUS[current.status]
    else:
        state = "READY"
    return StationStateOut(
        station=StationRef(id=station.id, name=station.name),
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


def _event(session: AsyncSession, pack: PackSession, kind: str, **payload: Any) -> None:
    session.add(SessionEvent(session_id=pack.id, type=kind, payload=payload or None, at=clock.now()))


def _add_flag(pack: PackSession, flag: str) -> None:
    if flag not in pack.flags:
        pack.flags = [*pack.flags, flag]


async def _lock_station(session: AsyncSession, station_id: uuid.UUID) -> None:
    """Tuần tự hóa mọi thao tác trên một station (DEC-11). Nhả khi transaction kết thúc."""
    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"station:{station_id}"})


async def _lookup_platform(
    session: AsyncSession, code: str, adapter: PlatformAdapter, timeout_s: float
) -> Package | None:
    """BR-04: tra sàn tối đa `timeout_s`; có → ghi đơn; không / quá hạn → None."""
    try:
        found = await asyncio.wait_for(adapter.find_by_tracking(None, code), timeout=timeout_s)
    except (TimeoutError, PlatformError) as exc:
        log.info("platform_lookup_failed", code=code, error=type(exc).__name__)
        return None
    if found is None:
        return None
    try:
        # Station khác vừa ghi cùng đơn (tra ngoài lock): bỏ qua, bước mở phiên đọc lại từ DB (review #5).
        async with session.begin_nested():
            result = await orders.upsert_platform_order(session, found)
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
    package = await orders.find_package(session, code)
    if package is None:
        package = await orders.create_unverified_package(session, code)
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
            packed_at=done.ended_at.isoformat() if done and done.ended_at else None,
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
    _event(session, pack, "SCAN_OPEN", code=code)
    return "SESSION_OPENED", None


async def complete_session(
    session: AsyncSession, pack: PackSession, *, tray: Tray, close_code: str | None, actor_label: str
) -> None:
    """Đóng phiên hợp lệ: BR-18 cờ Cam 2, kiện PACKED, phiên đóng gói lại thay phiên cũ (BR-03)."""
    if not pack.cam2_seen_match or tray.match == "UNAVAILABLE":
        _add_flag(pack, "CAM2_UNVERIFIED")
    if tray.match == "MATCH":
        _add_flag(pack, "LABEL_ON_TRAY")
    pack.status = "COMPLETED"
    pack.ended_at = clock.now()
    pack.close_code = close_code
    pack.mismatch = None
    package = await _require_package(session, pack.package_id)
    await orders.transition(session, package, "PACKED", source="WAREHOUSE", actor_label=actor_label)
    if pack.supersedes_session_id:
        old = await session.get(PackSession, pack.supersedes_session_id)
        if old is not None and old.status == "COMPLETED":
            old.status = "SUPERSEDED"
    _event(session, pack, "COMPLETED", close_code=close_code)
    media_jobs.enqueue_build_clips(session, pack.id, pack.ended_at)  # J-01 sau commit


def mark_mismatch(pack: PackSession, *, source: str, actual: str) -> None:
    pack.status = "MISMATCH"
    pack.mismatch = {"source": source, "expected": pack.open_code, "actual": actual}
    _add_flag(pack, "HAD_MISMATCH")


async def _continue_session(
    session: AsyncSession, station: Station, pack: PackSession, code: str
) -> tuple[str, AlertOut | None]:
    tray = await read_tray(get_redis(), station.id, pack.open_code)
    if tray.match == "MATCH":
        pack.cam2_seen_match = True
    # Thứ tự cứng (BR-06, review #2): khay có mã khác luôn xét trước khi so mã quét.
    if tray.blocks_close:
        mark_mismatch(
            pack, source="CAM2", actual=", ".join(c for c in tray.codes if c != pack.open_code) or ""
        )
        _event(session, pack, "MISMATCH", source="CAM2", scanned=code, tray=list(tray.codes))
        return "MISMATCH", None
    if code == pack.open_code:
        await complete_session(session, pack, tray=tray, close_code=code, actor_label=station.name)
        return "SESSION_COMPLETED", None
    mark_mismatch(pack, source="SCAN", actual=code)
    _event(session, pack, "MISMATCH", source="SCAN", scanned=code)
    return "MISMATCH", None


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
    return ScanOut(outcome=previous.response["outcome"], alert=previous.response.get("alert"), state=state)


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
    # Tra sàn ngoài khóa station (02a §4.1): chỉ khi mã hợp lệ, station rảnh và mã chưa có.
    if (
        valid
        and await active_session(session, station.id) is None
        and await orders.find_package(session, code) is None
    ):
        await _lookup_platform(session, code, adapter, settings.platform_lookup_timeout_s)
        await session.flush()

    await _lock_station(session, station.id)
    # Retry chạy chồng với lần gửi đầu: kiểm lại dưới lock (review #4).
    previous = await _dedup(session, client_scan_id, station.id)
    if previous is not None:
        return await _replay(session, station, previous, settings)
    pack = await active_session(session, station.id, refresh=True)
    if await pending_for_station(session, station.id) is not None or (
        pack is not None and pack.status == "WAITING_APPROVAL"
    ):
        outcome, alert = "IGNORED", None  # xét trước định dạng mã (review #20)
    elif not valid:
        outcome, alert = (
            "ALERT",
            _alert("INVALID_CODE", "Mã vừa quét không phải mã vận đơn. Quét lại mã trên phiếu."),
        )
    elif pack is None:
        outcome, alert = await _open_session(session, station, code, settings)
    else:
        outcome, alert = await _continue_session(session, station, pack, code)

    session.add(
        ScanDedup(
            client_scan_id=client_scan_id,
            station_id=station.id,
            response={"outcome": outcome, "alert": alert.model_dump() if alert else None},
        )
    )
    await session.flush()
    state = await build_state(session, station, settings)
    notify_after_commit(session, station.id, state)
    await commit(session)
    log.info(
        "scan", station_id=str(station.id), code=code, outcome=outcome, alert=alert.code if alert else None
    )
    return ScanOut(outcome=outcome, alert=alert, state=state)


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
    pack.status = status
    pack.ended_at = clock.now()
    pack.cancel_reason = reason
    pack.note = note
    pack.mismatch = None
    package = await _require_package(session, pack.package_id)
    await orders.transition(
        session, package, pack.package_status_before or "NEW", source="WAREHOUSE", actor_label=actor_label
    )
    _event(session, pack, status, reason=reason, note=note)
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
    await _lock_station(session, station.id)
    pack = await session.get(PackSession, session_id)
    if pack is None or pack.station_id != station.id or pack.status not in ("OPEN", "MISMATCH"):
        raise AppError("SESSION_NOT_OPEN", "Phiên không còn mở.", 409)
    await end_without_packing(
        session, pack, status="CANCELLED", reason=reason, note=note and note.strip(), actor_label=station.name
    )
    await session.flush()
    state = await build_state(session, station, settings)
    notify_after_commit(session, station.id, state)
    await commit(session)
    return state


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
                tracking_number=tracking,
                status=pack.status,
                flags=list(pack.flags),
                started_at=pack.started_at,
                ended_at=pack.ended_at,
                clips=[RecentClip(id=c.id, camera_role=c.camera_role, status=c.status) for c in clips],
            )
        )
    return RecentOut(items=items)


async def check_timeouts(session: AsyncSession, settings: Settings) -> dict[str, int]:
    """J-07 (BR-16): mở quá `warn` phút → cảnh báo một lần; quá `abandon` phút → ABANDONED.

    Phiên đang chờ duyệt không bị bỏ dở (02a BR-16). Mỗi phiên: lock station → đọc lại `FOR UPDATE`
    → kiểm lại → commit riêng.
    Không ghi đè lần quét đóng vừa xảy ra; một phiên lỗi không hỏng cả lô (review M1 #2).
    """
    from aicam.realtime import publish

    cfg = await settings_service.get(session)
    warn_after = timedelta(minutes=cfg.session_warn_minutes)
    abandon_after = timedelta(minutes=cfg.session_abandon_minutes)
    candidates = (
        await session.execute(
            select(PackSession.id, PackSession.station_id).where(PackSession.status.in_(("OPEN", "MISMATCH")))
        )
    ).all()
    await session.commit()
    counts = {"warned": 0, "abandoned": 0}
    for session_id, station_id in candidates:
        try:
            await _lock_station(session, station_id)
            pack = await session.scalar(
                select(PackSession)
                .where(PackSession.id == session_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if pack is None or pack.status not in ("OPEN", "MISMATCH"):
                await rollback(session)
                continue
            age = clock.now() - pack.started_at
            if age >= abandon_after:
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
                        "minutes": cfg.session_warn_minutes,
                    },
                )
            else:
                await rollback(session)
        except Exception:
            await rollback(session)
            log.exception("check_timeouts_failed", session_id=str(session_id))
    return counts


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
    await _lock_station(session, station_id)
    pack = await active_session(session, station_id, refresh=True)
    if pack is None:
        return None
    _add_flag(pack, "VIDEO_INCOMPLETE")
    _event(session, pack, "CAMERA_OFFLINE", camera_role=camera_role)
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
