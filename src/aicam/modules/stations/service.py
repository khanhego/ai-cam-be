"""Station + camera (02a §4 API-60..65; J-08 phía api, J-09)."""

import base64
import json
import re
import uuid
from collections.abc import Sequence
from urllib.parse import urlsplit

import structlog
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.approvals.queries import pending_for_station
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession
from aicam.modules.stations.mediamtx import MediaMTX, MediaMTXError, mask, mediamtx_path, with_credentials
from aicam.modules.stations.models import Camera, Station
from aicam.modules.stations.probe import CameraUnreachable, grab_frame, onvif_clock_offset_ms
from aicam.modules.stations.schemas import (
    AccountRef,
    CameraIn,
    CameraOut,
    CameraTestOut,
    LiveCamera,
    LiveOut,
    LiveStation,
    Roi,
    StationCreateIn,
    StationOut,
    StationPatchIn,
)
from aicam.modules.users.queries import get_user_ref

log = structlog.get_logger()

VISION_CONFIG_CHANNEL = "vision.config"


async def get_station_by_account(session: AsyncSession, user_id: uuid.UUID) -> Station | None:
    result: Station | None = await session.scalar(select(Station).where(Station.account_user_id == user_id))
    return result


async def get_station(session: AsyncSession, station_id: uuid.UUID) -> Station | None:
    return await session.get(Station, station_id)


async def cameras_of(session: AsyncSession, station_id: uuid.UUID) -> Sequence[Camera]:
    return (
        await session.scalars(select(Camera).where(Camera.station_id == station_id).order_by(Camera.role))
    ).all()


def camera_out(camera: Camera) -> CameraOut:
    return CameraOut(
        id=camera.id,
        role=camera.role,
        rtsp_url_masked=mask(camera.rtsp_url),
        status=camera.status,
        roi=Roi(**camera.roi) if camera.roi else None,
        clock_offset_ms=camera.clock_offset_ms,
    )


async def station_out(session: AsyncSession, station: Station) -> StationOut:
    account = None
    if station.account_user_id:
        user = await get_user_ref(session, station.account_user_id)
        if user:
            account = AccountRef(id=user.id, username=user.username)
    return StationOut(
        id=station.id,
        name=station.name,
        is_active=station.is_active,
        account=account,
        cameras=[camera_out(c) for c in await cameras_of(session, station.id)],
        kind=station.kind,
        work_mode=station.work_mode,
        operator_name=station.operator_name,
    )


async def list_stations(session: AsyncSession) -> list[StationOut]:
    stations = (await session.scalars(select(Station).order_by(Station.name))).all()
    return [await station_out(session, s) for s in stations]


async def _require_station(session: AsyncSession, station_id: uuid.UUID) -> Station:
    station = await session.get(Station, station_id)
    if station is None:
        raise AppError("NOT_FOUND", "Không tìm thấy station.", 404)
    return station


async def _check_name(session: AsyncSession, name: str, exclude: uuid.UUID | None = None) -> None:
    query = select(Station.id).where(func.lower(Station.name) == name.strip().lower())
    if exclude:
        query = query.where(Station.id != exclude)
    if await session.scalar(query):
        raise AppError("NAME_TAKEN", "Tên station đã tồn tại.", 409, {"fields": {"name": "Đã tồn tại"}})


async def _check_account(session: AsyncSession, user_id: uuid.UUID, exclude: uuid.UUID | None = None) -> None:
    user = await get_user_ref(session, user_id)
    if user is None or user.role != "STATION":
        raise AppError(
            "VALIDATION_ERROR",
            "Dữ liệu không hợp lệ.",
            422,
            {"fields": {"account_user_id": "Phải là tài khoản loại Station"}},
        )
    query = select(Station.id).where(Station.account_user_id == user_id)
    if exclude:
        query = query.where(Station.id != exclude)
    if await session.scalar(query):
        raise AppError(
            "ACCOUNT_IN_USE",
            "Tài khoản station đã gắn với station khác.",
            409,
            {"fields": {"account_user_id": "Đã gắn station khác"}},
        )


async def _flush_or_conflict(session: AsyncSession) -> None:
    """Hai request song song cùng tên / cùng tài khoản: unique index chặn, trả lỗi đúng mã."""
    try:
        await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        if "account_user_id" in str(exc.orig):
            raise AppError("ACCOUNT_IN_USE", "Tài khoản station đã gắn với station khác.", 409) from exc
        raise AppError("NAME_TAKEN", "Tên station đã tồn tại.", 409) from exc


async def lock_station(session: AsyncSession, station_id: uuid.UUID) -> None:
    """Cùng khóa advisory với `sessions.lock_station` (DEC-11): đổi cấu hình bàn tuần tự với quét."""
    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"station:{station_id}"})


async def is_busy(session: AsyncSession, station_id: uuid.UUID) -> bool:
    """Có phiên đang hoạt động hoặc yêu cầu duyệt đang chờ (API-60 `kind`, API-100, API-101)."""
    active = await session.scalar(
        select(PackSession.id)
        .where(PackSession.station_id == station_id, PackSession.status.in_(ACTIVE_STATUSES))
        .limit(1)
    )
    return active is not None or await pending_for_station(session, station_id) is not None


def default_work_mode(kind: str, current: str | None = None) -> str:
    """02 API-60: đổi sang PACK / RETURN → `work_mode = kind`; sang BOTH giữ chế độ đang chạy."""
    if kind != "BOTH":
        return kind
    return current or "PACK"


async def clear_operator(session: AsyncSession, account_user_id: uuid.UUID) -> bool:
    """BR-28 (02 §6.3 #17): station đăng xuất (API-03) / bị thu hồi phiên (API-91) → xóa tên người kiểm."""
    station = await get_station_by_account(session, account_user_id)
    if station is None or station.operator_name is None:
        return False
    station.operator_name = None
    return True


async def create_station(
    session: AsyncSession, data: StationCreateIn, actor: uuid.UUID, ip: str | None
) -> StationOut:
    await _check_name(session, data.name)
    if data.account_user_id:
        await _check_account(session, data.account_user_id)
    station = Station(
        name=data.name.strip(),
        account_user_id=data.account_user_id,
        kind=data.kind,
        work_mode=default_work_mode(data.kind),
    )
    session.add(station)
    await _flush_or_conflict(session)
    audit.record(session, "STATION_UPDATE", user_id=actor, object_type="STATION", object_id=station.id, ip=ip,
                 data={"op": "create", "name": station.name, "kind": station.kind})  # fmt: skip
    out = await station_out(session, station)
    await commit(session)
    return out


async def patch_station(
    session: AsyncSession, station_id: uuid.UUID, data: StationPatchIn, actor: uuid.UUID, ip: str | None
) -> tuple[StationOut, bool]:
    """Trả (station, đổi chế độ) — người gọi đẩy `station.state` khi chế độ đổi."""
    station = await _require_station(session, station_id)
    changes = data.model_dump(exclude_unset=True)
    mode_changed = False
    if data.kind is not None and data.kind != station.kind:
        await lock_station(session, station.id)
        # G3 BB-16: đọc lại sau khóa — API-100 đổi chế độ / patch khác vừa commit thì quyết theo giá trị mới.
        await session.refresh(station)
        if await is_busy(session, station.id):
            raise AppError(
                "STATION_BUSY", "Station đang có phiên hoặc yêu cầu duyệt. Đổi loại sau khi xong.", 409
            )
        new_mode = default_work_mode(data.kind, station.work_mode)
        mode_changed = new_mode != station.work_mode
        station.kind, station.work_mode = data.kind, new_mode
    if data.name is not None:
        await _check_name(session, data.name, exclude=station.id)
        station.name = data.name.strip()
    if "account_user_id" in changes:
        if data.account_user_id:
            await _check_account(session, data.account_user_id, exclude=station.id)
        station.account_user_id = data.account_user_id
    if data.is_active is not None:
        station.is_active = data.is_active
    await _flush_or_conflict(session)
    audit.record(session, "STATION_UPDATE", user_id=actor, object_type="STATION", object_id=station.id, ip=ip,
                 data={k: str(v) for k, v in changes.items()})  # fmt: skip
    out = await station_out(session, station)
    await commit(session)
    return out, mode_changed


async def set_camera(
    session: AsyncSession,
    station_id: uuid.UUID,
    role: str,
    data: CameraIn,
    *,
    mediamtx: MediaMTX,
    settings: Settings,
    actor: uuid.UUID,
    ip: str | None,
) -> CameraOut:
    await _require_station(session, station_id)
    camera = await session.scalar(select(Camera).where(Camera.station_id == station_id, Camera.role == role))
    if camera is None:
        camera = Camera(station_id=station_id, role=role, rtsp_url=data.rtsp_url, mediamtx_path="")
        session.add(camera)
        await session.flush()
        camera.mediamtx_path = mediamtx_path(camera.id)
    cipher = Cipher(settings.fernet_key)
    camera.rtsp_url = data.rtsp_url
    # Không gửi = giữ giá trị cũ (form sửa không hiện lại tài khoản / mật khẩu); "" = xóa (review M1 #11).
    if data.username is not None:
        camera.username = data.username or None
    if data.password is not None:
        camera.password_enc = cipher.encrypt(data.password) if data.password else None
    password = cipher.decrypt(camera.password_enc) if camera.password_enc else None
    audit.record(session, "CAMERA_UPDATE", user_id=actor, object_type="CAMERA", object_id=camera.id, ip=ip,
                 data={"role": role, "rtsp_url": mask(data.rtsp_url)})  # fmt: skip
    source = with_credentials(data.rtsp_url, camera.username, password)
    path = camera.mediamtx_path
    out = camera_out(camera)
    await commit(session)
    # Lỗi MediaMTX không chặn việc lưu (02 API-61): camera giữ OFFLINE tới khi J-08 thấy luồng.
    try:
        await mediamtx.upsert_path(path, source)
    except MediaMTXError as exc:
        log.warning("mediamtx_upsert_failed", camera_id=str(out.id), error=str(exc))
    return out


async def probe_camera(data: CameraIn) -> CameraTestOut:
    source = with_credentials(data.rtsp_url, data.username, data.password)
    try:
        frame = await grab_frame(source)
    except CameraUnreachable as exc:
        raise AppError(
            "CAMERA_UNREACHABLE",
            "Không kết nối được camera. Kiểm tra địa chỉ và mật khẩu camera.",
            422,
            {"reason": exc.reason},
        ) from exc
    host = urlsplit(data.rtsp_url).hostname
    offset = await onvif_clock_offset_ms(host) if host else None
    return CameraTestOut(
        ok=True,
        snapshot="data:image/jpeg;base64," + base64.b64encode(frame).decode(),
        clock_offset_ms=offset,
    )


async def _require_camera(session: AsyncSession, camera_id: uuid.UUID) -> Camera:
    camera = await session.get(Camera, camera_id)
    if camera is None:
        raise AppError("NOT_FOUND", "Không tìm thấy camera.", 404)
    return camera


async def snapshot(session: AsyncSession, camera_id: uuid.UUID, settings: Settings) -> bytes:
    camera = await _require_camera(session, camera_id)
    try:
        return await grab_frame(f"{settings.mediamtx_rtsp_url}/{camera.mediamtx_path}")
    except CameraUnreachable as exc:
        raise AppError(
            "CAMERA_UNREACHABLE", "Không lấy được ảnh từ camera.", 422, {"reason": exc.reason}
        ) from exc


async def set_roi(
    session: AsyncSession, camera_id: uuid.UUID, roi: Roi, actor: uuid.UUID, ip: str | None
) -> CameraOut:
    camera = await _require_camera(session, camera_id)
    if camera.role != "CAM2":
        raise AppError("ROI_ONLY_CAM2", "Chỉ Cam 2 có vùng đọc mã.", 409)
    camera.roi = roi.model_dump()
    audit.record(session, "CAMERA_UPDATE", user_id=actor, object_type="CAMERA", object_id=camera.id, ip=ip,
                 data={"roi": camera.roi})  # fmt: skip

    async def _notify_vision() -> None:
        await get_redis().publish(VISION_CONFIG_CHANNEL, json.dumps({"camera_id": str(camera_id)}))

    after_commit(session, _notify_vision)
    out = camera_out(camera)
    await commit(session)
    return out


async def live(session: AsyncSession) -> LiveOut:
    stations = (
        await session.scalars(select(Station).where(Station.is_active.is_(True)).order_by(Station.name))
    ).all()
    out = []
    for station in stations:
        cams = [
            LiveCamera(id=c.id, role=c.role, status=c.status, whep_url=f"/live/{c.mediamtx_path}/whep")
            for c in await cameras_of(session, station.id)
        ]
        out.append(LiveStation(id=station.id, name=station.name, cameras=cams))
    return LiveOut(stations=out)


async def watched_paths(session: AsyncSession) -> list[str]:
    """Path MediaMTX cần theo dõi (camera thuộc station đang bật)."""
    rows = await session.scalars(
        select(Camera.mediamtx_path)
        .join(Station, Station.id == Camera.station_id)
        .where(Station.is_active.is_(True))
    )
    return list(rows.all())


async def vision_cameras(session: AsyncSession) -> Sequence[Camera]:
    """Camera của station đang bật — tiến trình vision đọc mã khay trên Cam 2 (T-12) và giữ khung mới nhất của
    mọi camera (Cam 1 + Cam 2) cho ảnh chụp (T-121)."""
    rows = await session.scalars(
        select(Camera)
        .join(Station, Station.id == Camera.station_id)
        .where(Station.is_active.is_(True), Camera.role.in_(("CAM1", "CAM2")))
        .order_by(Camera.station_id, Camera.role)
    )
    return rows.all()


async def apply_camera_health(session: AsyncSession, path: str, status: str) -> Camera | None:
    """Ghi trạng thái camera do J-08 phát hiện (subscriber `camera.health` trong api)."""
    camera = await session.scalar(select(Camera).where(Camera.mediamtx_path == path))
    if camera is None or camera.status == status:
        return None
    camera.status = status
    # ONLINE: lúc có lại tín hiệu; OFFLINE: lúc mất tín hiệu = lần cuối còn thấy (N01 "mất > 60 giây",
    # dedupe `cam:{id}:{last_seen_at}`, "(đã có lại HH:MM)" — DEC-732).
    camera.last_seen_at = clock.now()
    await commit(session)
    return camera


async def update_clock_offsets(session: AsyncSession) -> int:
    """J-09: đo lệch giờ mọi camera qua ONVIF; không có ONVIF → null (DEC-33)."""
    cameras = (await session.scalars(select(Camera))).all()
    for camera in cameras:
        host = urlsplit(camera.rtsp_url).hostname
        camera.clock_offset_ms = await onvif_clock_offset_ms(host) if host else None
    await commit(session)
    return len(cameras)


_CAMERA_PATH_RE = re.compile(r"cam-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


async def reconcile_mediamtx(session: AsyncSession, mediamtx: MediaMTX, settings: Settings) -> dict[str, int]:
    """Một phần J-10 (DEC-102): đưa cấu hình path MediaMTX về khớp DB.

    - Camera của station đang bật mà MediaMTX không có path (MediaMTX khởi động lại làm mất path thêm qua
      API) → thêm lại, nếu không camera ngừng ghi và mất bằng chứng.
    - Path dạng `cam-<uuid>` không còn camera trong DB (DB reset, test QA) → xóa để không ghi video rác.
      Bỏ qua bước xóa khi DB chưa có camera nào (phòng trỏ nhầm DB làm xóa sạch).
    """
    current = await mediamtx.list_paths()
    rows = (
        await session.execute(
            select(Camera, Station.is_active).join(Station, Station.id == Camera.station_id)
        )
    ).all()
    await session.commit()
    known = {cam.mediamtx_path for cam, _ in rows}
    cipher = Cipher(settings.fernet_key)
    added = removed = 0
    for cam, active in rows:
        if not active or cam.mediamtx_path in current:
            continue
        password = cipher.decrypt(cam.password_enc) if cam.password_enc else None
        try:
            await mediamtx.upsert_path(
                cam.mediamtx_path, with_credentials(cam.rtsp_url, cam.username, password)
            )
            added += 1
        except MediaMTXError as exc:
            log.warning("mediamtx_readd_failed", camera_id=str(cam.id), error=str(exc))
    if known:
        for name in current:
            if _CAMERA_PATH_RE.fullmatch(name) and name not in known:
                try:
                    await mediamtx.delete_path(name)
                    removed += 1
                except MediaMTXError as exc:
                    log.warning("mediamtx_delete_failed", path=name, error=str(exc))
    if added or removed:
        log.info("mediamtx_reconciled", added=added, removed=removed)
    return {"added": added, "removed": removed}
