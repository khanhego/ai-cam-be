"""Đăng ký task Celery. Mỗi task mở engine + Redis riêng (Celery đồng bộ, gọi coroutine qua asyncio.run)."""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.redis import close_redis, init_redis
from aicam.core.settings import get_settings
from aicam.modules.backup import jobs as backup_jobs
from aicam.modules.claims import pack as claim_packs
from aicam.modules.claims import service as claims
from aicam.modules.imports import service as imports
from aicam.modules.media import exports, jobs, snapshots
from aicam.modules.media import service as media
from aicam.modules.platforms import budget
from aicam.modules.platforms import dispatch as platform_dispatch
from aicam.modules.platforms import sync as platform_sync
from aicam.modules.reconciliation import service as reconciliation
from aicam.modules.sessions import service as sessions
from aicam.modules.stations import service as stations
from aicam.modules.stations.mediamtx import HttpMediaMTX, MediaMTXError
from aicam.workers.celery_app import app

log = structlog.get_logger()


def _run[T](job: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def _wrapped() -> T:
        settings = get_settings()
        init_engine(settings.database_url)
        init_redis(settings.redis_url)
        try:
            async with sessionmaker()() as session:
                return await job(session)
        finally:
            await close_redis()
            await dispose_engine()

    return asyncio.run(_wrapped())


@app.task(name="stations.check_clock_drift", soft_time_limit=120)  # type: ignore[untyped-decorator]
def check_clock_drift() -> int:
    """J-09 (02a §7): mỗi 10 phút."""
    return _run(stations.update_clock_offsets)


@app.task(name="sessions.check_timeouts", soft_time_limit=25)  # type: ignore[untyped-decorator]
def check_timeouts() -> dict[str, int]:
    """J-07 (02a §7): mỗi 30 giây, BR-16."""
    return _run(lambda db: sessions.check_timeouts(db, get_settings()))


@app.task(  # type: ignore[untyped-decorator]
    name="media.capture_pack_snapshot", bind=True, max_retries=3, default_retry_delay=30, soft_time_limit=60
)
def capture_pack_snapshot(self: Any, session_id: str) -> str:
    """J-17 (queue `video`, 02a §7): ảnh Cam 1 lúc đóng gói từ clip gốc; lỗi → thử lại 3 lần / 30 giây."""
    try:
        return _run(lambda db: snapshots.capture_pack_snapshot(db, uuid.UUID(session_id), get_settings()))
    except Exception as exc:  # ffmpeg / IO: thiếu ảnh không chặn gì, chỉ thử lại rồi log
        if self.request.retries >= self.max_retries:
            log.error("pack_snapshot_failed", session_id=session_id, error=str(exc)[:300])
            return "failed"
        raise self.retry(exc=exc) from exc


@app.task(name="claims.check_deadlines", soft_time_limit=120)  # type: ignore[untyped-decorator]
def check_claim_deadlines() -> int:
    """J-15 (60 phút, 02a §7): hồ sơ khiếu nại sắp hết hạn → ghi chú + WS (FR-08.04)."""
    return _run(claims.check_deadlines)


@app.task(name="sessions.flag_order_cancelled", soft_time_limit=60)  # type: ignore[untyped-decorator]
def flag_order_cancelled(package_id: str, kind: str = "CANCELLED") -> str:
    """BR-21 (02a §5, DEC-266): đơn hủy khi kiện đang đóng → gắn cờ phiên / hủy sau khi đóng (R3-8)."""
    return _run(
        lambda db: sessions.flag_order_cancelled(db, uuid.UUID(package_id), get_settings(), kind=kind)
    )


@app.task(  # type: ignore[untyped-decorator]
    name="media.build_session_clips", bind=True, max_retries=3, soft_time_limit=120, time_limit=150
)
def build_session_clips(self: Any, session_id: str) -> dict[str, Any]:
    """J-01 (queue `video`): cắt clip sau khi phiên kết thúc. Thử lại 3 lần; lần cuối lỗi → clip FAILED."""
    final = self.request.retries >= self.max_retries
    result = _run(
        lambda db: media.build_session_clips(db, uuid.UUID(session_id), get_settings(), final=final)
    )
    if result.waiting and result.retry_in is not None:
        # Chưa tới giờ cắt: hẹn lại với **cùng** số lần thử — chờ settle không tiêu lượt retry, kể cả khi job
        # đến sớm ở lượt cuối (vd API-46 / J-11 đẩy lại ngay sau khi đóng phiên).
        self.apply_async(args=[session_id], countdown=result.retry_in, retries=self.request.retries)
        return {"ready": 0, "failed": 0, "waiting_s": round(result.retry_in, 1)}
    if result.retry_in is not None and not final:
        raise self.retry(countdown=result.retry_in)
    return {"ready": len(result.ready), "failed": len(result.failed)}


@app.task(name="media.index_segments", soft_time_limit=55, time_limit=58)  # type: ignore[untyped-decorator]
def index_segments() -> dict[str, int]:
    """J-10 (mỗi phút): đồng bộ path MediaMTX với DB, index segment mới, dọn bản xuất hết hạn."""

    async def _job(db: AsyncSession) -> dict[str, int]:
        settings = get_settings()
        out: dict[str, int] = {}
        try:
            out.update(
                await stations.reconcile_mediamtx(db, HttpMediaMTX(settings.mediamtx_api_url), settings)
            )
        except MediaMTXError as exc:
            log.warning("mediamtx_unreachable", error=str(exc))
        out["indexed"] = await media.index_segments(db, settings)
        out["exports_expired"] = await exports.cleanup_expired(db, settings)
        out["evidence_packs_expired"] = await claim_packs.cleanup_expired(db, settings)
        return out

    return _run(_job)


@app.task(name="media.enforce_retention", soft_time_limit=1800)  # type: ignore[untyped-decorator]
def enforce_retention() -> dict[str, int]:
    """J-02 (02:00 giờ VN): xóa video thô / clip quá hạn theo setting lúc chạy (BR-09, AC-15, AC-20)."""
    return _run(lambda db: media.enforce_retention(db, get_settings()))


@app.task(name="media.render_export", soft_time_limit=660, time_limit=700)  # type: ignore[untyped-decorator]
def render_export(export_id: str) -> str:
    """J-03 (queue `export`, worker riêng concurrency 1 — DEC-32): encode bản xuất có overlay."""
    return _run(lambda db: exports.render_export(db, uuid.UUID(export_id), get_settings()))


@app.task(name="claims.build_evidence_pack", soft_time_limit=900, time_limit=960)  # type: ignore[untyped-decorator]
def build_evidence_pack(pack_id: str) -> str:
    """J-16 (queue `export`, cùng worker J-03 concurrency 1): dựng gói bằng chứng zip (FR-08.05)."""
    return _run(lambda db: claim_packs.build_evidence_pack(db, uuid.UUID(pack_id), get_settings()))


@app.task(name="maintenance.housekeeping", soft_time_limit=240)  # type: ignore[untyped-decorator]
def housekeeping() -> dict[str, int]:
    """J-11 (5 phút): dọn scan_dedup > 10 phút, CSV hết hạn; J-01 lại cho phiên thiếu clip."""

    async def _job(db: AsyncSession) -> dict[str, int]:
        settings = get_settings()
        out = {
            "scan_dedup": await sessions.purge_scan_dedup(db, timedelta(minutes=10)),
            "imports_expired": await imports.expire_previews(db),
            "import_files": await imports.purge_old_files(db, settings.import_root),
        }
        await db.commit()
        missing = await media.sessions_missing_clips(db, timedelta(minutes=5), timedelta(days=1))
        for session_id in missing:
            await jobs.send(jobs.BUILD_CLIPS, [str(session_id)], "video")
        out["clip_jobs"] = len(missing)
        return out

    return _run(_job)


# ---------------------------------------------------------------- Sàn (T-22; fan-out một task / shop — T-205)
# Task phân phối (beat, không tham số) → `dispatch.dispatch` gửi một task / shop; task shop chạy trong ngân
# sách riêng (`SYNC_TASK_BUDGET_S` / `SYNC_LONG_TASK_BUDGET_S`) < `soft_time_limit` (còn thời gian ghi lỗi,
# nhả lock).
# Tên cũ có `shop_id` (message API-73 / callback Phase 2 còn trong hàng đợi) vẫn chạy như task shop.


def _shop_uuid(shop_id: str | None) -> uuid.UUID | None:
    return uuid.UUID(shop_id) if shop_id else None


@app.task(name="platforms.sync_orders", soft_time_limit=150, time_limit=180)  # type: ignore[untyped-decorator]
def sync_orders(shop_id: str | None = None, lock_held: bool | str = False) -> dict[str, Any]:
    """J-04 (5 phút): không `shop_id` → phân phối một task / shop; có → như `platforms.sync_shop_orders`."""
    settings = get_settings()
    if shop_id is None:
        return _run(lambda db: platform_dispatch.dispatch(db, settings, platform_dispatch.ORDERS))
    return _run(
        lambda db: platform_dispatch.run_shop(
            db, settings, platform_dispatch.ORDERS, _shop_uuid(shop_id), lock_held=lock_held
        )
    )


@app.task(name="platforms.sync_shop_orders", soft_time_limit=150, time_limit=180)  # type: ignore[untyped-decorator]
def sync_shop_orders(shop_id: str, lock_held: bool | str = False) -> dict[str, Any]:
    """J-04 một shop (ngân sách 120 giây — 02a §7)."""
    settings = get_settings()
    return _run(
        lambda db: platform_dispatch.run_shop(
            db, settings, platform_dispatch.ORDERS, _shop_uuid(shop_id), lock_held=lock_held
        )
    )


@app.task(name="platforms.verify_unverified", soft_time_limit=150)  # type: ignore[untyped-decorator]
def verify_unverified() -> dict[str, int]:
    """J-05 (10 phút): xác minh lại kiện chưa xác minh (BR-04)."""
    settings = get_settings()

    async def _job(db: AsyncSession) -> dict[str, int]:
        with budget.time_budget(settings.sync_task_budget_s):
            return await platform_sync.verify_unverified(db, None, settings)

    return _run(_job)


@app.task(name="platforms.sync_shipping_status", soft_time_limit=60)  # type: ignore[untyped-decorator]
def sync_shipping_status() -> dict[str, Any]:
    """J-06 (15 phút): phân phối một task / shop."""
    settings = get_settings()
    return _run(lambda db: platform_dispatch.dispatch(db, settings, platform_dispatch.SHIPPING))


@app.task(name="platforms.sync_shop_shipping", soft_time_limit=330, time_limit=360)  # type: ignore[untyped-decorator]
def sync_shop_shipping(shop_id: str | None = None) -> dict[str, Any]:
    """J-06 một shop (ngân sách 300 giây): vận chuyển → HANDED_OVER / DELIVERED; hủy sau đóng (EX-P10)."""
    settings = get_settings()
    return _run(
        lambda db: platform_dispatch.run_shop(db, settings, platform_dispatch.SHIPPING, _shop_uuid(shop_id))
    )


@app.task(name="platforms.refresh_tokens", soft_time_limit=120)  # type: ignore[untyped-decorator]
def refresh_tokens() -> dict[str, Any]:
    """J-12 (30 phút): làm mới token theo grant (DEC-433, 507); bị từ chối → shop EXPIRED."""
    settings = get_settings()
    return _run(lambda db: platform_dispatch.refresh_all(db, settings))


@app.task(name="platforms.sync_returns", soft_time_limit=330, time_limit=360)  # type: ignore[untyped-decorator]
def sync_returns(shop_id: str | None = None) -> dict[str, Any]:
    """J-13 (15 phút): không `shop_id` → phân phối; có → như `platforms.sync_shop_returns`."""
    settings = get_settings()
    if shop_id is None:
        return _run(lambda db: platform_dispatch.dispatch(db, settings, platform_dispatch.RETURNS))
    return _run(
        lambda db: platform_dispatch.run_shop(db, settings, platform_dispatch.RETURNS, _shop_uuid(shop_id))
    )


@app.task(name="platforms.sync_shop_returns", soft_time_limit=330, time_limit=360)  # type: ignore[untyped-decorator]
def sync_shop_returns(shop_id: str) -> dict[str, Any]:
    """J-13 một shop (ngân sách 300 giây): yêu cầu trả → hồ sơ hàng hoàn."""
    settings = get_settings()
    return _run(
        lambda db: platform_dispatch.run_shop(db, settings, platform_dispatch.RETURNS, _shop_uuid(shop_id))
    )


@app.task(  # type: ignore[untyped-decorator]
    name="reconciliation.run_rules", soft_time_limit=get_settings().recon_run_soft_limit_s
)
def run_recon_rules() -> dict[str, Any]:
    """J-14 (30 phút; sau J-04 / J-06 / J-13 có thay đổi; API-123): đối soát 7 quy tắc (FR-06.02, 06.06)."""
    return _run(lambda db: reconciliation.run_rules(db, get_settings()))


# ---------------------------------------------------------------- Sao lưu cloud (02a §7 J-20..J-23, queue
# `backup`)

BACKUP_DB_RETRIES = 2  # 02a J-20: "Celery 2 lần thử (10 phút)"


@app.task(  # type: ignore[untyped-decorator]
    name="backup.run_db",
    bind=True,
    max_retries=BACKUP_DB_RETRIES,
    soft_time_limit=get_settings().backup_db_budget_s + 120,
    time_limit=get_settings().backup_db_budget_s + 180,
)
def backup_run_db(self: Any, run_id: str | None = None, trigger: str = "SCHEDULE") -> dict[str, Any]:
    """J-20 (01 / 07 / 13 / 19 giờ VN; API-184 gửi `run_id`): pg_dump + file nhập → mã hóa → tải → kiểm đọc
    lại. Lượt `FAILED` (không phải do trạng thái) → thử lại sau 10 phút như một lượt mới."""
    settings = get_settings()
    rid = uuid.UUID(run_id) if run_id else None
    result = _run(lambda db: backup_jobs.run_db(db, settings, run_id=rid, trigger=trigger))
    if result.get("status") == "FAILED" and "state" not in result and self.request.retries < self.max_retries:
        raise self.retry(countdown=600, kwargs={"run_id": None, "trigger": trigger})
    return result
