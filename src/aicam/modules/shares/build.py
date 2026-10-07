"""J-24 `shares.build` — dựng + tải link chia sẻ (02a §7 J-24, §7.4; FR-07.05, 07.07; NFR-42, NFR-46).

Mỗi phiên trong `share_item` (chốt lúc API-160): kiểm Cam 1 (và Cam 2 nếu ghép) còn `READY` + SHA-256 tệp gốc
khớp DB (ADR-008 — không dựng từ tệp đã bị sửa) → `render_side_by_side_to` (H.264 `+faststart`) → tải
`share/{token}/v{n}.mp4`; ảnh `READY` đã chốt → `p{n}-{k}.jpg` (bỏ ảnh không còn `READY` — §5.2 #10) → W1
`index.html` (`text/html`, `no-store`) chứa URL ký hạn = hạn link → khóa `share_link` `FOR UPDATE`: còn
`CREATING` → `ACTIVE` + `url_enc` (Fernet); bị thu hồi giữa chừng → xóa mọi đối tượng đã tải, dừng.

Lỗi: `RENDER_FAILED` (clip mất / lệch / FFmpeg) · `UPLOAD_FAILED` (kho lưu) · `TIMEOUT`
(`SHARE_BUILD_TIMEOUT_S`) → `FAILED`, xóa đối tượng đã tải (lỗi xóa → J-25 dọn sau).
Không log URL / token (NFR-42).
"""

import asyncio
import hashlib
import io
import shutil
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import structlog
from redis import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import commit
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.ratelimit import TokenBucket, mark_share_active
from aicam.modules.cloud.store import CloudError, ObjectStore
from aicam.modules.media import ffmpeg
from aicam.modules.media.exports import Rendered, render_side_by_side_to, session_lock
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.media.service import absolute
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings import service as settings_service
from aicam.modules.shares import w1
from aicam.modules.shares.models import ShareItem, ShareLink
from aicam.modules.shares.service import publish_updated
from aicam.modules.stations.models import Station

log = structlog.get_logger()

Renderer = Callable[..., Awaitable[Rendered]]
MAX_PRESIGN_S = 604_800  # SigV4: tối đa 7 ngày (BR-34)

MESSAGES = {
    "RENDER_FAILED": "Không dựng được video. Bấm Thử lại; nếu vẫn lỗi, báo Admin kèm mã hồ sơ.",
    "UPLOAD_FAILED": "Không tải được lên kho lưu cloud. Kiểm tra Internet rồi bấm Thử lại.",
    "TIMEOUT": "Dựng link quá thời gian cho phép. Bấm Thử lại; chọn ít phiên hơn nếu vẫn lỗi.",
}


class BuildError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass
class _Built:
    item: ShareItem
    pack: PackSession
    station: str | None
    video_key: str
    photo_keys: list[str]


def _bucket(settings: Settings, mbps: int) -> tuple[Redis | None, Callable[[int], None] | None]:
    """Token bucket chung với J-22, ưu tiên (`share:active` — J-22 nhường lượt).

    Redis lỗi → không giới hạn."""
    try:
        redis = Redis.from_url(settings.redis_url)
        bucket = TokenBucket(redis, mbps)
        mark_share_active(redis)
        return redis, bucket.priority_throttle
    except Exception:  # pragma: no cover — Redis mất: vẫn dựng link (chậm hơn không quan trọng)
        log.warning("share_build_no_ratelimit")
        return None, None


async def _put(
    store: ObjectStore,
    key: str,
    data: bytes | Path,
    content_type: str,
    throttle: Callable[[int], None] | None,
    cache_control: str | None = None,
) -> int:
    def _go() -> int:
        if isinstance(data, Path):
            with data.open("rb") as fh:
                return store.put_stream(
                    key, fh, content_type=content_type, throttle=throttle, cache_control=cache_control
                )
        return store.put_stream(
            key, io.BytesIO(data), content_type=content_type, throttle=throttle, cache_control=cache_control
        )

    try:
        return await asyncio.to_thread(_go)
    except CloudError as exc:
        raise BuildError("UPLOAD_FAILED", exc.code) from exc


async def _check_source(settings: Settings, clip: Clip) -> None:
    """Clip gốc còn `READY`, có tệp, SHA-256 tệp = DB (không dựng link từ tệp mất / bị sửa — ADR-008)."""
    if clip.status != "READY" or not clip.path:
        raise BuildError("RENDER_FAILED", f"clip {clip.camera_role} {clip.status}")
    path = absolute(settings, clip.path)
    try:
        sha = await asyncio.to_thread(ffmpeg.sha256_file, path)
    except FileNotFoundError as exc:
        raise BuildError("RENDER_FAILED", f"clip {clip.camera_role} không có tệp") from exc
    if clip.sha256 and sha != clip.sha256:
        raise BuildError("RENDER_FAILED", f"clip {clip.camera_role} lệch SHA-256")


async def _still_creating(db: AsyncSession, share_id: uuid.UUID) -> bool:
    status = await db.scalar(
        select(ShareLink.status).where(ShareLink.id == share_id).execution_options(populate_existing=True)
    )
    return status == "CREATING"


async def build(
    db: AsyncSession,
    share_id: uuid.UUID,
    settings: Settings,
    *,
    store: ObjectStore | None = None,
    render: Renderer = render_side_by_side_to,
) -> str:
    """J-24 (queue `export`): một lần dựng mỗi link (khóa advisory `share:{id}` — giao trùng → SKIPPED)."""
    async with session_lock(db, f"share:{share_id}") as got:
        if not got:
            log.warning("share_already_building", share_id=str(share_id))
            return "SKIPPED"
        return await _build(db, share_id, settings, store, render)


async def _build(
    db: AsyncSession,
    share_id: uuid.UUID,
    settings: Settings,
    store: ObjectStore | None,
    render: Renderer,
) -> str:
    link = await db.get(ShareLink, share_id, populate_existing=True)
    if link is None or link.status != "CREATING":
        return "SKIPPED"
    started = time.monotonic()
    link.job_started_at = clock.now()
    link.step, link.progress = "RENDERING", 0
    publish_updated(db, link)
    await commit(db)
    work = absolute(settings, f"shares/{link.id}")
    shutil.rmtree(work, ignore_errors=True)
    uploaded: list[str] = []
    redis: Redis | None = None
    sessions = 0
    try:
        store = store or cloud.share_store(settings)
        cfg = await settings_service.get(db)
        redis, throttle = _bucket(settings, cfg.backup_upload_mbps)
        async with asyncio.timeout(settings.share_build_timeout_s):
            result = await _build_steps(db, link, settings, store, render, work, uploaded, throttle)
        sessions = result
        outcome = "ACTIVE"
    except TimeoutError:
        outcome = await _fail(db, share_id, "TIMEOUT", "timeout", store, uploaded)
    except BuildError as exc:
        outcome = await _fail(db, share_id, exc.code, exc.detail, store, uploaded)
    except (ffmpeg.FFmpegError, OSError) as exc:
        outcome = await _fail(db, share_id, "RENDER_FAILED", type(exc).__name__, store, uploaded)
    except _Revoked:
        outcome = await _abandon(db, share_id, store, uploaded)
    except Exception as exc:  # lỗi lạ: không để link kẹt CREATING
        log.exception("share_build_error", share_id=str(share_id))
        await db.rollback()
        outcome = await _fail(db, share_id, "RENDER_FAILED", type(exc).__name__, store, uploaded)
    finally:
        shutil.rmtree(work, ignore_errors=True)
        if redis is not None:
            redis.close()
    log.info(
        "share_build",
        share_id=str(share_id),
        status=outcome,
        sessions=sessions,
        duration_s=round(time.monotonic() - started, 1),
    )  # không URL / token (NFR-42)
    return outcome


class _Revoked(Exception):
    """Link bị thu hồi / không còn `CREATING` trong lúc dựng."""


async def _build_steps(
    db: AsyncSession,
    link: ShareLink,
    settings: Settings,
    store: ObjectStore,
    render: Renderer,
    work: Path,
    uploaded: list[str],
    throttle: Callable[[int], None] | None,
) -> int:
    items = list(
        (
            await db.scalars(select(ShareItem).where(ShareItem.share_id == link.id).order_by(ShareItem.ord))
        ).all()
    )
    if not items:
        raise BuildError("RENDER_FAILED", "link không có phiên")
    total = len(items)
    prefix = link.object_prefix
    built: list[_Built] = []

    async def _set(step: str, progress: int, index: int | None = None) -> None:
        link.step, link.progress = step, progress
        link.step_index, link.step_total = index, total if index is not None else None
        publish_updated(db, link)
        await commit(db)

    for n, item in enumerate(items, start=1):
        if not await _still_creating(db, link.id):
            raise _Revoked
        base, share = 90 * (n - 1) // total, 90 // total
        await _set("RENDERING", base, n)
        pack = await db.get(PackSession, item.session_id)
        if pack is None:
            raise BuildError("RENDER_FAILED", "phiên không còn")
        clips = {
            c.camera_role: c for c in (await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all()
        }
        cam1 = clips.get("CAM1")
        if cam1 is None:
            raise BuildError("RENDER_FAILED", f"phiên {n} không có clip Cam 1")
        await _check_source(settings, cam1)
        sources = [cam1]
        cam2 = clips.get("CAM2")
        if link.layout == "SIDE_BY_SIDE" and cam2 is not None and cam2.status == "READY" and cam2.path:
            await _check_source(settings, cam2)
            sources.append(cam2)
        video = work / f"v{n}.mp4"

        async def _progress(pct: int, base: int = base, share: int = share) -> None:
            pct_total = base + share * pct // 200  # nửa đầu phần của phiên: dựng
            if pct_total - link.progress >= 5:
                link.progress = pct_total
                publish_updated(db, link)
                await commit(db)

        rendered = await render(db, pack, sources, video, settings, _progress)
        size = video.stat().st_size
        await _set("UPLOADING", base + share // 2, n)
        key = f"{prefix}v{n}.mp4"
        uploaded.append(key)
        await _put(store, key, video, "video/mp4", throttle)
        video.unlink(missing_ok=True)
        photo_keys: list[str] = []
        kept: list[uuid.UUID] = []
        for snap_id in item.snapshot_ids or []:
            snap = await db.get(Snapshot, snap_id)
            if snap is None or snap.status != "READY" or not snap.path:
                log.warning("share_snapshot_skipped", share_id=str(link.id), snapshot_id=str(snap_id))
                continue  # 02a §5.2 #10: bỏ ảnh không còn READY (MISSING / DELETED)
            path = absolute(settings, snap.path)
            try:
                data = await asyncio.to_thread(path.read_bytes)
            except FileNotFoundError:
                log.warning("share_snapshot_file_missing", share_id=str(link.id), snapshot_id=str(snap_id))
                continue
            if snap.sha256 and hashlib.sha256(data).hexdigest() != snap.sha256:
                log.warning("share_snapshot_checksum", share_id=str(link.id), snapshot_id=str(snap_id))
                continue  # ảnh gốc bất biến — lệch thì không đưa vào link
            pkey = f"{prefix}p{n}-{len(photo_keys) + 1}.jpg"
            uploaded.append(pkey)
            await _put(store, pkey, data, "image/jpeg", throttle)
            photo_keys.append(pkey)
            kept.append(snap.id)
        item.video_key, item.video_sha256, item.size_bytes = key, rendered.sha256, size
        item.source_sha256 = {c.camera_role: c.sha256 for c in sources}
        item.snapshot_ids = kept
        station = await db.get(Station, pack.station_id)
        built.append(_Built(item, pack, station.name if station else None, key, photo_keys))
        await commit(db)

    if not await _still_creating(db, link.id):
        raise _Revoked
    await _set("PUBLISHING", 95)
    expires_s = int((link.expires_at - clock.now()).total_seconds())
    if expires_s <= 0:
        raise BuildError("TIMEOUT", "link đã quá hạn trước khi công bố")
    expires_s = min(expires_s, MAX_PRESIGN_S)
    html, index_url = await _publish_page(db, link, settings, store, built, expires_s)
    index_key = f"{prefix}index.html"
    uploaded.append(index_key)
    await _put(store, index_key, html.encode(), "text/html; charset=utf-8", throttle, "no-store")
    # Công bố dưới khóa: bị thu hồi trong lúc dựng → không bao giờ thành ACTIVE (02a §6).
    locked = await db.scalar(
        select(ShareLink)
        .where(ShareLink.id == link.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked is None or locked.status != "CREATING":
        await db.rollback()
        raise _Revoked
    locked.status, locked.progress = "ACTIVE", 100
    locked.step = locked.step_index = locked.step_total = None
    locked.url_enc = Cipher(settings.fernet_key).encrypt(index_url)
    locked.object_keys = list(uploaded)
    locked.error_code = locked.error_message = None
    publish_updated(db, locked)
    await commit(db)
    return len(built)


async def _publish_page(
    db: AsyncSession,
    link: ShareLink,
    settings: Settings,
    store: ObjectStore,
    built: list[_Built],
    expires_s: int,
) -> tuple[str, str]:
    """W1 + URL ký (một lần, cùng hạn = hạn link)."""

    def _sign() -> tuple[str, list[tuple[str, str, list[str]]]]:
        index = store.presign_get(
            f"{link.object_prefix}index.html", expires_s, content_type="text/html; charset=utf-8"
        )
        media = [
            (
                store.presign_get(b.video_key, expires_s, content_type="video/mp4"),
                store.presign_get(
                    b.video_key, expires_s, filename=f"phien-{b.item.ord}.mp4", content_type="video/mp4"
                ),
                [store.presign_get(k, expires_s, content_type="image/jpeg") for k in b.photo_keys],
            )
            for b in built
        ]
        return index, media

    index_url, media = await asyncio.to_thread(_sign)
    package = await db.get(Package, link.package_id)
    order = await db.get(Order, package.order_id) if package and package.order_id else None
    shop = await db.get(Shop, order.shop_id) if order and order.shop_id else None
    ctx = w1.W1Context(
        origin=w1.origin_of(index_url),
        tracking_number=package.tracking_number if package else "—",
        platform_order_sn=order.platform_order_sn if order else None,
        platform=shop.platform if shop else None,
        expires_at=link.expires_at,
        tz=settings.tz_display,
        sessions=[
            w1.W1Session(
                number=b.item.ord,
                type=b.pack.type,
                status=b.pack.status,
                started_at=b.pack.started_at,
                ended_at=b.pack.ended_at,
                station=b.station,
                operator=b.pack.operator_name if b.pack.type == "RETURN" else None,
                conclusion=b.pack.inspection_conclusion if b.pack.type == "RETURN" else None,
                video_url=video_url,
                download_url=download_url,
                size_bytes=b.item.size_bytes or 0,
                video_sha256=b.item.video_sha256 or "",
                source_sha256=dict(b.item.source_sha256 or {}),
                photos=[w1.W1Photo(u) for u in photos],
            )
            for b, (video_url, download_url, photos) in zip(built, media, strict=True)
        ],
    )
    return w1.render(ctx), index_url


async def _delete_uploaded(store: ObjectStore | None, prefix: str, uploaded: list[str]) -> bool:
    if store is None or not uploaded:
        return True
    try:
        await asyncio.to_thread(store.delete_prefix, prefix, all_versions=True)
    except CloudError:
        log.warning("share_build_cleanup_pending", prefix_len=len(prefix))
        return False
    return True


async def _fail(
    db: AsyncSession,
    share_id: uuid.UUID,
    code: str,
    detail: str,
    store: ObjectStore | None,
    uploaded: list[str],
) -> str:
    log.warning("share_build_failed", share_id=str(share_id), code=code, detail=detail[:200])
    prefix = await db.scalar(select(ShareLink.object_prefix).where(ShareLink.id == share_id))
    if prefix is None:
        return "SKIPPED"
    await db.commit()  # G3-SH-2: không giữ transaction / khóa dòng link khi xóa qua mạng (02a §6)
    deleted = await _delete_uploaded(store, prefix, uploaded)
    link = await db.scalar(
        select(ShareLink)
        .where(ShareLink.id == share_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if link is None:
        return "SKIPPED"
    if link.status == "CREATING":
        link.status = "FAILED"
        link.error_code, link.error_message = code, MESSAGES[code]
        link.step = link.step_index = link.step_total = None
    # G3-SH-1: TIMEOUT — lời tải trong `to_thread` không hủy được, có thể ghi xong **sau** lần xóa này → để
    # J-25 xóa lại + kiểm danh sách rỗng rồi mới đặt `cloud_deleted_at` (lifecycle 8 ngày: lưới cuối).
    if deleted and code != "TIMEOUT" and link.status in ("FAILED", "REVOKED"):
        link.cloud_deleted_at = clock.now()
    publish_updated(db, link)
    await commit(db)
    return link.status


async def _abandon(
    db: AsyncSession, share_id: uuid.UUID, store: ObjectStore | None, uploaded: list[str]
) -> str:
    """Bị thu hồi giữa lúc dựng: xóa mọi đối tượng đã tải; J-25 xác nhận / dọn tiếp nếu còn."""
    link = await db.get(ShareLink, share_id, populate_existing=True)
    if link is None:
        return "SKIPPED"
    if not await _delete_uploaded(store, link.object_prefix, uploaded):
        link.cloud_deleted_at = None  # J-25 thử xóa lại mỗi phút (revoke_pending)
    await db.commit()
    return link.status
