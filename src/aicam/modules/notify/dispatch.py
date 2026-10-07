"""J-26 quét điều kiện, J-27 gom / giữ / gửi (BR-36, 02a §7.5, §6; NFR-43, FR-06.08, 06.10).

BR-36:
1. Bỏ trùng: `notify_event` unique (`code`, `dedupe_key`) — `INSERT … ON CONFLICT DO NOTHING`.
2. Gom: sự kiện × kênh `enabled` có đăng ký mã → thêm vào tin `QUEUED` (kênh, mã) tạo trong 2 phút gần nhất
   chưa gửi; không có → tạo, `send_after = occurred_at + 2 phút`. Tối đa 10 dòng + "và {n} mục khác"
   (`render`).
3. Trần 30 tin `SENT` / 60 phút / kênh: tin đến hạn khi đã đủ trần → `HELD`, `send_after` = lúc slot đầu tiên
   rảnh (tin `SENT` cũ nhất trong cửa sổ + 60 phút).
4. Giờ yên lặng: mức ≠ `HIGH` → tin `HELD` (kênh, mã), `send_after` = hết giờ yên lặng.
   Thả `HELD` đến hạn của một kênh: 1 tin → `QUEUED`; ≥ 2 tin → gộp **một** tin "Tóm tắt {n} thông báo",
   tin gốc `SKIPPED` (DEC-472).
5. Gửi lỗi → `RETRYING`, `next_attempt_at` 1, 2, 4, 8, 16, 32, 60, 60 … phút (`retry_after` của Telegram được
   tôn trọng); quá `created_at + 24 giờ` → `DROPPED`, kênh `last_status = ERROR`.

Khóa Redis `notify:scan` / `notify:dispatch` (TTL 60 giây, bận → bỏ lượt); tin khóa `FOR UPDATE SKIP LOCKED`,
đánh `SENT` **sau** khi nhà cung cấp trả OK (crash giữa chừng → gửi trùng tối đa 1 lần — RB-34).
"""

import asyncio
import secrets
import time
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import commit
from aicam.core.redis import get_redis
from aicam.core.settings import Settings
from aicam.modules.notify import catalog, providers, render
from aicam.modules.notify.conditions import Draft, collect
from aicam.modules.notify.models import NotifyChannel, NotifyEvent, NotifyMessage
from aicam.modules.notify.providers import SendError
from aicam.modules.notify.providers import zalo as zalo_provider
from aicam.modules.notify.providers.base import HTTP_TIMEOUT_S
from aicam.modules.notify.service import channel_error, in_quiet, quiet_end_after
from aicam.modules.settings import service as settings_service
from aicam.modules.stations.models import Camera

log = structlog.get_logger()

SCAN_LOCK, DISPATCH_LOCK = "notify:scan", "notify:dispatch"
LOCK_TTL_S = 60
SCAN_BUDGET_S = 20.0
DISPATCH_BUDGET_S = 10.0
GROUP_WINDOW = timedelta(minutes=2)  # BR-36 (2)
RATE_LIMIT, RATE_WINDOW = 30, timedelta(minutes=60)  # BR-36 (3)
GIVE_UP_AFTER = timedelta(hours=24)  # FR-06.10, NFR-43
BACKOFF_MIN = (1, 2, 4, 8, 16, 32, 60)
EVENT_BATCH = 500
SEND_BATCH = 50
KEEP_DAYS = 30  # J-11 dọn tin / sự kiện (FR-06.10, DEC-473)
MSG_CHANNEL_OFF = "Kênh đã tắt — tin bị bỏ."

_RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"""


async def _lock(key: str) -> str | None:
    token = secrets.token_hex(8)
    return token if await get_redis().set(key, token, nx=True, ex=LOCK_TTL_S) else None


async def _unlock(key: str, token: str) -> None:
    await get_redis().eval(_RELEASE_IF_OWNER, 1, key, token)  # type: ignore[misc]


def backoff(attempts: int) -> timedelta:
    """Lần lỗi thứ `attempts` (1, 2, …) → chờ 1, 2, 4, 8, 16, 32, 60, 60 … phút."""
    return timedelta(minutes=BACKOFF_MIN[min(attempts, len(BACKOFF_MIN)) - 1])


# ---------------------------------------------------------------- J-26


async def record_events(db: AsyncSession, drafts: list[Draft], now: datetime) -> int:
    """Ghi sự kiện mới (bỏ trùng theo `code`, `dedupe_key`); trả số sự kiện mới."""
    new = 0
    for d in drafts:
        stmt = (
            insert(NotifyEvent)
            .values(code=d.code, severity=d.severity, dedupe_key=d.dedupe_key, occurred_at=now, data=d.data)
            .on_conflict_do_nothing(index_elements=["code", "dedupe_key"])
            .returning(NotifyEvent.id)
        )
        if (await db.execute(stmt)).scalar_one_or_none() is not None:
            new += 1
            log.info("notify_event", code=d.code, dedupe_key=d.dedupe_key)
    return new


async def scan(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    """J-26 (30 giây): điều kiện N01..N09 → `notify_event`."""
    if not settings.notify_enabled:
        return {"skipped": "disabled"}
    token = await _lock(SCAN_LOCK)
    if token is None:
        return {"skipped": "locked"}
    try:
        now = clock.now()
        drafts, failed = await collect(db, settings, now)
        new = await record_events(db, drafts, now)
        await commit(db)
        return {"conditions": len(drafts), "new": new, "failed": failed}
    finally:
        await _unlock(SCAN_LOCK, token)


# ---------------------------------------------------------------- J-27 bước 1: gom


def _item(ev: NotifyEvent) -> dict[str, Any]:
    return {
        "code": ev.code,
        "severity": ev.severity,
        "event_id": str(ev.id),
        "at": clock.iso_z(ev.occurred_at),
        "data": ev.data,
    }


def _max_severity(a: str, b: str) -> str:
    return a if catalog.SEVERITY_RANK.get(a, 0) >= catalog.SEVERITY_RANK.get(b, 0) else b


def _render(msg: NotifyMessage, settings: Settings, recovered: dict[str, datetime] | None = None) -> str:
    return render.render(
        msg.event_code,
        msg.severity,
        msg.items,
        tz=settings.tz_display,
        site_address=settings.site_address,
        recovered=recovered,
    )


def _append(msg: NotifyMessage, item: dict[str, Any], settings: Settings) -> None:
    msg.items = [*msg.items, item]  # gán lại để SQLAlchemy thấy jsonb đổi
    msg.item_count = len(msg.items)
    msg.severity = _max_severity(msg.severity, item["severity"])
    msg.text = _render(msg, settings)  # bản xem trước cho API-175; dựng lại lúc gửi


async def _open_message(
    db: AsyncSession, channel_id: Any, code: str, status: str, now: datetime
) -> NotifyMessage | None:
    query = select(NotifyMessage).where(
        NotifyMessage.channel_id == channel_id,
        NotifyMessage.event_code == code,
        NotifyMessage.status == status,
    )
    if status == "QUEUED":
        query = query.where(NotifyMessage.created_at > now - GROUP_WINDOW, NotifyMessage.attempts == 0)
    return (await db.execute(query.order_by(NotifyMessage.created_at.desc()).limit(1))).scalar_one_or_none()


async def group_events(db: AsyncSession, settings: Settings, now: datetime) -> int:
    cfg = await settings_service.get(db)
    events = (
        await db.scalars(
            select(NotifyEvent)
            .where(NotifyEvent.processed_at.is_(None))
            .order_by(NotifyEvent.occurred_at, NotifyEvent.id)
            .limit(EVENT_BATCH)
            .with_for_update(skip_locked=True)
        )
    ).all()
    if not events:
        return 0
    channels = (await db.scalars(select(NotifyChannel).where(NotifyChannel.enabled.is_(True)))).all()
    quiet = in_quiet(cfg, now, settings.tz_display)
    for ev in events:
        held = quiet and ev.severity != "HIGH"
        for ch in channels:
            if ev.code not in ch.events:
                continue
            status = "HELD" if held else "QUEUED"
            msg = await _open_message(db, ch.id, ev.code, status, now)
            if msg is None:
                send_after = (
                    quiet_end_after(cfg, now, settings.tz_display) if held else ev.occurred_at + GROUP_WINDOW
                )
                msg = NotifyMessage(
                    channel_id=ch.id,
                    event_code=ev.code,
                    severity=ev.severity,
                    status=status,
                    items=[],
                    item_count=0,
                    created_at=now,
                    send_after=send_after,
                )
                db.add(msg)
            _append(msg, _item(ev), settings)
            await db.flush()
        ev.processed_at = now
    return len(events)


# ---------------------------------------------------------------- J-27 bước 3: thả HELD


async def release_held(db: AsyncSession, settings: Settings, now: datetime) -> int:
    held = (
        await db.scalars(
            select(NotifyMessage)
            .where(NotifyMessage.status == "HELD", NotifyMessage.send_after <= now)
            .order_by(NotifyMessage.channel_id, NotifyMessage.created_at)
            .with_for_update(skip_locked=True)
        )
    ).all()
    by_channel: dict[Any, list[NotifyMessage]] = {}
    for m in held:
        by_channel.setdefault(m.channel_id, []).append(m)
    for channel_id, msgs in by_channel.items():
        if len(msgs) == 1:
            msgs[0].status, msgs[0].send_after = "QUEUED", now
            continue
        items = [{**i, "summary": True} for m in msgs for i in m.items]  # đánh dấu tin tóm tắt
        lead = max(msgs, key=lambda m: (catalog.SEVERITY_RANK.get(m.severity, 0), -m.created_at.timestamp()))
        severity = lead.severity
        for m in msgs:
            severity = _max_severity(severity, m.severity)
            m.status = "SKIPPED"  # DEC-472: tin gốc vào tin tóm tắt
        summary = NotifyMessage(
            channel_id=channel_id,
            event_code=lead.event_code,
            severity=severity,
            status="QUEUED",
            items=items,
            item_count=len(items),
            created_at=now,
            send_after=now,
        )
        summary.text = _render(summary, settings)
        db.add(summary)
    await db.flush()
    return len(held)


# ---------------------------------------------------------------- J-27 bước 2, 4, 5: gửi


async def _recovered_cameras(db: AsyncSession, msg: NotifyMessage) -> dict[str, datetime]:
    """EX-N4: camera của mục N01 đã `ONLINE` lúc gửi → "(đã có lại HH:MM)" (mốc = `last_seen_at`)."""
    ids = {
        str(i.get("data", {}).get("camera_id")): i.get("data", {}).get("since")
        for i in msg.items
        if i.get("code") == "N01"
    }
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(Camera.id, Camera.status, Camera.last_seen_at).where(
                Camera.id.in_([k for k in ids if k and k != "None"])
            )
        )
    ).all()
    out: dict[str, datetime] = {}
    for cid, status, seen in rows:
        since = ids.get(str(cid))
        if status == "ONLINE" and seen is not None and (since is None or clock.iso_z(seen) > since):
            out[str(cid)] = seen
    return out


async def _due_ids(db: AsyncSession, now: datetime) -> list[Any]:
    return list(
        (
            await db.scalars(
                select(NotifyMessage.id)
                .where(
                    or_(
                        (NotifyMessage.status == "QUEUED") & (NotifyMessage.send_after <= now),
                        (NotifyMessage.status == "RETRYING") & (NotifyMessage.next_attempt_at <= now),
                    )
                )
                .order_by(NotifyMessage.send_after, NotifyMessage.created_at)
                .limit(SEND_BATCH)
            )
        ).all()
    )


async def _sent_in_window(db: AsyncSession, channel_id: Any, now: datetime) -> tuple[int, datetime | None]:
    count, first = (
        await db.execute(
            select(func.count(), func.min(NotifyMessage.sent_at)).where(
                NotifyMessage.channel_id == channel_id,
                NotifyMessage.status == "SENT",
                NotifyMessage.sent_at > now - RATE_WINDOW,
            )
        )
    ).one()
    return int(count or 0), first


async def send_one(db: AsyncSession, settings: Settings, message_id: Any) -> str:
    """Gửi một tin đến hạn; trả trạng thái sau lượt (hoặc `skip` khi tin đang bị khóa / không còn đến hạn)."""
    now = clock.now()
    msg = (
        await db.execute(
            select(NotifyMessage)
            .where(NotifyMessage.id == message_id, NotifyMessage.status.in_(("QUEUED", "RETRYING")))
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if msg is None:
        await db.rollback()
        return "skip"
    ch = await db.get(NotifyChannel, msg.channel_id, with_for_update=True, populate_existing=True)
    if ch is None or not ch.enabled:
        msg.status, msg.last_error = "DROPPED", MSG_CHANNEL_OFF
        await commit(db)
        log.info(
            "notify_send",
            message_id=str(msg.id),
            channel_id=str(msg.channel_id),
            status="DROPPED",
            attempts=msg.attempts,
            provider_code="CHANNEL_DISABLED",
        )
        return "DROPPED"
    sent, first = await _sent_in_window(db, ch.id, now)
    if sent >= RATE_LIMIT and first is not None:  # BR-36 (3)
        msg.status, msg.send_after = "HELD", first + RATE_WINDOW
        await commit(db)
        log.info("notify_rate_held", channel_id=str(ch.id), message_id=str(msg.id))
        return "HELD"
    msg.text = _render(msg, settings, await _recovered_cameras(db, msg))
    provider = providers.get_provider(ch.type, settings, db)
    error: SendError | None = None
    try:
        async with asyncio.timeout(HTTP_TIMEOUT_S + 2):
            await provider.send(ch.target, msg.text)
    except TimeoutError:
        error = SendError("Quá thời gian gửi.", provider_code="TIMEOUT", timeout=True)
    except SendError as exc:
        error = exc
    now = clock.now()
    msg.attempts += 1
    if error is None:
        msg.status, msg.sent_at, msg.next_attempt_at, msg.last_error = "SENT", now, None, None
        ch.last_status, ch.last_sent_at, ch.last_error = "OK", now, None
    else:
        msg.last_error = error.message
        ch.last_status = "ERROR"
        code = "NOTIFY_TIMEOUT" if error.timeout else "NOTIFY_SEND_FAILED"
        ch.last_error = channel_error(code, error.message, error.provider_code)
        wait = backoff(msg.attempts)
        if error.retry_after_s is not None:
            wait = max(wait, timedelta(seconds=error.retry_after_s))
        give_up_at = msg.created_at + GIVE_UP_AFTER
        if now >= give_up_at:  # đã thử tới mốc 24 giờ kể từ khi tạo tin → bỏ (FR-06.10, EX-N2)
            msg.status, msg.next_attempt_at = "DROPPED", None
        else:  # lần thử cuối rơi đúng mốc 24 giờ
            msg.status, msg.next_attempt_at = "RETRYING", min(now + wait, give_up_at)
    await commit(db)
    log.info(
        "notify_send",
        message_id=str(msg.id),
        channel_id=str(ch.id),
        status=msg.status,
        attempts=msg.attempts,
        provider_code=error.provider_code if error else None,
    )
    if msg.status == "DROPPED":
        log.warning("notify_dropped", message_id=str(msg.id), channel_id=str(ch.id))
    return msg.status


async def dispatch(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    """J-27 (15 giây): gom → thả HELD → gửi tin đến hạn trong ngân sách 10 giây."""
    if not settings.notify_enabled:
        return {"skipped": "disabled"}
    token = await _lock(DISPATCH_LOCK)
    if token is None:
        return {"skipped": "locked"}
    out: dict[str, Any] = {"grouped": 0, "released": 0}
    try:
        now = clock.now()
        out["grouped"] = await group_events(db, settings, now)
        out["released"] = await release_held(db, settings, now)
        await commit(db)
        deadline = time.monotonic() + DISPATCH_BUDGET_S
        for message_id in await _due_ids(db, now):
            if time.monotonic() >= deadline:
                break
            status = await send_one(db, settings, message_id)
            out[status] = out.get(status, 0) + 1
        return out
    finally:
        # G3-NT-1: lượt làm mới token Zalo bị che khỏi hủy phải xong trước khi `asyncio.run` của worker đóng.
        await zalo_provider.drain_pending()
        await _unlock(DISPATCH_LOCK, token)


# ---------------------------------------------------------------- J-11 dọn


async def purge(db: AsyncSession, now: datetime | None = None) -> dict[str, int]:
    """Tin > 30 ngày (nhật ký FR-06.10) và sự kiện đã xử lý > 30 ngày (DEC-473)."""
    cutoff = (now or clock.now()) - timedelta(days=KEEP_DAYS)
    msgs = await db.execute(delete(NotifyMessage).where(NotifyMessage.created_at < cutoff))
    events = await db.execute(
        delete(NotifyEvent).where(NotifyEvent.processed_at.is_not(None), NotifyEvent.occurred_at < cutoff)
    )
    return {"notify_messages": int(msgs.rowcount or 0), "notify_events": int(events.rowcount or 0)}  # type: ignore[attr-defined]
