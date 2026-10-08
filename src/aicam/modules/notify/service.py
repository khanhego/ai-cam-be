"""API-170..176 — kênh thông báo, gửi thử, nhật ký, giờ yên lặng (FR-06.04, 06.07, 06.08, 06.10; 02a §4).

Chỉ ADMIN (kiểm ở router). Bot token / khóa OA không bao giờ trả API / ghi log (02 §6 "Bí mật"). Gửi thử chạy
**ngoài** transaction đọc (02a API-174) rồi mới ghi kết quả + audit.
"""

import asyncio
import re
import uuid
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.notify import catalog, providers
from aicam.modules.notify.models import NotifyChannel, NotifyMessage
from aicam.modules.notify.providers import SendError
from aicam.modules.notify.schemas import (
    ChannelBrief,
    ChannelCreateIn,
    ChannelError,
    ChannelOut,
    ChannelsOut,
    ChannelUpdateIn,
    EventInfo,
    MessageOut,
    MessagePage,
    Providers,
    ProviderState,
    QuietHours,
    TestSendOut,
)
from aicam.modules.settings import service as settings_service
from aicam.modules.settings.models import Setting

log = structlog.get_logger()

NAME_MIN, NAME_MAX = 2, 40
TELEGRAM_TARGET = re.compile(r"^-?\d{1,20}$")
ZALO_TARGET = re.compile(r"^\d{1,64}$")
HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
TEST_TIMEOUT_S = 10.0  # FR-06.10 "kết quả ≤ 10 giây"
LOG_DAYS = 30  # FR-06.10 nhật ký 30 ngày
PENDING_STATUSES = ("QUEUED", "HELD", "RETRYING")
SUMMARY_LABEL = "Tóm tắt thông báo"

MSG_NAME = f"Tên kênh {NAME_MIN}–{NAME_MAX} ký tự."
MSG_TARGET = {
    "TELEGRAM": "Chat ID là một số (nhóm thường bắt đầu bằng -100).",
    "ZALO_OA": "Zalo user ID là dãy 1–64 chữ số.",
}
MSG_EVENTS = "Chọn ít nhất 1 sự kiện."
MSG_EVENTS_UNKNOWN = "Sự kiện không hợp lệ."
MSG_NAME_TAKEN = "Đã có kênh tên này."
MSG_TIME = "Nhập giờ dạng HH:MM."
MSG_TIME_SAME = "Giờ bắt đầu và kết thúc phải khác nhau."
MSG_PROVIDER = {
    "TELEGRAM": "Chưa cấu hình bot Telegram trên máy chủ. Liên hệ IT.",
    "ZALO_OA": "Chưa cấu hình Zalo OA trên máy chủ. Liên hệ IT.",
}
MSG_TIMEOUT = {
    "TELEGRAM": "Không kết nối được Telegram từ máy chủ (mạng chặn?).",
    "ZALO_OA": "Không kết nối được Zalo từ máy chủ (mạng chặn?).",
}


def _validation(fields: dict[str, str]) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})


def _not_found() -> AppError:
    return AppError("NOT_FOUND", "Không tìm thấy kênh thông báo.", 404)


# ---------------------------------------------------------------- giờ yên lặng (BR-36 (4), DEC-444)


def hhmm(t: time) -> str:
    return t.strftime("%H:%M")


def quiet_hours(cfg: Setting) -> QuietHours:
    return QuietHours(enabled=cfg.quiet_hours_enabled, start=hhmm(cfg.quiet_start), end=hhmm(cfg.quiet_end))


def in_quiet(cfg: Setting, at: datetime, tz: str) -> bool:
    """`at` nằm trong giờ yên lặng (giờ VN). Khoảng qua nửa đêm (22:00–07:00) xét hai phía."""
    if not cfg.quiet_hours_enabled:
        return False
    local = at.astimezone(ZoneInfo(tz)).time().replace(tzinfo=None)
    start, end = cfg.quiet_start, cfg.quiet_end
    if start < end:
        return start <= local < end
    return local >= start or local < end


def quiet_end_after(cfg: Setting, at: datetime, tz: str) -> datetime:
    """Mốc kết thúc giờ yên lặng kế tiếp sau `at` (UTC) — lúc thả tin `HELD`."""
    zone = ZoneInfo(tz)
    local = at.astimezone(zone)
    candidate = datetime.combine(local.date(), cfg.quiet_end, tzinfo=zone)
    if candidate <= local:
        candidate += timedelta(days=1)
    return candidate.astimezone(at.tzinfo or zone)


# ---------------------------------------------------------------- chuyển đổi


def channel_out(ch: NotifyChannel) -> ChannelOut:
    err = ch.last_error
    return ChannelOut(
        id=ch.id,
        name=ch.name,
        type=ch.type,
        target=ch.target,
        events=[e for e in catalog.CODES if e in ch.events],
        enabled=ch.enabled,
        last_status=ch.last_status,
        last_sent_at=ch.last_sent_at,
        last_error=ChannelError.model_validate(err) if err else None,
        created_at=ch.created_at,
    )


def channel_error(code: str, message: str, provider_code: str | None) -> dict[str, Any]:
    """`notify_channel.last_error` jsonb `{code, message, at, provider_code}` (D22 "Lỗi gửi lúc 09:30: …")."""
    return {"code": code, "message": message, "at": clock.iso_z(clock.now()), "provider_code": provider_code}


def message_label(msg: NotifyMessage) -> str:
    """Tin tóm tắt (gộp nhiều mã sự kiện khi thả `HELD` — DEC-472) có nhãn riêng."""
    items = [i for i in msg.items if isinstance(i, dict)]
    if len({i.get("code") for i in items}) > 1 or any(i.get("summary") for i in items):
        return SUMMARY_LABEL
    return catalog.label(msg.event_code)


# ---------------------------------------------------------------- kiểm dữ liệu


def _clean_name(raw: str | None, fields: dict[str, str]) -> str | None:
    if raw is None:
        return None
    name = " ".join(raw.split())
    if not NAME_MIN <= len(name) <= NAME_MAX:
        fields["name"] = MSG_NAME
    return name


def _clean_target(raw: str | None, channel_type: str, fields: dict[str, str]) -> str | None:
    if raw is None:
        return None
    target = raw.strip()
    pattern = TELEGRAM_TARGET if channel_type == "TELEGRAM" else ZALO_TARGET
    if not pattern.fullmatch(target):
        fields["target"] = MSG_TARGET.get(channel_type, MSG_TARGET["TELEGRAM"])
    return target


def _clean_events(raw: list[str] | None, fields: dict[str, str]) -> list[str] | None:
    if raw is None:
        return None
    if not raw:
        fields["events"] = MSG_EVENTS
        return []
    if any(code not in catalog.BY_CODE for code in raw):
        fields["events"] = MSG_EVENTS_UNKNOWN
        return []
    return [code for code in catalog.CODES if code in set(raw)]  # bỏ trùng, theo thứ tự danh mục


def _require_configured(channel_type: str, settings: Settings) -> None:
    if not providers.configured(channel_type, settings):
        raise AppError(
            "PROVIDER_NOT_CONFIGURED", MSG_PROVIDER.get(channel_type, "Loại kênh chưa cấu hình."), 409,
            {"type": channel_type},
        )  # fmt: skip


async def _flush_or_name_taken(db: AsyncSession) -> None:
    """Hai Admin cùng thêm / đổi tên trùng: unique `lower(name)` chặn trong savepoint → 409 (02a §6)."""
    try:
        async with db.begin_nested():
            await db.flush()
    except IntegrityError as exc:
        raise AppError(
            "CHANNEL_NAME_EXISTS", MSG_NAME_TAKEN, 409, {"fields": {"name": MSG_NAME_TAKEN}}
        ) from exc


async def _name_taken(db: AsyncSession, name: str, exclude: uuid.UUID | None = None) -> bool:
    query = select(NotifyChannel.id).where(func.lower(NotifyChannel.name) == name.lower())
    if exclude is not None:
        query = query.where(NotifyChannel.id != exclude)
    return (await db.scalar(query)) is not None


async def _get_channel(db: AsyncSession, channel_id: uuid.UUID, *, lock: bool = False) -> NotifyChannel:
    query = (
        select(NotifyChannel).where(NotifyChannel.id == channel_id).execution_options(populate_existing=True)
    )
    if lock:
        query = query.with_for_update()
    ch = (await db.execute(query)).scalar_one_or_none()
    if ch is None:
        raise _not_found()
    return ch


def _audit_data(ch: NotifyChannel) -> dict[str, Any]:
    return {"name": ch.name, "type": ch.type, "events": list(ch.events), "enabled": ch.enabled}


# ---------------------------------------------------------------- API-170..173


async def list_channels(db: AsyncSession, settings: Settings) -> ChannelsOut:
    """API-170: kênh + trạng thái nhà cung cấp + giờ yên lặng + danh mục sự kiện."""
    cfg = await settings_service.get(db)
    rows = (
        await db.scalars(select(NotifyChannel).order_by(NotifyChannel.created_at, NotifyChannel.id))
    ).all()
    return ChannelsOut(
        providers=Providers(
            TELEGRAM=ProviderState(configured=providers.configured("TELEGRAM", settings)),
            ZALO_OA=ProviderState(configured=providers.configured("ZALO_OA", settings)),
        ),
        quiet_hours=quiet_hours(cfg),
        events=[
            EventInfo(code=e.code, label=e.label, severity=e.severity, suggested_channel=e.suggested_channel)
            for e in catalog.EVENTS
        ],
        items=[channel_out(ch) for ch in rows],
    )


async def create_channel(
    db: AsyncSession, body: ChannelCreateIn, p: Principal, settings: Settings
) -> ChannelOut:
    """API-171 (FR-06.04)."""
    fields: dict[str, str] = {}
    name = _clean_name(body.name, fields)
    target = _clean_target(body.target, body.type, fields)
    events = _clean_events(body.events, fields)
    if fields:
        raise _validation(fields)
    _require_configured(body.type, settings)
    if name is None or target is None or events is None:  # không xảy ra — trường bắt buộc của API-171
        raise _validation({"name": MSG_NAME})
    if await _name_taken(db, name):
        raise AppError("CHANNEL_NAME_EXISTS", MSG_NAME_TAKEN, 409, {"fields": {"name": MSG_NAME_TAKEN}})
    ch = NotifyChannel(
        name=name, type=body.type, target=target, events=events, enabled=body.enabled, created_by=p.user_id
    )
    db.add(ch)
    await _flush_or_name_taken(db)
    audit.record(db, "NOTIFY_CHANNEL_CREATE", user_id=p.user_id, object_type="notify_channel",
                 object_id=ch.id, ip=p.ip, data=_audit_data(ch))  # fmt: skip
    await commit(db)
    log.info("notify_channel_create", channel_id=str(ch.id), type=ch.type, events=len(events))
    return channel_out(ch)


async def update_channel(
    db: AsyncSession, channel_id: uuid.UUID, body: ChannelUpdateIn, p: Principal, settings: Settings
) -> ChannelOut:
    """API-172 (FR-06.04, 06.07): sửa từng trường; đổi loại → kiểm lại Chat ID / user ID theo loại mới."""
    ch = await _get_channel(db, channel_id, lock=True)
    before = _audit_data(ch)
    fields: dict[str, str] = {}
    new_type = body.type or ch.type
    name = _clean_name(body.name, fields)
    target_raw = body.target if body.target is not None else (ch.target if body.type is not None else None)
    target = _clean_target(target_raw, new_type, fields)
    events = _clean_events(body.events, fields)
    if fields:
        raise _validation(fields)
    if body.type is not None and body.type != ch.type:
        _require_configured(body.type, settings)
    if name is not None and name != ch.name:
        if await _name_taken(db, name, exclude=ch.id):
            raise AppError("CHANNEL_NAME_EXISTS", MSG_NAME_TAKEN, 409, {"fields": {"name": MSG_NAME_TAKEN}})
        ch.name = name
    if body.type is not None:
        ch.type = body.type
    if target is not None:
        ch.target = target
    if events is not None:
        ch.events = events
    if body.enabled is not None:
        ch.enabled = body.enabled
    ch.updated_at = clock.now()
    await _flush_or_name_taken(db)
    audit.record(db, "NOTIFY_CHANNEL_UPDATE", user_id=p.user_id, object_type="notify_channel",
                 object_id=ch.id, ip=p.ip, data={"before": before, "after": _audit_data(ch)})  # fmt: skip
    await commit(db)
    return channel_out(ch)


async def delete_channel(db: AsyncSession, channel_id: uuid.UUID, p: Principal) -> None:
    """API-173: tin chờ của kênh (`QUEUED` / `HELD` / `RETRYING`) bị bỏ — dòng tin xóa theo kênh (FK CASCADE,
    02a §3); số tin bị bỏ ghi vào audit + log (DEC-731)."""
    ch = await _get_channel(db, channel_id, lock=True)
    dropped = int(
        await db.scalar(
            select(func.count())
            .select_from(NotifyMessage)
            .where(NotifyMessage.channel_id == ch.id, NotifyMessage.status.in_(PENDING_STATUSES))
        )
        or 0
    )
    audit.record(db, "NOTIFY_CHANNEL_DELETE", user_id=p.user_id, object_type="notify_channel",
                 object_id=ch.id, ip=p.ip, data={**_audit_data(ch), "dropped_messages": dropped})  # fmt: skip
    await db.delete(ch)
    await commit(db)
    log.info("notify_channel_delete", channel_id=str(channel_id), dropped=dropped)


# ---------------------------------------------------------------- API-174 gửi thử


def test_text(ch: NotifyChannel) -> str:
    labels = ", ".join(catalog.label(c) for c in catalog.CODES if c in ch.events)
    return f"Tin thử từ Hệ thống X — kênh {ch.name}. Bạn sẽ nhận: {labels}."


async def test_send(db: AsyncSession, channel_id: uuid.UUID, p: Principal, settings: Settings) -> TestSendOut:
    """API-174 (FR-06.10): gửi tin thử ≤ 10 giây; cập nhật `last_status` / `last_error`; audit
    `NOTIFY_TEST`."""
    ch = await _get_channel(db, channel_id)
    _require_configured(ch.type, settings)
    channel_type, target, text = ch.type, ch.target, test_text(ch)
    await db.commit()  # không giữ transaction trong lúc gọi mạng (02a API-174)
    provider = providers.get_provider(channel_type, settings, db)
    error: SendError | None = None
    try:
        async with asyncio.timeout(TEST_TIMEOUT_S):
            await provider.send(target, text)
    except TimeoutError:
        error = SendError(
            MSG_TIMEOUT.get(channel_type, "Quá thời gian gửi."), provider_code="TIMEOUT", timeout=True
        )
    except SendError as exc:
        error = exc
    except Exception as exc:  # G3-NT-3: lỗi lạ của nhà cung cấp → lỗi gửi có mã (không 500 / không kẹt tin)
        log.exception("notify_provider_crashed", channel_type=channel_type)
        error = SendError("Lỗi không rõ khi gửi.", provider_code=type(exc).__name__)
    now = clock.now()
    ch = await _get_channel(db, channel_id, lock=True)  # kênh có thể vừa bị xóa → 404
    code = None if error is None else ("NOTIFY_TIMEOUT" if error.timeout else "NOTIFY_SEND_FAILED")
    if error is None:
        ch.last_status, ch.last_sent_at, ch.last_error = "OK", now, None
    else:
        ch.last_status = "ERROR"
        ch.last_error = channel_error(code or "NOTIFY_SEND_FAILED", error.message, error.provider_code)
    audit.record(db, "NOTIFY_TEST", user_id=p.user_id, object_type="notify_channel", object_id=ch.id, ip=p.ip,
                 data={"ok": error is None, "error_code": code,
                       "provider_code": error.provider_code if error else None})  # fmt: skip
    await commit(db)
    log.info(
        "notify_test", channel_id=str(ch.id), ok=error is None, provider_code=error and error.provider_code
    )
    if error is not None:
        status = 504 if error.timeout else 502
        raise AppError(
            code or "NOTIFY_SEND_FAILED", error.message, status, {"provider_code": error.provider_code}
        )
    return TestSendOut(ok=True, sent_at=now)


# ---------------------------------------------------------------- API-175 nhật ký


async def list_messages(
    db: AsyncSession,
    *,
    channel_id: uuid.UUID | None,
    status: str | None,
    page: int,
    page_size: int,
) -> MessagePage:
    """API-175 (FR-06.10): 30 ngày, mới nhất trước."""
    where = [NotifyMessage.created_at >= clock.now() - timedelta(days=LOG_DAYS)]
    if channel_id is not None:
        where.append(NotifyMessage.channel_id == channel_id)
    if status is not None:
        where.append(NotifyMessage.status == status)
    total = int(await db.scalar(select(func.count()).select_from(NotifyMessage).where(*where)) or 0)
    rows = (
        await db.execute(
            select(NotifyMessage, NotifyChannel.name)
            .join(NotifyChannel, NotifyChannel.id == NotifyMessage.channel_id)
            .where(*where)
            .order_by(NotifyMessage.created_at.desc(), NotifyMessage.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    return MessagePage(
        items=[
            MessageOut(
                id=m.id,
                channel=ChannelBrief(id=m.channel_id, name=name),
                event_code=m.event_code,
                event_label=message_label(m),
                item_count=m.item_count,
                text=m.text,
                status=m.status,
                attempts=m.attempts,
                last_error=m.last_error,
                created_at=m.created_at,
                sent_at=m.sent_at,
                next_attempt_at=m.next_attempt_at,
            )
            for m, name in rows
        ],
        page=page,
        page_size=page_size,
        total=total,
    )


# ---------------------------------------------------------------- API-176 giờ yên lặng


def _parse_hhmm(raw: str, field: str, fields: dict[str, str]) -> time | None:
    value = raw.strip()
    if not HHMM.fullmatch(value):
        fields[field] = MSG_TIME
        return None
    hour, minute = value.split(":")
    return time(int(hour), int(minute))


async def update_quiet_hours(db: AsyncSession, body: QuietHours, p: Principal) -> QuietHours:
    """API-176 (FR-06.08, BR-36 (4)): `HH:MM` giờ VN, bắt đầu ≠ kết thúc; audit `NOTIFY_SETTINGS_UPDATE`."""
    fields: dict[str, str] = {}
    start = _parse_hhmm(body.start, "start", fields)
    end = _parse_hhmm(body.end, "end", fields)
    if start is not None and end is not None and start == end:
        fields["end"] = MSG_TIME_SAME
    if fields:
        raise _validation(fields)
    if start is None or end is None:  # không xảy ra — đã kiểm ở trên
        raise _validation({"start": MSG_TIME})
    cfg = await settings_service.get(db)
    before = quiet_hours(cfg)
    cfg.quiet_hours_enabled, cfg.quiet_start, cfg.quiet_end = body.enabled, start, end
    after = quiet_hours(cfg)
    audit.record(db, "NOTIFY_SETTINGS_UPDATE", user_id=p.user_id, object_type="setting", object_id="1",
                 ip=p.ip,
                 data={"before": before.model_dump(), "after": after.model_dump()})  # fmt: skip
    await commit(db)
    return after
