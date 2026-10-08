"""J-28 — tóm tắt ngày 18:00 giờ VN (N10, FR-06.11, AC-55): số liệu như D2 hôm nay (`reports.service._counts`)
→ `notify_event` `summary:{yyyy-mm-dd}` (một lần / ngày); J-27 gửi tới kênh chọn N10."""

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import commit
from aicam.core.settings import Settings
from aicam.modules.notify.conditions import Draft
from aicam.modules.notify.dispatch import record_events
from aicam.modules.reports import service as reports

N10_FIELDS = (
    "packed",
    "had_mismatch",
    "returns_received",
    "returns_received_issue",
    "claims_open",
    "claims_due_soon",
    "claims_overdue_unsent",
    "refund_only_pending",
)


async def draft(db: AsyncSession, settings: Settings, now: datetime) -> Draft:
    day = now.astimezone(ZoneInfo(settings.tz_display)).date()
    start, end = reports._bounds(day, settings.tz_display)
    counts = (await reports._counts(db, start, end)).model_dump()
    data: dict[str, Any] = {k: counts[k] for k in N10_FIELDS}
    data["day_start"] = clock.iso_z(start)
    return Draft("N10", "INFO", f"summary:{day.isoformat()}", data)


async def daily_summary(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    """J-28 (crontab UTC 11:00 = 18:00 VN)."""
    if not settings.notify_enabled:
        return {"skipped": "disabled"}
    now = clock.now()
    d = await draft(db, settings, now)
    new = await record_events(db, [d], now)
    await commit(db)
    return {"new": new, "dedupe_key": d.dedupe_key}
