"""API-120, API-121, API-123 — 02 §6.2 (API-122 ở `orders/router.py`)."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.reconciliation import service
from aicam.modules.reconciliation.schemas import (
    AlertStatus,
    ReconAlertOut,
    ReconAlertPage,
    ResolveIn,
    Rule,
    RunOut,
    Severity,
)

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]
Lead = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]

router = APIRouter(tags=["reconciliation"])


@router.get("/recon-alerts", response_model=ReconAlertPage)
async def list_alerts(
    _: Staff,
    db: DbSession,
    settings: AppSettings,
    status: AlertStatus | None = None,
    severity: Severity | None = None,
    rule: Rule | None = None,
    package_id: uuid.UUID | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ReconAlertPage:
    """API-120: danh sách cảnh báo lệch + tổng mở theo mức (FR-06.03, D15)."""
    return await service.list_alerts(
        db, tz=settings.tz_display, status=status, severity=severity, rule=rule, package_id=package_id,
        date_from=date_from, date_to=date_to, page=page, page_size=page_size,
    )  # fmt: skip


@router.post("/recon-alerts/{alert_id}/resolve", response_model=ReconAlertOut)
async def resolve_alert(
    alert_id: uuid.UUID, body: ResolveIn, p: Lead, db: DbSession, settings: AppSettings
) -> ReconAlertOut:
    """API-121: đánh dấu đã xử lý (FR-06.03)."""
    return await service.resolve(db, alert_id, body.note, actor=p.user_id, ip=p.ip, tz=settings.tz_display)


@router.post("/recon/run", response_model=RunOut, status_code=202)
async def run_now(_: Lead) -> RunOut:
    """API-123: chạy đối soát ngay (FR-06.02) — chỉ đẩy J-14; J-14 đang chạy → 409 `RECON_IN_PROGRESS`."""
    await service.request_run()
    return RunOut(queued=True)
