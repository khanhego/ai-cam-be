"""T-292 (DEC-531, `test_evidence_prior_br39` (16), AC-56): API-189 `MARK_WRONG_SCAN` trả `affected_shares[]` =
link `CREATING` / `ACTIVE` (còn hạn) chứa phiên — mọi nguồn — + audit `active_shares[]`; **không** tự thu hồi.
API-164 `review_pending_count` = số phiên "Cần soát" của hồ sơ (nguồn `SESSION` → 0)."""

from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.shares.models import ShareLink

from .shares_fixtures import NOW, ShareWorld, login, make_share_world, share_api, share_settings, share_store

__all__ = ["share_api", "share_settings", "share_store"]

pytestmark = pytest.mark.integration


@pytest.fixture
async def w(db: AsyncSession, share_settings: Settings) -> ShareWorld:
    return await make_share_world(db, share_settings)


async def _share(api: AsyncClient, headers: dict[str, str], body: dict[str, object]) -> str:
    res = await api.post("/api/v1/shares", headers=headers, json=body)
    assert res.status_code == 202, res.text
    return str(res.json()["id"])


async def test_mark_wrong_scan_reports_affected_shares(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    cskh, _ = await login(share_api, db, "CSKH")
    sup, _ = await login(share_api, db, "SUPERVISOR")
    claim_body: dict[str, object] = {
        "source_type": "CLAIM", "claim_id": str(w.claim.id), "session_ids": [str(w.ret_a.id)],
        "layout": "SIDE_BY_SIDE", "include_snapshots": False, "recipient": "CSKH Shopee", "expires_days": 7,
    }  # fmt: skip
    session_body: dict[str, object] = {
        **claim_body, "source_type": "SESSION", "claim_id": None, "session_id": str(w.ret_a.id),
    }  # fmt: skip
    active1 = await _share(share_api, cskh, claim_body)
    active2 = await _share(share_api, sup, session_body)  # nguồn phiên (D4) — vẫn tính
    revoked = await _share(share_api, cskh, claim_body)
    expired = await _share(share_api, cskh, claim_body)
    other = await _share(share_api, cskh, {**claim_body, "session_ids": [str(w.ret_b.id)]})  # phiên khác
    for sid in (active1, active2, expired, other):
        link = await db.get(ShareLink, sid)
        assert link is not None
        link.status = "ACTIVE"
        if sid == expired:
            link.expires_at = NOW - timedelta(minutes=1)
    await db.flush()
    assert (await share_api.post(f"/api/v1/shares/{revoked}/revoke", headers=cskh)).status_code == 200
    await db.refresh(w.claim)
    res = await share_api.post(
        f"/api/v1/claims/{w.claim.id}/return-sessions/{w.ret_a.id}/review",
        headers=cskh,
        json={"version": w.claim.version, "action": "MARK_WRONG_SCAN", "reason_code": "WRONG_SCAN",
              "note": "Xem video: kiện của đơn khác"},
    )  # fmt: skip
    assert res.status_code == 200, res.text
    body = res.json()
    affected = {s["id"]: s for s in body["affected_shares"]}
    assert set(affected) == {active1, active2}
    assert affected[active1]["can_revoke"] is True  # CSKH tạo
    assert affected[active2]["can_revoke"] is False  # Supervisor tạo — "Nhờ Admin / Supervisor thu hồi"
    assert affected[active2]["created_by"]["display_name"] == "SUPERVISOR QA"
    assert affected[active1]["recipient"] == "CSKH Shopee"
    # Không tự thu hồi.
    for sid in (active1, active2):
        link = await db.get(ShareLink, sid, populate_existing=True)
        assert link is not None
        assert link.status == "ACTIVE"
    entry = await db.scalar(select(AuditLog).where(AuditLog.action == "SESSION_WRONG_SCAN_MARK"))
    assert entry is not None
    assert entry.data is not None
    assert sorted(entry.data["active_shares"]) == sorted([active1, active2])
    # Bỏ đánh dấu / xác nhận → `affected_shares = []`.
    res = await share_api.post(
        f"/api/v1/claims/{w.claim.id}/return-sessions/{w.ret_a.id}/review",
        headers=cskh,
        json={"version": body["version"], "action": "UNMARK_WRONG_SCAN", "note": "Đánh dấu nhầm"},
    )
    assert res.status_code == 200
    assert res.json()["affected_shares"] == []


async def test_options_review_pending_count(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.get("/api/v1/shares/options", headers=headers, params={"claim_id": str(w.claim.id)})
    assert res.json()["review_pending_count"] == 1  # phiên `review` (Supervisor hủy, chưa có lý do)
    res = await share_api.get(
        "/api/v1/shares/options", headers=headers, params={"session_id": str(w.ret_a.id)}
    )
    assert res.json()["review_pending_count"] == 0
    w.review.cancel_cause = "OTHER"  # đã có lý do → hết "Cần soát"
    await db.flush()
    res = await share_api.get("/api/v1/shares/options", headers=headers, params={"claim_id": str(w.claim.id)})
    assert res.json()["review_pending_count"] == 0
