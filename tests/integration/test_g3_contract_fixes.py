"""G3 Phase 2 rà contract: C-01 quyền giữ clip, C-04 `CLAIM_EXISTS` kèm details ở nhánh unique, C-05 lỗi
độ dài trả `details.fields` tiếng Việt (không phải lỗi Pydantic)."""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.modules.claims import service as claims
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.orders.models import Package
from aicam.modules.users.permissions import PERMISSIONS

from .factories import PASSWORD, make_user
from .returns_helpers import make_desk

pytestmark = pytest.mark.integration


def test_clip_hold_admin_only() -> None:
    """C-01: API-42 chỉ ADMIN."""
    assert "clips.hold" in PERMISSIONS["ADMIN"]
    assert "clips.hold" not in PERMISSIONS["SUPERVISOR"]
    assert "clips.hold" not in PERMISSIONS["CSKH"]


async def test_claim_exists_from_unique_carries_details(
    db: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C-04: lưới unique BR-27 (advisory lock bị vượt) vẫn trả `details` {claim_id, code}."""
    user = await make_user(db, "tst_c04_cskh", "CSKH")
    package = Package(tracking_number="SPXTSTC0400001", warehouse_status="DELIVERED")
    db.add(package)
    await db.flush()
    p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)
    body = ClaimCreateIn(package_id=package.id, type="BUYER_CLAIM", counterparty="PLATFORM")
    first = await claims.create_manual(db, body, p)

    original = claims.find_open
    calls = {"n": 0}

    async def miss_once(*args: Any, **kw: Any) -> Any:
        calls["n"] += 1
        return None if calls["n"] == 1 else await original(*args, **kw)

    monkeypatch.setattr(claims, "find_open", miss_once)
    with pytest.raises(AppError) as exc:
        await claims.create_manual(db, body, p)
    assert exc.value.code == "CLAIM_EXISTS"
    assert exc.value.details == {"claim_id": str(first.id), "code": first.code}


async def _headers(api: AsyncClient, db: AsyncSession, name: str, role: str) -> dict[str, str]:
    user = await make_user(db, name, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def test_length_errors_are_field_messages(api: AsyncClient, db: AsyncSession) -> None:
    """C-05: quá dài / quá ngắn → 422 `VALIDATION_ERROR` với `details.fields` tiếng Việt."""
    admin = await _headers(api, db, "tst_c05_admin", "ADMIN")
    package = Package(tracking_number="SPXTSTC0500001", warehouse_status="DELIVERED")
    db.add(package)
    await db.flush()

    res = await api.post(f"/api/v1/packages/{package.id}/warehouse-status", headers=admin,
                         json={"to_status": "HANDED_OVER", "reason": "x" * 501})  # fmt: skip
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {"reason": "Nhập lý do 5–500 ký tự"}
    res = await api.post(f"/api/v1/packages/{package.id}/warehouse-status", headers=admin,
                         json={"to_status": "HANDED_OVER", "reason": "abc"})  # fmt: skip
    assert res.json()["error"]["details"]["fields"] == {"reason": "Nhập lý do 5–500 ký tự"}

    body = {"package_id": str(package.id), "type": "OTHER", "counterparty": "CARRIER"}
    created = await api.post("/api/v1/claims", headers=admin, json=body)
    claim = created.json()
    res = await api.patch(f"/api/v1/claims/{claim['id']}", headers=admin,
                          json={"version": claim["version"], "platform_claim_ref": "R" * 65})  # fmt: skip
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {
        "platform_claim_ref": "Mã khiếu nại bên sàn tối đa 64 ký tự"
    }

    res = await api.post(
        f"/api/v1/recon-alerts/{uuid.uuid4()}/resolve", headers=admin, json={"note": "n" * 501}
    )
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {"note": "Nhập ghi chú 1–500 ký tự"}

    desk = await make_desk(api, db)
    res = await api.put("/api/v1/station/operator", headers=desk.headers, json={"name": "N" * 201})
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {"name": "Nhập tên người kiểm 2–40 ký tự"}
