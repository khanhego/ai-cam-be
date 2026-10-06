"""BR-37 / FR-04.14 (L11, T-213): station tự hủy phiên mở hoàn chỉ trong 60 giây đầu, chưa lưu kết luận, chưa
chụp ảnh tay (API-12 409 `CANCEL_REQUIRES_SUPERVISOR`, API-10 `self_cancel_until`); API-20 `return_summary`;
API-21 hủy phiên RETURN bắt ghi chú 5–500 (DEC-447)."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.media.models import Snapshot
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_user
from .returns_helpers import Desk, buyer_return_case, make_desk, make_order

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 6, 2, 0, tzinfo=UTC)  # 09:00 giờ VN


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    clock.freeze(T0)
    return await make_desk(api, db)


async def _open_return(desk: Desk, db: AsyncSession, n: int = 41) -> dict[str, Any]:
    order, _ = await make_order(db, n)
    await buyer_return_case(db, order, n)
    body = (await desk.scan(f"SPXRTTST{n:06d}")).json()
    assert body["outcome"] == "SESSION_OPENED", body
    return body["state"]["session"]  # type: ignore[no-any-return]


async def _cancel(desk: Desk, session_id: str, reason: str = "WRONG_SCAN") -> Response:
    return await desk.api.post(
        f"/api/v1/station/sessions/{session_id}/cancel", headers=desk.headers, json={"reason": reason}
    )


def _snapshot(db: AsyncSession, session_id: str, at: datetime, status: str) -> None:
    db.add(
        Snapshot(
            session_id=session_id, kind="MANUAL", camera_role="CAM1", taken_at=at,
            path=f"snapshots/{at.timestamp()}.jpg", sha256="ab" * 32, size_bytes=10, status=status,
        )
    )  # fmt: skip


def _until(session: dict[str, Any]) -> datetime | None:
    raw = session.get("self_cancel_until")
    return datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else None


@pytest.mark.parametrize("after_s", [45, 60])
async def test_self_cancel_within_60_seconds(desk: Desk, db: AsyncSession, after_s: int) -> None:
    """BR-37 ví dụ: mở 09:00:00, 09:00:45 hủy được; đúng giây 60 vẫn được (≤ 60)."""
    session = await _open_return(desk, db)
    assert _until(session) == T0 + timedelta(seconds=60)

    clock.advance(timedelta(seconds=after_s))
    res = await _cancel(desk, session["id"])

    assert res.status_code == 200, res.text
    assert res.json()["state"]["state"] == "READY"


async def test_cancel_after_60_seconds_requires_supervisor(desk: Desk, db: AsyncSession) -> None:
    """09:01:01 → 409 `CANCEL_REQUIRES_SUPERVISOR`, phiên vẫn mở; API-10 vẫn trả mốc (FE so `server_time`)."""
    session = await _open_return(desk, db)
    clock.advance(timedelta(seconds=61))

    res = await _cancel(desk, session["id"], "NOT_A_RETURN")

    assert res.status_code == 409
    error = res.json()["error"]
    assert error["code"] == "CANCEL_REQUIRES_SUPERVISOR"
    assert error["details"] == {"reason": "TIME_EXCEEDED"}
    assert error["message"] == "Phiên đã quá 60 giây. Bấm Gọi quản lý để hủy."
    state = await desk.state()
    assert state["session"]["status"] == "OPEN"
    assert _until(state["session"]) == T0 + timedelta(seconds=60)


async def test_cancel_after_snapshot_requires_supervisor(desk: Desk, db: AsyncSession) -> None:
    """09:00:30 đã chụp 1 ảnh → `self_cancel_until = null`, API-12 409 (kể cả ảnh đã bị xóa)."""
    session = await _open_return(desk, db)
    _snapshot(db, session["id"], T0 + timedelta(seconds=30), "DELETED")
    await db.flush()
    clock.advance(timedelta(seconds=31))

    assert (await desk.state())["session"]["self_cancel_until"] is None
    res = await _cancel(desk, session["id"])
    assert (res.status_code, res.json()["error"]["details"]) == (409, {"reason": "SNAPSHOT_TAKEN"})


async def test_cancel_after_inspection_saved_requires_supervisor(desk: Desk, db: AsyncSession) -> None:
    session = await _open_return(desk, db)
    lines = [
        {"order_item_id": line["order_item_id"], "quantity_received": line["quantity_received"],
         "condition": line["condition"], "note": None}
        for line in session["inspection"]["lines"]
    ]  # fmt: skip
    saved = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection",
        headers=desk.headers,
        json={"conclusion": "OK", "note": "", "lines": lines},
    )
    assert saved.status_code == 200, saved.text
    clock.advance(timedelta(seconds=5))

    assert (await desk.state())["session"]["self_cancel_until"] is None
    res = await _cancel(desk, session["id"])
    assert (res.status_code, res.json()["error"]["details"]) == (409, {"reason": "INSPECTION_SAVED"})


async def test_pack_session_cancel_not_limited(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter
) -> None:
    """BR-37 chỉ áp phiên RETURN: phiên PACK hủy sau 5 phút vẫn được, không có `self_cancel_until`."""
    clock.freeze(T0)
    packer = await make_desk(api, db, 2, mode="PACK", kind="PACK", operator=None)
    body = (await packer.scan("SPXTST0000001")).json()
    assert body["outcome"] == "SESSION_OPENED", body
    assert body["state"]["session"]["self_cancel_until"] is None
    clock.advance(timedelta(minutes=5))

    res = await _cancel(packer, body["state"]["session"]["id"], "OUT_OF_STOCK")

    assert res.status_code == 200, res.text


async def test_supervisor_cancel_return_needs_note_and_summary(desk: Desk, db: AsyncSession) -> None:
    """D13 (API-20 `return_summary`) → API-21 `CANCEL_SESSION` phiên RETURN: thiếu ghi chú / < 5 ký tự → 422;
    đủ → `CANCELLED` lý do `SUPERVISOR` + ghi chú."""
    await make_user(db, "tst_sup_br37", "SUPERVISOR")
    login = await desk.api.post(
        "/api/v1/auth/login", json={"username": "tst_sup_br37", "password": PASSWORD, "client": "DASHBOARD"}
    )
    sup = {"Authorization": f"Bearer {login.json()['access_token']}"}
    session = await _open_return(desk, db)
    _snapshot(db, session["id"], T0 + timedelta(seconds=40), "READY")
    await db.flush()
    clock.advance(timedelta(minutes=4))
    created = await desk.api.post(
        "/api/v1/station/approval-requests",
        headers=desk.headers,
        json={"type": "ASSIST", "session_id": session["id"]},
    )
    assert created.status_code == 201, created.text
    approval_id = created.json()["approval_request"]["id"]

    item = (await desk.api.get("/api/v1/approval-requests", headers=sup)).json()["items"][0]
    assert item["session_type"] == "RETURN"
    assert item["return_summary"] == {
        "conclusion": None,
        "snapshot_count": 1,
        "opened_at": "2026-10-06T02:00:00Z",
    }

    for note in (None, "   ", "abcd"):
        res = await desk.api.post(
            f"/api/v1/approval-requests/{approval_id}/decision",
            headers=sup,
            json={"action": "CANCEL_SESSION", "note": note, "reason_code": "WRONG_SCAN"},
        )
        assert res.status_code == 422, res.text
        assert res.json()["error"]["details"]["fields"] == {"note": "Nhập ghi chú (5–500 ký tự)."}
    # (9) T-281 (02 API-21 v0.3): thiếu / sai `reason_code` → 422 `fields.reason_code` (cả hai lỗi cùng lúc).
    for code in (None, "SUPERVISOR", "FOO"):
        res = await desk.api.post(
            f"/api/v1/approval-requests/{approval_id}/decision",
            headers=sup,
            json={"action": "CANCEL_SESSION", "note": "x", "reason_code": code},
        )
        assert res.status_code == 422, res.text
        assert res.json()["error"]["details"]["fields"] == {
            "reason_code": "Chọn lý do hủy.",
            "note": "Nhập ghi chú (5–500 ký tự).",
        }
    pack = await db.get(PackSession, session["id"], populate_existing=True)
    assert pack is not None
    assert pack.status == "WAITING_APPROVAL"  # 422 không đổi phiên

    res = await desk.api.post(
        f"/api/v1/approval-requests/{approval_id}/decision",
        headers=sup,
        json={"action": "CANCEL_SESSION", "note": "  Quét nhầm kiện bên cạnh  ", "reason_code": "WRONG_SCAN"},
    )
    assert res.status_code == 200, res.text
    pack = await db.get(PackSession, session["id"], populate_existing=True)
    assert pack is not None
    assert (pack.status, pack.cancel_reason, pack.cancel_cause, pack.note) == (
        "CANCELLED",
        "SUPERVISOR",
        "WRONG_SCAN",
        "Quét nhầm kiện bên cạnh",
    )
    from aicam.core.audit import AuditLog

    audit = await db.scalar(select(AuditLog).where(AuditLog.action == "APPROVAL_DECISION"))
    assert audit is not None
    assert audit.data["reason_code"] == "WRONG_SCAN"
