"""Hồ sơ khiếu nại (T-110): tự tạo từ phiên hoàn (BR-08, BR-27), API-130..135, J-15, gộp kiện tạm.

02a §4 API-130..135, §5; TC-04.21, TC-04.22, TC-08.01..15, TC-08.11, TC-08.12, AC-06, AC-37.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.modules.claims import service as claims
from aicam.modules.claims.models import Claim, ClaimNote
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Package
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.returns import service as returns
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_user
from .returns_helpers import (
    Desk,
    buyer_return_case,
    make_desk,
    make_order,
    pack_session_with_clips,
    return_session,
)

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)
REASON = "Phiên đóng gói sai kiện"


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    return await make_desk(api, db)


async def _login(api: AsyncClient, db: AsyncSession, role: str = "CSKH", name: str = "") -> dict[str, str]:
    user = await make_user(db, f"tst_{role.lower()}{name}", role, display_name=f"{role} {name}".strip())
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


def _lines(session: dict[str, Any], **override: Any) -> list[dict[str, Any]]:
    return [
        {
            "order_item_id": line["order_item_id"],
            "quantity_received": override.get("quantity_received", line["quantity_received"]),
            "condition": override.get("condition", line["condition"]),
            "note": None,
        }
        for line in session["inspection"]["lines"]
    ]


async def _receive(
    desk: Desk, code: str, conclusion: str, close_code: str | None = None, **lines: Any
) -> Any:
    """Mở phiên bằng `code`, lưu kết luận, quét lại (hoặc `close_code`) để đóng. Trả body API-11 đóng."""
    opened = (await desk.scan(code)).json()
    assert opened["outcome"] == "SESSION_OPENED", opened
    session = opened["state"]["session"]
    saved = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection",
        headers=desk.headers,
        json={
            "conclusion": conclusion,
            "note": "ghi chú" if conclusion == "OTHER" else "",
            "lines": _lines(session, **lines),
        },
    )
    assert saved.status_code == 200, saved.text
    closed = (await desk.scan(close_code or code)).json()
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    return closed


# ---------------------------------------------------------------- BR-08 tự tạo


async def test_issue_creates_claim_with_pack_and_return_evidence(
    desk: Desk, db: AsyncSession, api: AsyncClient
) -> None:
    """TC-04.21, AC-06, TC-08.11: "Hộp rỗng" → KN tự tạo: loại EMPTY_BOX, Sàn, hạn = hạn người bán; bằng chứng
    tự chọn = phiên PACK hiệu lực + phiên RETURN + ảnh (lúc đóng gói + chụp tay)."""
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)
    case.seller_due_at = clock.now() + timedelta(days=2)  # hạn sàn còn (hạn đã qua → BR-42, T-214)
    old = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=6))
    old.status = "SUPERSEDED"
    pack = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=5))
    opened = (await desk.scan("SPXRTTST000041")).json()
    session_id = uuid.UUID(opened["state"]["session"]["id"])
    db.add(
        Snapshot(
            session_id=session_id,
            kind="MANUAL",
            camera_role="CAM1",
            taken_at=clock.now(),
            path="snapshots/x_01.jpg",
            sha256="ef" * 32,
            size_bytes=5,
            status="READY",
        )
    )
    await db.flush()
    await desk.api.put(
        f"/api/v1/station/sessions/{session_id}/inspection",
        headers=desk.headers,
        json={
            "conclusion": "EMPTY_BOX",
            "note": "",
            "lines": _lines(opened["state"]["session"], quantity_received=0, condition="MISSING_ITEM"),
        },
    )

    body = (await desk.scan("SPXTST0000041")).json()

    code = body["closed_session"]["claim_code"]
    assert code is not None
    assert code.startswith("KN-")
    claim = await db.scalar(select(Claim).where(Claim.code == code))
    assert claim is not None
    headers = await _login(api, db)
    detail = (await api.get(f"/api/v1/claims/{claim.id}", headers=headers)).json()
    assert (detail["type"], detail["counterparty"], detail["status"], detail["source"]) == (
        "EMPTY_BOX",
        "PLATFORM",
        "NEW",
        "AUTO_RETURN",
    )
    assert detail["return_case"]["code"] == case.code
    assert detail["deadline_source"] == "PLATFORM"
    assert datetime.fromisoformat(detail["deadline_at"]) == case.seller_due_at
    sessions = [e["session"] for e in detail["evidence"] if e["kind"] == "SESSION"]
    assert [(s["id"], s["type"]) for s in sessions] == [(str(pack.id), "PACK"), (str(session_id), "RETURN")]
    assert all(e["auto"] for e in detail["evidence"])
    snapshots = [e["snapshot"] for e in detail["evidence"] if e["kind"] == "SNAPSHOT"]
    assert sorted(s["kind"] for s in snapshots) == ["MANUAL", "PACK_CLOSE"]
    assert all(s["url"].startswith("/api/v1/media/snapshots/") for s in snapshots)
    assert [o["id"] for o in detail["other_sessions"]] == [str(old.id)]  # TC-08.13
    assert detail["missing"] == ["RETURN_CLIP_PENDING"]  # clip phiên hoàn chưa cắt
    assert detail["notes"][0]["text"] == "Tạo tự động từ phiên mở hoàn (Hộp rỗng)."
    assert detail["allowed_transitions"] == ["SUBMITTED", "CLOSED"]
    audit = await db.scalar(
        select(AuditLog).where(AuditLog.action == "CLAIM_CREATE", AuditLog.object_id == str(claim.id))
    )
    assert audit is not None
    assert audit.user_id is None


async def test_ok_conclusion_no_claim(desk: Desk, db: AsyncSession) -> None:
    """TC-04.19: "Nguyên vẹn" → không có hồ sơ khiếu nại."""
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)

    body = await _receive(desk, "SPXRTTST000041", "OK")

    assert body["closed_session"]["claim_code"] is None
    assert await db.scalar(select(Claim.id)) is None


async def test_failed_delivery_goes_to_carrier(desk: Desk, db: AsyncSession) -> None:
    """BR-08 (02 §6.3 #15): hồ sơ hàng hoàn "Giao thất bại" → bên nhận ĐVVC; không có hạn sàn → mặc định."""
    order, (package,) = await make_order(db, 42, warehouse_status="HANDED_OVER")
    await returns.attach_or_create(db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:42"))
    await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=2))

    body = await _receive(desk, "SPXTST0000042", "DAMAGED", condition="DAMAGED")

    claim = await db.scalar(select(Claim).where(Claim.code == body["closed_session"]["claim_code"]))
    assert claim is not None
    assert (claim.counterparty, claim.type, claim.deadline_source) == ("CARRIER", "DAMAGED", "DEFAULT")
    assert claim.deadline_at is not None
    assert abs((claim.deadline_at - clock.now()) - timedelta(days=7)) < timedelta(minutes=1)


async def test_existing_claim_same_type_gets_evidence(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """BR-27: kiện đã có hồ sơ "Hộp rỗng" mở → phiên hoàn mới thêm vào hồ sơ đó (không tạo hồ sơ thứ hai)."""
    order, (package,) = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    headers = await _login(api, db)
    created = await api.post(
        "/api/v1/claims",
        headers=headers,
        json={"package_id": str(package.id), "type": "EMPTY_BOX", "counterparty": "PLATFORM"},
    )
    assert created.status_code == 201
    first = created.json()

    body = await _receive(desk, "SPXRTTST000041", "EMPTY_BOX", quantity_received=0, condition="MISSING_ITEM")

    assert body["closed_session"]["claim_code"] == first["code"]
    assert len((await db.scalars(select(Claim))).all()) == 1
    detail = (await api.get(f"/api/v1/claims/{first['id']}", headers=headers)).json()
    assert detail["version"] == first["version"] + 1
    assert any(e["kind"] == "SESSION" and e["session"]["type"] == "RETURN" for e in detail["evidence"])
    assert detail["notes"][-1]["text"].startswith("Thêm phiên mở hoàn lúc ")


# ---------------------------------------------------------------- TC-04.22 (AC-06): 5 loại kết luận có vấn đề

# (kết luận, tình trạng dòng, số nhận / 2) — dòng nhất quán với kết luận (BR-22 chỉ khóa "Nguyên vẹn").
CONCLUSION_LINES: dict[str, tuple[str, int]] = {
    "OK": ("OK", 2),
    "DAMAGED": ("DAMAGED", 2),
    "MISSING_ITEM": ("MISSING_ITEM", 1),
    "WRONG_ITEM": ("WRONG_ITEM", 2),
    "EMPTY_BOX": ("MISSING_ITEM", 0),
    "OTHER": ("OK", 2),
}


async def _j01_ready(db: AsyncSession, session_id: uuid.UUID) -> None:
    """Giả lập J-01 đã cắt xong clip Cam 1 / Cam 2 của phiên RETURN (`READY`)."""
    pack = await db.get(PackSession, session_id)
    assert pack is not None
    for role in ("CAM1", "CAM2"):
        db.add(
            Clip(
                session_id=session_id,
                camera_role=role,
                status="READY",
                start_at=pack.started_at,
                end_at=(pack.ended_at or pack.started_at) + timedelta(seconds=5),
                path=f"clips/{session_id}-{role}.mp4",
                sha256="ab" * 32,
                flags=[],
            )
        )
    await db.flush()


async def _receive_parcel(
    desk: Desk, db: AsyncSession, n: int, conclusion: str
) -> tuple[Package, uuid.UUID, uuid.UUID, Any]:
    """Kiện n: đơn + hồ sơ khách trả + phiên PACK có clip; mở / kết luận / đóng.

    Trả (kiện, id phiên PACK, id phiên RETURN, body API-11 đóng)."""
    order, (package,) = await make_order(db, n)
    await buyer_return_case(db, order, n)
    pack = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=3))
    condition, received = CONCLUSION_LINES[conclusion]
    body = await _receive(
        desk, f"SPXRTTST{n:06d}", conclusion, quantity_received=received, condition=condition
    )
    return package, pack.id, uuid.UUID(body["closed_session"]["id"]), body


@pytest.mark.parametrize("conclusion", ["DAMAGED", "MISSING_ITEM", "WRONG_ITEM", "EMPTY_BOX", "OTHER", "OK"])
async def test_each_conclusion_creates_matching_claim(
    desk: Desk, db: AsyncSession, api: AsyncClient, conclusion: str
) -> None:
    """TC-04.22, AC-06, BR-08: từng kết luận có vấn đề → hồ sơ khiếu nại tự tạo đúng loại (= kết luận), bên
    nhận Sàn (khách trả hàng), bằng chứng = phiên PACK + phiên RETURN; sau J-01 READY không còn thiếu clip.
    "Nguyên vẹn" → không có hồ sơ."""
    package, pack_id, return_id, body = await _receive_parcel(desk, db, 51, conclusion)

    code = body["closed_session"]["claim_code"]
    if conclusion == "OK":
        assert code is None
        assert body["closed_session"]["return_case_status"] == "RECEIVED_OK"
        assert await db.scalar(select(Claim.id).where(Claim.package_id == package.id)) is None
        return
    assert body["closed_session"]["return_case_status"] == "RECEIVED_ISSUE"
    claim = await db.scalar(select(Claim).where(Claim.code == code))
    assert claim is not None
    assert (claim.type, claim.counterparty, claim.source, claim.status) == (
        conclusion,
        "PLATFORM",
        "AUTO_RETURN",
        "NEW",
    )
    await _j01_ready(db, return_id)
    headers = await _login(api, db)
    detail = (await api.get(f"/api/v1/claims/{claim.id}", headers=headers)).json()
    sessions = [e["session"] for e in detail["evidence"] if e["kind"] == "SESSION"]
    assert [(s["id"], s["type"]) for s in sessions] == [(str(pack_id), "PACK"), (str(return_id), "RETURN")]
    for s in sessions:
        assert sorted((c["camera_role"], c["status"]) for c in s["clips"]) == [
            ("CAM1", "READY"),
            ("CAM2", "READY"),
        ]
    assert detail["missing"] == []
    assert (
        detail["notes"][0]["text"]
        == f"Tạo tự động từ phiên mở hoàn ({claims.CONCLUSION_LABELS[conclusion]})."
    )


async def test_ten_parcels_five_ok_five_issue_types(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """TC-04.22, AC-06: 10 kiện liên tiếp trên một bàn (5 Nguyên vẹn xen 5 có vấn đề — đủ 5 loại) → 10 phiên
    `COMPLETED` có kết luận; đúng 5 hồ sơ, mỗi loại một hồ sơ, mỗi hồ sơ có phiên PACK + RETURN của chính kiện
    đó (clip READY sau J-01); 5 kiện Nguyên vẹn không có hồ sơ."""
    plan = ["OK", "DAMAGED", "OK", "MISSING_ITEM", "OK", "WRONG_ITEM", "OK", "EMPTY_BOX", "OK", "OTHER"]
    received: dict[str, tuple[Package, uuid.UUID, uuid.UUID, Any]] = {}
    for i, conclusion in enumerate(plan):
        result = await _receive_parcel(desk, db, 51 + i, conclusion)
        await _j01_ready(db, result[2])
        received[f"{conclusion}-{i}"] = result

    rows = (
        await db.scalars(
            select(PackSession).where(PackSession.type == "RETURN").order_by(PackSession.ended_at)
        )
    ).all()
    assert [(r.status, r.inspection_conclusion) for r in rows] == [("COMPLETED", c) for c in plan]
    claims_rows = (await db.scalars(select(Claim))).all()
    assert sorted(c.type for c in claims_rows) == sorted(c for c in plan if c != "OK")
    headers = await _login(api, db)
    for key, (package, pack_id, return_id, body) in received.items():
        claim = next((c for c in claims_rows if c.package_id == package.id), None)
        if key.startswith("OK-"):
            assert claim is None, key
            assert body["closed_session"]["claim_code"] is None
            continue
        assert claim is not None, key
        assert body["closed_session"]["claim_code"] == claim.code
        assert (claim.type, claim.counterparty) == (key.split("-")[0], "PLATFORM")
        detail = (await api.get(f"/api/v1/claims/{claim.id}", headers=headers)).json()
        sessions = [e["session"] for e in detail["evidence"] if e["kind"] == "SESSION"]
        assert [(s["id"], s["type"]) for s in sessions] == [
            (str(pack_id), "PACK"),
            (str(return_id), "RETURN"),
        ]
        assert detail["missing"] == [], key
    state = await desk.state()
    assert (state["today_return_count"], state["today_return_issue_count"]) == (10, 5)


# ---------------------------------------------------------------- API-131


async def test_manual_create_and_duplicate(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-08.08 (bằng chứng tự chọn = phiên PACK hiệu lực), TC-08.09 trùng loại → 409 CLAIM_EXISTS."""
    _, (package,) = await make_order(db, 11)
    pack = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=1))
    headers = await _login(api, db)
    body = {
        "package_id": str(package.id),
        "type": "BUYER_CLAIM",
        "counterparty": "PLATFORM",
        "note": "  Khách báo thiếu 1 tất  ",
    }

    res = await api.post("/api/v1/claims", headers=headers, json=body)

    assert res.status_code == 201, res.text
    detail = res.json()
    assert (detail["source"], detail["status"], detail["version"]) == ("MANUAL", "NEW", 1)
    assert [e["session"]["id"] for e in detail["evidence"] if e["kind"] == "SESSION"] == [str(pack.id)]
    assert detail["missing"] == []
    assert detail["notes"][-1]["kind"] == "NOTE"
    assert detail["notes"][-1]["text"] == "Khách báo thiếu 1 tất"
    assert detail["notes"][-1]["author"]["display_name"] == "CSKH"
    dup = await api.post("/api/v1/claims", headers=headers, json=body)
    assert dup.status_code == 409
    err = dup.json()["error"]
    assert (err["code"], err["details"]["code"], err["details"]["claim_id"]) == (
        "CLAIM_EXISTS",
        detail["code"],
        detail["id"],
    )
    other_type = await api.post("/api/v1/claims", headers=headers, json={**body, "type": "OTHER"})
    assert other_type.status_code == 201


async def test_manual_create_no_pack_clip_and_validation(api: AsyncClient, db: AsyncSession) -> None:
    """TC-08.14 `NO_PACK_CLIP`; kiện lạ → 404; hồ sơ hàng hoàn không chứa kiện → 422; Station → 403."""
    _, (package,) = await make_order(db, 50, warehouse_status="NEW")
    headers = await _login(api, db)
    body = {"package_id": str(package.id), "type": "LOST_IN_TRANSIT", "counterparty": "CARRIER"}

    res = await api.post("/api/v1/claims", headers=headers, json=body)
    assert res.status_code == 201
    assert res.json()["missing"] == ["NO_PACK_CLIP"]
    missing = await api.post(
        "/api/v1/claims", headers=headers, json={**body, "package_id": str(uuid.uuid4())}
    )
    assert missing.status_code == 404
    bad_case = await api.post(
        "/api/v1/claims", headers=headers, json={**body, "type": "OTHER", "return_case_id": str(uuid.uuid4())}
    )
    assert bad_case.json()["error"]["details"]["fields"] == {
        "return_case_id": "Hồ sơ hàng hoàn không chứa kiện này"
    }
    station = await make_desk(api, db, 2)
    assert (await api.get("/api/v1/claims", headers=station.headers)).status_code == 403


async def test_manual_from_recon_alert(api: AsyncClient, db: AsyncSession) -> None:
    """API-131 `recon_alert_id` → cảnh báo `RESOLVED` action `OPEN_CLAIM`, hồ sơ nguồn RECON."""
    from aicam.modules.reconciliation.models import ReconAlert

    _, (package,) = await make_order(db, 49, warehouse_status="RETURN_MISSING")
    alert = ReconAlert(
        package_id=package.id, rule="RETURN_OVERDUE", severity="HIGH", context={}, context_key="k"
    )
    db.add(alert)
    await db.flush()
    headers = await _login(api, db, "SUPERVISOR")

    res = await api.post(
        "/api/v1/claims",
        headers=headers,
        json={
            "package_id": str(package.id),
            "type": "LOST_IN_TRANSIT",
            "counterparty": "CARRIER",
            "recon_alert_id": str(alert.id),
        },
    )

    assert res.status_code == 201
    assert res.json()["source"] == "RECON"
    await db.refresh(alert)
    assert (alert.status, alert.resolution_action, str(alert.claim_id)) == (
        "RESOLVED",
        "OPEN_CLAIM",
        res.json()["id"],
    )


# ---------------------------------------------------------------- API-133


async def _claim(api: AsyncClient, headers: dict[str, str], db: AsyncSession, n: int = 11) -> dict[str, Any]:
    _, (package,) = await make_order(db, n)
    res = await api.post(
        "/api/v1/claims",
        headers=headers,
        json={"package_id": str(package.id), "type": "BUYER_CLAIM", "counterparty": "PLATFORM"},
    )
    assert res.status_code == 201
    return res.json()  # type: ignore[no-any-return]


async def _patch(api: AsyncClient, headers: dict[str, str], claim: dict[str, Any], **body: Any) -> Any:
    return await api.patch(
        f"/api/v1/claims/{claim['id']}", headers=headers, json={"version": claim["version"], **body}
    )


async def test_full_lifecycle(api: AsyncClient, db: AsyncSession) -> None:
    """TC-08.01, AC-37: nhận phụ trách → Đã gửi (mã sàn) → Đang chờ → Thắng (số tiền) → Đóng."""
    headers = await _login(api, db)
    claim = await _claim(api, headers, db)
    user_id = (await api.get("/api/v1/me", headers=headers)).json()["id"]

    steps: list[dict[str, Any]] = [
        {"owner_user_id": user_id},
        {"status": "SUBMITTED", "platform_claim_ref": "SPE-998877"},
        {"status": "WAITING"},
        {"status": "WON", "recovered_amount": 150000},
        {"status": "CLOSED"},
    ]
    for step in steps:
        res = await _patch(api, headers, claim, **step)
        assert res.status_code == 200, res.text
        assert res.json()["version"] == claim["version"] + 1
        claim = res.json()

    assert (claim["status"], claim["recovered_amount"], claim["platform_claim_ref"]) == (
        "CLOSED",
        150000,
        "SPE-998877",
    )
    assert claim["owner"]["id"] == user_id
    assert claim["closed_at"] is not None
    assert claim["allowed_transitions"] == []
    changes = [n["text"] for n in claim["notes"] if n["kind"] == "STATUS_CHANGE"]
    assert changes[0] == "Người phụ trách: CSKH."
    assert "Mới → Đã gửi." in changes
    assert "Thắng → Đóng." in changes
    assert "Số tiền thu hồi: 150.000 đ." in changes
    actions = (
        await db.scalars(
            select(AuditLog.action).where(
                AuditLog.action == "CLAIM_UPDATE", AuditLog.object_id == claim["id"]
            )
        )
    ).all()
    assert len(actions) == 5
    listing = (await api.get("/api/v1/claims?owner=me", headers=headers)).json()
    assert [i["code"] for i in listing["items"]] == [claim["code"]]
    assert listing["status_counts"]["CLOSED"] == 1


async def test_patch_validation_and_transitions(api: AsyncClient, db: AsyncSession) -> None:
    """TC-08.02..08.05: SUBMITTED thiếu mã, WON thiếu tiền, NEW → WON cấm, đóng sớm thiếu lý do."""
    headers = await _login(api, db)
    claim = await _claim(api, headers, db)

    submitted = await _patch(api, headers, claim, status="SUBMITTED")
    assert (submitted.status_code, submitted.json()["error"]["code"]) == (422, "VALIDATION_ERROR")
    won = await _patch(api, headers, claim, status="WON")
    assert won.status_code == 409
    assert won.json()["error"]["code"] == "INVALID_TRANSITION"
    assert won.json()["error"]["details"]["allowed"] == ["SUBMITTED", "CLOSED"]
    closed = await _patch(api, headers, claim, status="CLOSED")
    assert closed.json()["error"]["details"]["fields"] == {"reason": "Nhập lý do đóng hồ sơ 5–500 ký tự"}
    ok = await _patch(api, headers, claim, status="SUBMITTED", reason="Gửi qua chat")
    assert ok.status_code == 200
    waiting = (await _patch(api, headers, ok.json(), status="WAITING")).json()
    no_money = await _patch(api, headers, waiting, status="WON")
    assert no_money.json()["error"]["details"]["fields"] == {"recovered_amount": "Nhập số tiền thu hồi"}
    owner_bad = await _patch(api, headers, waiting, owner_user_id=str(uuid.uuid4()))
    assert "owner_user_id" in owner_bad.json()["error"]["details"]["fields"]


async def test_version_conflict_and_closed(api: AsyncClient, db: AsyncSession) -> None:
    """TC-08.07 VERSION_CONFLICT (`details.current`); TC-08.06 hồ sơ đóng: API-134 → CLAIM_CLOSED, ghi chú
    vẫn thêm được."""
    headers = await _login(api, db)
    claim = await _claim(api, headers, db)
    a = await _patch(api, headers, claim, platform_claim_ref="SPE-1")
    assert a.status_code == 200

    b = await _patch(api, headers, claim, platform_claim_ref="SPE-2")

    assert b.status_code == 409
    err = b.json()["error"]
    assert err["code"] == "VERSION_CONFLICT"
    assert err["details"]["current"]["version"] == claim["version"] + 1
    closed = (await _patch(api, headers, a.json(), status="CLOSED", reason="Không gửi nữa")).json()
    evidence = await api.put(
        f"/api/v1/claims/{claim['id']}/evidence",
        headers=headers,
        json={"version": closed["version"], "session_ids": [], "snapshot_ids": []},
    )
    assert (evidence.status_code, evidence.json()["error"]["code"]) == (409, "CLAIM_CLOSED")
    patch_closed = await _patch(api, headers, closed, platform_claim_ref="X")
    assert patch_closed.json()["error"]["code"] == "CLAIM_CLOSED"
    note = await api.post(
        f"/api/v1/claims/{claim['id']}/notes", headers=headers, json={"text": " Đã lưu hồ sơ "}
    )
    assert note.status_code == 201
    assert (note.json()["kind"], note.json()["text"]) == ("NOTE", "Đã lưu hồ sơ")
    empty = await api.post(f"/api/v1/claims/{claim['id']}/notes", headers=headers, json={"text": "   "})
    assert empty.status_code == 422


# ---------------------------------------------------------------- API-134


async def test_evidence_remove_auto_requires_note(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-08.15: bỏ bằng chứng tự chọn không ghi lý do → 422 `fields.note`; có lý do → 200 + ghi chú;
    phiên của kiện khác → 422; thêm phiên bị thay thế được (`auto = false`)."""
    _, (package,) = await make_order(db, 11)
    old = await pack_session_with_clips(
        db, desk.station, package, clock.now() - timedelta(days=3), snapshot=False
    )
    old.status = "SUPERSEDED"
    pack = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=2))
    _, (stranger,) = await make_order(db, 12)
    foreign = await pack_session_with_clips(db, desk.station, stranger, clock.now(), snapshot=False)
    headers = await _login(api, db)
    claim = (
        await api.post(
            "/api/v1/claims",
            headers=headers,
            json={"package_id": str(package.id), "type": "BUYER_CLAIM", "counterparty": "PLATFORM"},
        )
    ).json()
    url = f"/api/v1/claims/{claim['id']}/evidence"

    no_note = await api.put(
        url, headers=headers, json={"version": 1, "session_ids": [str(old.id)], "snapshot_ids": []}
    )
    assert no_note.json()["error"]["details"]["fields"] == {"note": "Nhập lý do bỏ bằng chứng (5–500 ký tự)."}
    foreign_res = await api.put(
        url, headers=headers, json={"version": 1, "session_ids": [str(foreign.id)], "snapshot_ids": []}
    )
    assert "session_ids" in foreign_res.json()["error"]["details"]["fields"]
    ok = await api.put(
        url,
        headers=headers,
        json={"version": 1, "session_ids": [str(old.id)], "snapshot_ids": [], "note": REASON},
    )

    assert ok.status_code == 200, ok.text
    detail = ok.json()
    assert [(e["session"]["id"], e["auto"]) for e in detail["evidence"]] == [(str(old.id), False)]
    # BR-38 (Phase 3): bằng chứng bỏ không mất dòng — hiện ở `removed_evidence` (thêm lại được), không ở
    # `other_sessions`.
    assert detail["other_sessions"] == []
    assert [e["session"]["id"] for e in detail["removed_evidence"] if e["session"]] == [str(pack.id)]
    assert sorted((e["kind"], e["removed"]["reason"]) for e in detail["removed_evidence"]) == [
        ("SESSION", REASON),
        ("SNAPSHOT", REASON),
    ]
    assert detail["notes"][-1]["text"] == f"Cập nhật bằng chứng: thêm 1, bỏ 2. Lý do: {REASON}"
    assert detail["version"] == 2


# ---------------------------------------------------------------- API-130, J-15


async def test_due_soon_job_and_filters(api: AsyncClient, db: AsyncSession) -> None:
    """TC-08.12: hạn còn 47 giờ → J-15 ghi chú một lần; API-130 `due=soon`; quá hạn → `due=overdue`."""
    clock.freeze(T0)
    headers = await _login(api, db)
    soon = await _claim(api, headers, db, 11)
    late = await _claim(api, headers, db, 12)
    far = await _claim(api, headers, db, 13)
    for item, delta in ((soon, timedelta(hours=47)), (late, timedelta(hours=-1)), (far, timedelta(days=5))):
        res = await _patch(api, headers, item, deadline_at=clock.iso_z(T0 + delta))
        assert res.status_code == 200

    assert await claims.check_deadlines(db) == 2
    assert await claims.check_deadlines(db) == 0

    texts = (await db.scalars(select(ClaimNote.text).where(ClaimNote.kind == "SYSTEM"))).all()
    assert sum(t.startswith("Sắp hết hạn khiếu nại") for t in texts) == 2
    due = (await api.get("/api/v1/claims?due=soon", headers=headers)).json()
    assert [(i["code"], i["due_soon"], i["overdue"]) for i in due["items"]] == [(soon["code"], True, False)]
    overdue = (await api.get("/api/v1/claims?due=overdue", headers=headers)).json()
    assert [(i["code"], i["overdue"]) for i in overdue["items"]] == [(late["code"], True)]
    by_code = (await api.get(f"/api/v1/claims?q={far['code'].lower()}", headers=headers)).json()
    assert by_code["total"] == 1
    by_tracking = (await api.get("/api/v1/claims?q=spxtst0000012", headers=headers)).json()
    assert [i["code"] for i in by_tracking["items"]] == [late["code"]]
    assert by_tracking["items"][0]["package"]["tracking_number"] == "SPXTST0000012"
    counts = (await api.get("/api/v1/claims?status=CLOSED", headers=headers)).json()
    assert (counts["total"], counts["status_counts"]["NEW"]) == (0, 3)


# ---------------------------------------------------------------- gộp kiện tạm (DEC-269, DEC-311)


async def test_unidentified_merge_moves_claim(desk: Desk, db: AsyncSession) -> None:
    """Hồ sơ khiếu nại trên kiện tạm → gộp chưa xác định sang kiện thật: hồ sơ đổi kiện + thêm phiên PACK
    hiệu lực; kiện tạm xóa được (không vướng khóa ngoại)."""
    case, placeholder = await returns.create_unidentified(db)
    pack = return_session(desk.station, placeholder, case, conclusion="DAMAGED", open_code="SPXTST0000046")
    db.add(pack)
    placeholder.warehouse_status = "RETURN_RECEIVED_ISSUE"
    await db.flush()
    created = await claims.create_from_return(db, pack, case)
    assert created is not None
    assert created.claim.package_id == placeholder.id
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")
    pack_session = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=3))

    assert await returns.merge_unidentified_by_code(db, order) == [case.id]

    await db.refresh(created.claim)
    assert (created.claim.package_id, created.claim.order_id) == (package.id, order.id)
    evidence = {e.session_id for e in await _evidence(db, created.claim.id)}
    assert {pack.id, pack_session.id} <= evidence
    assert await db.get(Package, placeholder.id) is None


async def test_unidentified_merge_into_existing_claim(desk: Desk, db: AsyncSession) -> None:
    """API-112 / DEC-260: kiện thật đã có hồ sơ cùng loại đang mở → bằng chứng + ghi chú gộp vào, hồ sơ cũ
    đóng "Gộp vào KN-…"."""
    case, placeholder = await returns.create_unidentified(db)
    pack = return_session(desk.station, placeholder, case, conclusion="DAMAGED", open_code="SPXTST0000046")
    db.add(pack)
    placeholder.warehouse_status = "RETURN_RECEIVED_ISSUE"
    await db.flush()
    moved = await claims.create_from_return(db, pack, case)
    assert moved is not None
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")
    keeper = Claim(
        package_id=package.id,
        order_id=order.id,
        type="DAMAGED",
        counterparty="PLATFORM",
        status="SUBMITTED",
        source="MANUAL",
        version=1,
    )
    db.add(keeper)
    await db.flush()
    await db.refresh(keeper, ["code"])
    merged: list[claims.MergedClaim] = []

    assert await returns.merge_unidentified(db, case, order, "SPXTST0000046", merged_claims=merged)

    await db.refresh(moved.claim)
    await db.refresh(keeper)
    assert merged == [claims.MergedClaim(moved.claim.code, keeper.code)]
    assert (moved.claim.status, moved.claim.close_reason) == ("CLOSED", f"Gộp vào {keeper.code}")
    assert pack.id in {e.session_id for e in await _evidence(db, keeper.id)}
    notes = (await db.scalars(select(ClaimNote.text).where(ClaimNote.claim_id == keeper.id))).all()
    assert f"[{moved.claim.code}] Tạo tự động từ phiên mở hoàn (Hư hỏng)." in notes


async def _evidence(db: AsyncSession, claim_id: uuid.UUID) -> list[Any]:
    from aicam.modules.claims.models import ClaimEvidence

    return list((await db.scalars(select(ClaimEvidence).where(ClaimEvidence.claim_id == claim_id))).all())
