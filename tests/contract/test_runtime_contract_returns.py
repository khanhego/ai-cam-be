"""Contract test runtime Phase 2 (T-118; 02 §6 v0.4, 02a §11): gọi thật API hàng hoàn / đối soát /
khiếu nại / ảnh / cài đặt mở rộng và kiểm mọi mốc giờ là ISO-8601 UTC hậu tố `Z`, lỗi theo dạng
`{"error": {...}}` với mã 02.

Bảng tĩnh `spec.CONTRACT` ↔ `/openapi.json` (path, method, mã, trường, enum) ở `test_openapi_contract.py`.
"""

import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.media import snapshots
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.reconciliation import service as recon
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.settings.models import Setting
from aicam.modules.stations.models import Camera
from aicam.realtime import publish
from tests.contract.spec import CONTRACT
from tests.integration.factories import PASSWORD, make_user
from tests.integration.returns_helpers import (
    buyer_return_case,
    make_desk,
    make_order,
    pack_session_with_clips,
)

from .test_runtime_contract import _assert_utc_z

pytestmark = pytest.mark.integration

JPEG = b"\xff\xd8\xff\xe0" + b"0" * 64 + b"\xff\xd9"


@pytest.fixture(autouse=True)
def _quiet(monkeypatch: pytest.MonkeyPatch, test_settings: Settings, tmp_path: Path) -> None:
    async def _drop(*_: Any, **__: Any) -> None:
        return None

    async def _grab(url: str, timeout_s: float) -> bytes:
        return JPEG

    monkeypatch.setattr(publish, "to_dashboard", _drop)
    monkeypatch.setattr(snapshots, "_grab", _grab)
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()


async def _login(api: AsyncClient, db: AsyncSession, username: str, role: str) -> dict[str, str]:
    await make_user(db, username, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


def _error(res: Response, status: int, code: str) -> None:
    assert res.status_code == status, (code, res.status_code, res.text)
    err = res.json()["error"]
    assert err["code"] == code, err
    assert isinstance(err["message"], str)
    assert err["message"]


_BY_ID = {a.id: a for a in CONTRACT}


def _values(body: Any, path: list[str]) -> list[Any] | None:
    """Giá trị tại đường dẫn chấm (`[]` = mọi phần tử). None = thiếu khóa (khác giá trị null)."""
    if not path:
        return [body]
    head, rest = path[0], path[1:]
    if body is None:
        return []  # cha null / rỗng: không xét con
    if head.endswith("[]"):
        key = head[:-2]
        if not isinstance(body, dict) or key not in body:
            return None
        out: list[Any] = []
        for item in body[key] or []:
            found = _values(item, rest)
            if found is None:
                return None
            out.extend(found)
        return out
    if not isinstance(body, dict) or head not in body:
        return None
    return _values(body[head], rest)


def _assert_contract_shape(label: str, body: Any) -> int:
    """G3 R13: response thật có đủ trường bắt buộc của 02 §6 (spec.py) và giá trị enum hợp lệ."""
    api = _BY_ID.get(label)
    if api is None or not isinstance(body, dict):
        return 0
    for name in api.fields:
        assert _values(body, name.split(".")) is not None, f"{label}: thiếu trường {name}"
    for name, allowed in api.enums.items():
        for value in _values(body, name.split(".")) or []:
            assert value is None or value in allowed, f"{label}: {name} = {value!r} ngoài enum 02"
    return len(api.fields)


async def test_phase2_responses_follow_contract(
    api: AsyncClient, db: AsyncSession, redis_client: object, test_settings: Settings
) -> None:
    api._transport.app.dependency_overrides[get_platform_adapter] = MockAdapter  # type: ignore[attr-defined]
    adm = await _login(api, db, "tst_c2_admin", "ADMIN")
    sup = await _login(api, db, "tst_c2_sup", "SUPERVISOR")
    cskh = await _login(api, db, "tst_c2_cskh", "CSKH")
    desk = await make_desk(api, db)
    db.add(Camera(station_id=desk.station.id, role="CAM1", rtsp_url="rtsp://x", mediamtx_path="cam-c2"))
    order, (package,) = await make_order(db, 41)
    pack = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=3))
    case = await buyer_return_case(db, order, 41)
    await db.flush()
    checked: dict[str, int] = {}

    async def call(label: str, method: str, url: str, headers: dict[str, str], status: int = 200,
                   **kw: Any) -> Any:  # fmt: skip
        res = await api.request(method, f"/api/v1{url}", headers=headers, **kw)
        assert res.status_code == status, f"{label}: {res.status_code} {res.text}"
        body = res.json() if res.content else None
        checked[label] = checked.get(label, 0) + (_assert_utc_z(body, label) if body is not None else 0)
        if res.status_code == next((a.status for a in CONTRACT if a.id == label), res.status_code):
            _assert_contract_shape(label, body)
        return body

    async def scan(code: str) -> Any:
        return await call("API-11", "POST", "/station/scan", desk.headers,
                          json={"code": code, "client_scan_id": str(uuid.uuid4())})  # fmt: skip

    # ---- Bàn nhận hoàn: API-100 / 101 / 104 / 11 mở / 103 / 106 / 102 / 11 đóng / 10 / 15
    await call("API-100", "PUT", "/station/work-mode", desk.headers, json={"work_mode": "RETURN"})
    await call("API-101", "PUT", "/station/operator", desk.headers, json={"name": "Lan QA"})
    lookup = await call(
        "API-104", "GET", "/station/return-lookup", desk.headers, params={"q": "SPXRTTST000041"}
    )
    assert lookup["items"][0]["can_open"] is True
    opened = await scan("SPXRTTST000041")
    assert opened["outcome"] == "SESSION_OPENED", opened
    session = opened["state"]["session"]
    assert session["started_at"].endswith("Z")
    shot = await call("API-103", "POST", f"/station/sessions/{session['id']}/snapshots", desk.headers, 201)
    assert shot["snapshot"]["taken_at"].endswith("Z")
    image = await api.get(shot["snapshot"]["url"])
    assert (image.status_code, image.headers["content-type"]) == (200, "image/jpeg")  # API-106
    lines = [{"order_item_id": li["order_item_id"], "quantity_received": li["quantity_requested"],
              "condition": "DAMAGED"} for li in session["inspection"]["lines"]]  # fmt: skip
    await call("API-102", "PUT", f"/station/sessions/{session['id']}/inspection", desk.headers,
               json={"conclusion": "DAMAGED", "lines": lines, "note": "Rách tay áo"})  # fmt: skip
    closed = await scan("SPXRTTST000041")
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    assert closed["closed_session"]["type"] == "RETURN"
    await call("API-10", "GET", "/station/state", desk.headers)
    await call("API-15", "GET", "/station/sessions/recent", desk.headers)

    # ---- Tra cứu / hồ sơ hàng hoàn: API-110 / 111 / 30 / 31 / 32
    listing = await call("API-110", "GET", "/returns", cskh, params={"tab": "ALL", "q": "SPXRTTST000041"})
    assert listing["items"][0]["id"] == str(case.id)
    detail = await call("API-111", "GET", f"/returns/{case.id}", cskh)
    assert detail["sessions"][0]["conclusion"] == "DAMAGED"
    await call("API-30", "GET", "/packages", cskh, params={"q": "SPXRTTST000041"})
    pkg = await call("API-31", "GET", f"/packages/{package.id}", cskh)
    assert any(s["type"] == "RETURN" for s in pkg["sessions"])
    await call("API-32", "GET", "/reports/daily", sup)

    # ---- Hồ sơ khiếu nại tự tạo (BR-08): API-130 / 132 / 133 / 134 / 135 / 136 / 137
    claims = await call("API-130", "GET", "/claims", cskh, params={"q": "SPXTST0000041"})
    claim_id = claims["items"][0]["id"]
    claim = await call("API-132", "GET", f"/claims/{claim_id}", cskh)
    patched = await call("API-133", "PATCH", f"/claims/{claim_id}", cskh,
                         json={"version": claim["version"], "platform_claim_ref": "SHP-C2"})  # fmt: skip
    _error(await api.patch(f"/api/v1/claims/{claim_id}", headers=cskh,
                           json={"version": claim["version"], "platform_claim_ref": "cũ"}),
           409, "VERSION_CONFLICT")  # fmt: skip
    sessions = sorted(
        {e["session"]["id"] for e in claim["evidence"] if e["kind"] == "SESSION"} | {str(pack.id)}
    )
    snaps = sorted({e["snapshot"]["id"] for e in claim["evidence"] if e["kind"] == "SNAPSHOT"})
    evidence = await call("API-134", "PUT", f"/claims/{claim_id}/evidence", cskh,
                          json={"version": patched["version"], "session_ids": sessions, "snapshot_ids": snaps,
                                "note": "Giữ đủ bằng chứng tự chọn"})  # fmt: skip
    await call("API-135", "POST", f"/claims/{claim_id}/notes", cskh, 201, json={"text": "Đã gửi sàn"})
    started = await call("API-136", "POST", f"/claims/{claim_id}/evidence-packs", cskh, 202)
    await call("API-137", "GET", f"/evidence-packs/{started['id']}", cskh)
    bad_sig = {"uid": str(uuid.uuid4()), "exp": "9999999999", "sig": "x" * 64}
    _error(await api.get(f"/api/v1/media/evidence-packs/{started['id']}/pack.zip", params=bad_sig),
           403, "SIGNATURE_INVALID")  # fmt: skip
    _error(await api.post("/api/v1/claims", headers=cskh, json={
        "package_id": str(package.id), "type": "DAMAGED", "counterparty": "PLATFORM"}),
           409, "CLAIM_EXISTS")  # fmt: skip
    assert evidence["version"] >= patched["version"]  # không đổi bằng chứng → không tăng version

    # ---- Sửa kết luận (API-113), giữ clip chỉ ADMIN (API-42), retention (API-80 / 82)
    corrected = await call("API-113", "PUT", f"/sessions/{session['id']}/inspection", sup, json={
        "conclusion": "DAMAGED", "lines": lines, "note": "Rách 2 chỗ", "reason": "Kiểm lại ảnh"})  # fmt: skip
    assert corrected is not None
    pack_clip = next(c["id"] for s in pkg["sessions"] if s["id"] == str(pack.id) for c in s["clips"])
    _error(
        await api.put(f"/api/v1/clips/{pack_clip}/hold", headers=sup, json={"held": True}), 403, "FORBIDDEN"
    )
    await call("API-42", "PUT", f"/clips/{pack_clip}/hold", adm, json={"held": True})
    current = await call("API-80", "GET", "/settings", adm)
    await call("API-82", "GET", "/settings/retention-impact", adm,
               params={"retention_raw_days": current["retention_raw_days"],
                       "retention_clip_days": 70})  # fmt: skip

    # ---- Đối soát: J-14 thật trên kiện PACKED 25 giờ → API-120 / 121 / 122 / 123
    row = await db.get(Setting, 1)
    assert row is not None
    row.recon_start_at = clock.now() - timedelta(days=30)
    _, (late,) = await make_order(db, 52, warehouse_status="PACKED", status="READY_TO_SHIP")
    late.status_changed_at = clock.now() - timedelta(hours=25)
    _, (fresh,) = await make_order(db, 53, warehouse_status="NEW", status="READY_TO_SHIP")
    await db.flush()
    await recon.run_rules(db, test_settings)
    alerts = await call("API-120", "GET", "/recon-alerts", sup, params={"package_id": str(late.id)})
    assert alerts["items"][0]["rule"] == "PACKED_NOT_HANDED_OVER"
    await call("API-121", "POST", f"/recon-alerts/{alerts['items'][0]['id']}/resolve", sup,
               json={"note": "Đã gọi ĐVVC"})  # fmt: skip
    await call("API-122", "POST", f"/packages/{fresh.id}/warehouse-status", sup,
               json={"to_status": "HANDED_OVER", "reason": "ĐVVC đã lấy, sàn chưa báo"})  # fmt: skip
    await call("API-123", "POST", "/recon/run", sup, 202)

    # ---- Phiên chưa xác định (API-105) → hủy; chế độ sai → WRONG_WORK_MODE
    unknown = await call("API-105", "POST", "/station/return-sessions", desk.headers,
                         json={"unidentified_code": "SPXVN0000000000",
                               "client_scan_id": str(uuid.uuid4())})  # fmt: skip
    assert unknown["outcome"] == "SESSION_OPENED", unknown
    unknown_id = unknown["state"]["session"]["id"]
    await call("API-12", "POST", f"/station/sessions/{unknown_id}/cancel", desk.headers,
               json={"reason": "NOT_A_RETURN"})  # fmt: skip
    await call("API-100", "PUT", "/station/work-mode", desk.headers, json={"work_mode": "PACK"})
    wrong = await api.get(
        "/api/v1/station/return-lookup", headers=desk.headers, params={"q": "SPXRTTST000041"}
    )
    _error(wrong,
           409, "WRONG_WORK_MODE")  # fmt: skip

    total = sum(checked.values())
    assert total >= 60, f"chỉ kiểm được {total} mốc giờ: {checked}"
    # API-104, API-137 không có trường giờ trong 02 §6 (chỉ kiểm khi có).
    expected = {"API-11", "API-103", "API-110", "API-111", "API-31", "API-130", "API-132", "API-133",
                "API-134", "API-113", "API-120", "API-121"}  # fmt: skip
    missing = expected - {k for k, v in checked.items() if v}
    assert not missing, f"response không có mốc giờ để kiểm: {missing}"


def test_contract_shape_check_detects_missing_field_and_bad_enum() -> None:
    """Tự kiểm bộ so: thiếu trường / enum lạ → lỗi."""
    with pytest.raises(AssertionError, match="thiếu trường"):
        _assert_contract_shape("API-100", {})
    api = _BY_ID["API-11"]
    body = {name.split(".")[0]: None for name in api.fields}
    body["outcome"] = "LẠ"
    with pytest.raises(AssertionError, match="ngoài enum"):
        _assert_contract_shape("API-11", body)
