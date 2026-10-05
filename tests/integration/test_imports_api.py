"""API-50..54 nhập đơn từ file (T-17): FR-05.09, 05.10, BR-17, EX-P11.

TC-05.11 (phần API), 05.12, 05.13, 05.14, 05.15, 05.16, 05.17, 05.18, 05.20, 05.21.
"""

import hashlib
import io
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, OrderItem, Package, StatusHistory
from aicam.modules.platforms.mock.adapter import MockAdapter

from .factories import PASSWORD, make_user

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "qa" / "fixtures" / "csv"
HEADER = "platform_order_sn,tracking_number,sku,product_name,variation,quantity,buyer_note\n"
NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, tmp_path: Path) -> None:
    test_settings.import_root = tmp_path / "imports"
    clock.freeze(NOW)


async def _token(api: AsyncClient, username: str) -> dict[str, str]:
    """Đăng nhập lại sau khi tua giờ (access token 15 phút)."""
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _login(api: AsyncClient, db: AsyncSession, username: str, role: str) -> dict[str, str]:
    await make_user(db, username, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture
async def sup(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    return await _login(api, db, "tst_sup", "SUPERVISOR")


async def _upload(api: AsyncClient, h: dict[str, str], content: bytes, name: str = "don.csv") -> Response:
    return await api.post("/api/v1/imports", headers=h, files={"file": (name, content, "text/csv")})


async def test_ok_500_preview_commit_history(api: AsyncClient, db: AsyncSession, sup: dict[str, str]) -> None:
    """TC-05.11 (API): 500 đơn mới → xem trước → nhập; API-30 thấy kiện nguồn CSV; lịch sử có dòng."""
    res = await _upload(api, sup, (FIXTURES / "ok_500.csv").read_bytes(), "ok_500.csv")
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["status"] == "PREVIEW"
    assert body["counts"] == {"new": 500, "updated": 0, "skipped": 0, "error": 0}
    assert body["errors"] == []
    assert len(body["sample"]) == 20
    assert body["sample"][0] == {
        "row": 2, "tracking_number": "SPXCSV0000001", "platform_order_sn": "2410CSV00001",
        "product_name": "Tất cổ ngắn", "variation": "Trắng", "quantity": 2, "action": "NEW",
    }  # fmt: skip
    assert body["expires_at"].startswith("2026-10-05T01:30:00")

    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert res.status_code == 200, res.text
    counts = {"new": 500, "updated": 0, "skipped": 0, "error": 0}
    assert res.json() == {"id": body["id"], "status": "COMMITTED", "counts": counts}
    assert await db.scalar(select(func.count()).select_from(Order).where(Order.source == "CSV")) == 500

    found = (await api.get("/api/v1/packages", headers=sup, params={"q": "SPXCSV0000250"})).json()
    assert found["total"] == 1
    assert found["items"][0]["source"] == "CSV"

    hist = (await api.get("/api/v1/imports", headers=sup)).json()
    assert hist["total"] == 1
    item = hist["items"][0]
    assert item["status"] == "COMMITTED"
    assert item["created_by"]["display_name"] == "tst_sup"
    assert item["counts"]["new"] == 500
    assert item["committed_at"] is not None
    # Lọc theo object: test commit thật (song song) để lại audit IMPORT_COMMIT khác, không xóa được.
    audit = await db.scalar(
        select(AuditLog).where(AuditLog.action == "IMPORT_COMMIT", AuditLog.object_id == body["id"])
    )
    assert audit is not None
    assert audit.object_id == body["id"]

    # Bấm Nhập lần 2: trả lại kết quả cũ, không nhập thêm.
    again = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert again.status_code == 200
    assert again.json()["counts"]["new"] == 500


async def test_one_error_rejects_whole_file(api: AsyncClient, db: AsyncSession, sup: dict[str, str]) -> None:
    """TC-05.12: dòng 12 bỏ trống `tracking_number` → báo đúng dòng / cột; commit 409; 0 đơn."""
    res = await _upload(api, sup, (FIXTURES / "one_error.csv").read_bytes())
    body = res.json()
    assert res.status_code == 201
    assert body["counts"]["error"] == 1
    assert body["errors"] == [{"row": 12, "column": "tracking_number", "message": "Bỏ trống"}]
    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "IMPORT_HAS_ERRORS"
    assert await db.scalar(select(func.count()).select_from(Order)) == 0


async def test_missing_column(api: AsyncClient, sup: dict[str, str]) -> None:
    """TC-05.13: thiếu cột `tracking_number` → 422 FILE_INVALID + `details.missing_columns`."""
    res = await _upload(api, sup, (FIXTURES / "missing_column.csv").read_bytes())
    assert res.status_code == 422
    err = res.json()["error"]
    assert err["code"] == "FILE_INVALID"
    assert err["details"]["missing_columns"] == ["tracking_number"]


async def test_too_big_and_too_many_rows(api: AsyncClient, sup: dict[str, str]) -> None:
    """TC-05.14 (> 5 MB) và TC-05.20 (5.001 dòng) → 422 FILE_INVALID; sai đuôi file cũng vậy."""
    big = HEADER.encode() + b"x" * (5 * 1024 * 1024)
    res = await _upload(api, sup, big)
    assert (res.status_code, res.json()["error"]["code"]) == (422, "FILE_INVALID")

    rows = "".join(f"SN{n},SPXROW{n:07d},,Ao,,1,\n" for n in range(5001))
    res = await _upload(api, sup, (HEADER + rows).encode())
    assert (res.status_code, res.json()["error"]["code"]) == (422, "FILE_INVALID")
    assert res.json()["error"]["details"]["max_rows"] == 5000

    res = await _upload(api, sup, b"abc", "don.txt")
    assert (res.status_code, res.json()["error"]["code"]) == (422, "FILE_INVALID")


async def test_overlap_api_orders_are_skipped(
    api: AsyncClient, db: AsyncSession, sup: dict[str, str]
) -> None:
    """TC-05.15 / BR-17: đơn đã có từ Shopee → bỏ qua, giữ nguyên nguồn API và sản phẩm."""
    mock = MockAdapter()
    for n in range(1, 6):
        await orders.upsert_platform_order(db, mock.orders[f"2410TST{n:05d}"])
    res = await _upload(api, sup, (FIXTURES / "overlap_api.csv").read_bytes())
    body = res.json()
    assert body["counts"] == {"new": 0, "updated": 0, "skipped": 5, "error": 0}
    assert {s["action"] for s in body["sample"]} == {"SKIP"}
    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert res.json()["counts"]["skipped"] == 5
    order = await db.scalar(select(Order).where(Order.platform_order_sn == "2410TST00001"))
    assert order is not None
    assert order.source == "API"
    items = (await db.scalars(select(OrderItem).where(OrderItem.order_id == order.id))).all()
    assert [(i.product_name, i.quantity) for i in items] == [("Áo thun basic", 2)]


async def test_api_overwrites_csv_order_keeps_history(
    api: AsyncClient, db: AsyncSession, sup: dict[str, str]
) -> None:
    """TC-05.16 / FR-05.10: đơn CSV bị API ghi đè, audit ORDER_OVERWRITTEN_BY_API giữ bản CSV cũ."""
    csv = HEADER + "2410TST00040,SPXTST0000040,SKU-CSV,Hàng nhập file,Xanh,3,ghi chú CSV\n"
    body = (await _upload(api, sup, csv.encode())).json()
    await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)

    mock = MockAdapter()
    api_order = mock.orders["2410TST00001"]
    from dataclasses import replace

    await orders.upsert_platform_order(
        db, replace(api_order, platform_order_sn="2410TST00040", tracking_numbers=("SPXTST0000040",))
    )
    order = await db.scalar(select(Order).where(Order.platform_order_sn == "2410TST00040"))
    assert order is not None
    assert order.source == "API"
    log = await db.scalar(select(AuditLog).where(AuditLog.action == "ORDER_OVERWRITTEN_BY_API"))
    assert log is not None
    assert log.data is not None
    assert log.data["items"][0]["product_name"] == "Hàng nhập file"
    assert log.data["buyer_note"] == "ghi chú CSV"


async def test_update_csv_order_keeps_package_status_and_links_unverified(
    api: AsyncClient, db: AsyncSession, sup: dict[str, str]
) -> None:
    """Đơn CSV nhập lại → UPDATE; kiện đã PACKED giữ trạng thái; kiện chưa xác minh (BR-04) được gắn đơn."""
    first = HEADER + "2410CSU00001,SPXCSU0000001,,Áo,,1,\n"
    body = (await _upload(api, sup, first.encode())).json()
    await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    package = await orders.find_package(db, "SPXCSU0000001")
    assert package is not None
    history = (await db.scalars(select(StatusHistory).where(StatusHistory.package_id == package.id))).all()
    assert [(h.from_status, h.to_status, h.source) for h in history] == [(None, "NEW", "MANUAL")]
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    unverified = await orders.create_unverified_package(db, "SPXCSU0000002")

    second = HEADER + "2410CSU00001,SPXCSU0000001,,Áo,,2,\n2410CSU00001,SPXCSU0000002,,Quần,,1,\n"
    body = (await _upload(api, sup, second.encode())).json()
    assert body["counts"] == {"new": 0, "updated": 1, "skipped": 0, "error": 0}
    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert res.json()["counts"]["updated"] == 1
    await db.refresh(package)
    await db.refresh(unverified)
    assert package.warehouse_status == "PACKED"
    assert unverified.verified is True
    assert unverified.order_id == package.order_id
    items = (await db.scalars(select(OrderItem).where(OrderItem.order_id == package.order_id))).all()
    assert sorted((i.product_name, i.quantity) for i in items) == [("Quần", 1), ("Áo", 2)]


async def test_tracking_conflicts_are_row_errors(
    api: AsyncClient, db: AsyncSession, sup: dict[str, str]
) -> None:
    """Một mã vận đơn cho 2 đơn trong file, hoặc đã thuộc đơn khác trong hệ thống → lỗi dòng."""
    await orders.upsert_platform_order(db, MockAdapter().orders["2410TST00003"])
    csv = HEADER + (
        "2410CSX00001,SPXCSX0000001,,Áo,,1,\n"
        "2410CSX00002,SPXCSX0000001,,Áo,,1,\n"
        "2410CSX00003,SPXTST0000003,,Áo,,1,\n"
        "2410CSX00004,spxcsx0000004,,Áo,,abc,\n"
    )
    body = (await _upload(api, sup, csv.encode())).json()
    assert body["counts"]["error"] == 3
    assert body["errors"] == [
        {
            "row": 3,
            "column": "tracking_number",
            "message": "Mã vận đơn đã dùng cho đơn 2410CSX00001 ở dòng 2",
        },
        {"row": 4, "column": "tracking_number", "message": "Mã vận đơn đã thuộc đơn 2410TST00003"},
        {"row": 5, "column": "quantity", "message": "Phải là số nguyên từ 1 đến 10000"},
    ]


async def test_preview_expires(api: AsyncClient, sup: dict[str, str]) -> None:
    """TC-05.17: quá 30 phút → 409 IMPORT_EXPIRED."""
    body = (await _upload(api, sup, (FIXTURES / "overlap_api.csv").read_bytes())).json()
    clock.advance(timedelta(minutes=31))
    sup = await _token(api, "tst_sup")
    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert (res.status_code, res.json()["error"]["code"]) == (409, "IMPORT_EXPIRED")
    hist = (await api.get("/api/v1/imports", headers=sup)).json()
    assert hist["items"][0]["status"] == "EXPIRED"


async def test_original_file_and_expiry(api: AsyncClient, sup: dict[str, str]) -> None:
    """TC-05.18: file gốc tải về trùng SHA-256; TC-05.21: sau 90 ngày → 410 FILE_EXPIRED."""
    content = (FIXTURES / "ok_500.csv").read_bytes()
    body = (await _upload(api, sup, content, "đơn 04-10.csv")).json()
    await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    res = await api.get(f"/api/v1/imports/{body['id']}/file", headers=sup)
    assert res.status_code == 200
    assert hashlib.sha256(res.content).hexdigest() == hashlib.sha256(content).hexdigest()
    assert "attachment" in res.headers["content-disposition"]

    clock.advance(timedelta(days=91))
    sup = await _token(api, "tst_sup")
    res = await api.get(f"/api/v1/imports/{body['id']}/file", headers=sup)
    assert (res.status_code, res.json()["error"]["code"]) == (410, "FILE_EXPIRED")
    res = await api.get(f"/api/v1/imports/{uuid.uuid4()}/file", headers=sup)
    assert res.status_code == 404


async def test_xlsx_and_semicolon_csv(api: AsyncClient, sup: dict[str, str]) -> None:
    """Excel: mã / số lượng gõ dạng số vẫn đọc đúng; CSV `;` + BOM (Excel tiếng Việt)."""
    from openpyxl import Workbook

    book = Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.append(["Platform_Order_SN", "tracking_number", "product_name", "quantity"])
    sheet.append([241000001, "SPXXLS0000001", "Áo", 2.0])
    buf = io.BytesIO()
    book.save(buf)
    res = await _upload(api, sup, buf.getvalue(), "don.xlsx")
    assert res.status_code == 201, res.text
    assert res.json()["sample"][0]["platform_order_sn"] == "241000001"
    assert res.json()["sample"][0]["quantity"] == 2

    csv = "﻿platform_order_sn;tracking_number;product_name;quantity\n2410SC001;SPXSEMI000001;Áo;1\n"
    res = await _upload(api, sup, csv.encode())
    assert res.json()["counts"]["new"] == 1


async def test_permissions_and_template(api: AsyncClient, db: AsyncSession, sup: dict[str, str]) -> None:
    """Ma trận quyền: CSKH không nhập; chỉ người tạo xác nhận; API-53 file mẫu đúng cột."""
    cskh = await _login(api, db, "tst_cskh", "CSKH")
    admin = await _login(api, db, "tst_admin", "ADMIN")
    res = await _upload(api, cskh, (FIXTURES / "overlap_api.csv").read_bytes())
    assert res.status_code == 403
    body = (await _upload(api, sup, (FIXTURES / "overlap_api.csv").read_bytes())).json()
    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=admin)
    assert res.status_code == 403
    res = await api.get("/api/v1/imports/template", headers=sup)
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert res.content.decode("utf-8-sig").strip() == HEADER.strip()
    assert (await api.get("/api/v1/imports/template", headers=cskh)).status_code == 403
    assert await db.scalar(select(func.count()).select_from(Package)) == 0


async def test_commit_does_not_steal_package_claimed_after_classify(
    api: AsyncClient, db: AsyncSession, sup: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-F5 (BR-17): sau bước phân loại lại, đồng bộ sàn gắn mã vận đơn vào đơn API khác → lúc ghi khóa dòng
    kiện, thấy đã thuộc đơn khác → 409 IMPORT_CONFLICT, không nhập phần nào; bấm lại → lỗi dòng."""
    from aicam.modules.imports import service as imports_service
    from aicam.modules.platforms.base import PlatformItem, PlatformOrder

    csv = HEADER + "2410CSY00001,SPXCSY0000001,,Áo,,1,\n2410CSY00002,SPXCSY0000002,,Quần,,1,\n"
    body = (await _upload(api, sup, csv.encode())).json()
    assert body["counts"]["new"] == 2
    original = imports_service.classify

    async def classify_then_platform_claims(session: AsyncSession, parsed: Any) -> Any:
        result = await original(session, parsed)
        await orders.upsert_platform_order(
            session,
            PlatformOrder("2410APIY0001", "READY_TO_SHIP", ("SPXCSY0000002",), (PlatformItem("Quần", 1),)),
        )
        return result

    monkeypatch.setattr(imports_service, "classify", classify_then_platform_claims)
    res = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)

    assert (res.status_code, res.json()["error"]["code"]) == (409, "IMPORT_CONFLICT")
    assert await db.scalar(select(Order).where(Order.platform_order_sn == "2410CSY00001")) is None
    monkeypatch.setattr(imports_service, "classify", original)
    await orders.upsert_platform_order(
        db, PlatformOrder("2410APIY0001", "READY_TO_SHIP", ("SPXCSY0000002",), (PlatformItem("Quần", 1),))
    )
    again = await api.post(f"/api/v1/imports/{body['id']}/commit", headers=sup)
    assert (again.status_code, again.json()["error"]["code"]) == (409, "IMPORT_HAS_ERRORS")
    package = await orders.find_package(db, "SPXCSY0000002")
    owner = await db.get(Order, package.order_id) if package and package.order_id else None
    assert owner is not None
    assert owner.platform_order_sn == "2410APIY0001"
