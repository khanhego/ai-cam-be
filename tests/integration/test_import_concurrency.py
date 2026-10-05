"""API-51 xác nhận nhập chạy song song thật (02a §6, DEC-121 (4)): dữ liệu commit thật, dọn bằng TRUNCATE.

Đề xuất TC-05.22 (04 M05 chưa có TC cho `409 IMPORT_CONFLICT`):
- Cùng một bản xem trước bấm Nhập 2 lần đồng thời → khóa dòng `csv_import` (FOR UPDATE) tuần tự hóa: lần sau
  thấy `COMMITTED` và trả lại kết quả cũ (200, không nhập lại) — không phải 409.
- Hai bản xem trước của cùng một file (2 người / 2 tab) xác nhận đồng thời, cả hai đã phân loại đơn là NEW →
  một lần 200, lần kia đụng unique `order.platform_order_sn` → `409 IMPORT_CONFLICT`, rollback toàn bộ;
  không đơn / kiện nào bị nhập trùng.
"""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings
from aicam.main import create_app
from aicam.modules.imports import service as imports_service
from aicam.modules.orders.models import Order, Package
from aicam.modules.users.models import User

pytestmark = pytest.mark.integration

TABLES = 'csv_import, status_history, order_item, package, "order"'
CSV = (
    "platform_order_sn,tracking_number,sku,product_name,variation,quantity,buyer_note\n"
    "2410TSTCC001,SPXTSTCC00001,SKU1,Áo thun,Đen / L,1,\n"
    "2410TSTCC002,SPXTSTCC00002,SKU2,Quần jean,32,2,\n"
    "2410TSTCC003,SPXTSTCC00003,SKU3,Mũ,,1,Gói kỹ\n"
).encode()


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings, tmp_path: Path
) -> AsyncIterator[AsyncEngine]:
    test_settings.import_root = tmp_path / "imports"
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_impc_%'"))
    await dispose_engine()


async def _client_and_headers(test_settings: Settings) -> tuple[AsyncClient, dict[str, str]]:
    async with sessionmaker()() as db:
        user = User(username="tst_impc_sup", display_name="Sup", role="SUPERVISOR", password_hash="x")
        db.add(user)
        await db.commit()
        user_id = user.id
    app = create_app(test_settings)
    app.dependency_overrides[get_settings] = lambda: test_settings
    token, _ = encode_access_token(test_settings.jwt_secret, user_id, "SUPERVISOR", None, 15)
    client = AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver")
    return client, {"Authorization": f"Bearer {token}"}


async def _upload(client: AsyncClient, headers: dict[str, str]) -> str:
    res = await client.post("/api/v1/imports", headers=headers, files={"file": ("don.csv", CSV, "text/csv")})
    assert res.status_code == 201, res.text
    assert res.json()["counts"] == {"new": 3, "updated": 0, "skipped": 0, "error": 0}
    return str(res.json()["id"])


async def _counts() -> tuple[int, int]:
    async with sessionmaker()() as db:
        orders = await db.scalar(
            select(func.count()).select_from(Order).where(Order.platform_order_sn.like("2410TSTCC%"))
        )
        packages = await db.scalar(
            select(func.count()).select_from(Package).where(Package.tracking_number.like("SPXTSTCC%"))
        )
        return int(orders or 0), int(packages or 0)


async def test_two_previews_committed_at_once_one_conflicts(
    committed: AsyncEngine, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, headers = await _client_and_headers(test_settings)
    async with client:
        first, second = await _upload(client, headers), await _upload(client, headers)

        # Cả hai transaction phân loại xong (đơn đều NEW) rồi mới ghi — đua thật, không nhờ may rủi.
        barrier = asyncio.Barrier(2)
        original = imports_service.classify

        async def classify_then_wait(*args: Any, **kwargs: Any) -> Any:
            result = await original(*args, **kwargs)
            await asyncio.wait_for(barrier.wait(), timeout=10)
            return result

        monkeypatch.setattr(imports_service, "classify", classify_then_wait)

        async def commit(import_id: str) -> Response:
            return await client.post(f"/api/v1/imports/{import_id}/commit", headers=headers)

        results = await asyncio.wait_for(asyncio.gather(commit(first), commit(second)), timeout=30)

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 409], [r.text for r in results]
    ok = next(r for r in results if r.status_code == 200).json()
    conflict = next(r for r in results if r.status_code == 409).json()
    assert ok["counts"] == {"new": 3, "updated": 0, "skipped": 0, "error": 0}
    assert conflict["error"]["code"] == "IMPORT_CONFLICT"
    assert await _counts() == (3, 3)  # không nhập trùng đơn / kiện, lần lỗi không để lại phần nào
    async with sessionmaker()() as db:
        statuses_db = (
            (await db.execute(text("SELECT status FROM csv_import ORDER BY status"))).scalars().all()
        )
    assert statuses_db == ["COMMITTED", "PREVIEW"]  # lần bị 409 vẫn là bản xem trước, bấm Nhập lại được


async def test_same_preview_committed_twice_at_once_is_idempotent(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    client, headers = await _client_and_headers(test_settings)
    async with client:
        import_id = await _upload(client, headers)
        results = await asyncio.wait_for(
            asyncio.gather(
                *(client.post(f"/api/v1/imports/{import_id}/commit", headers=headers) for _ in range(2))
            ),
            timeout=30,
        )

    assert [r.status_code for r in results] == [200, 200], [r.text for r in results]
    assert results[0].json() == results[1].json()
    assert results[0].json()["counts"]["new"] == 3
    assert await _counts() == (3, 3)
