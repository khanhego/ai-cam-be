"""Sàn / shop trên item danh sách và bộ lọc `platform`, `shop_id` dùng chung (02 §6.2 API-30 / 110 / 120 /
130 — FR-07.01, T-215). `null` = kiện / đơn chưa gắn shop."""

import uuid
from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel
from sqlalchemy import ColumnElement, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.orders.models import Shop

PlatformCode = Literal["SHOPEE", "TIKTOK"]


class ShopRef(BaseModel):
    id: uuid.UUID
    name: str | None


async def shops_by_id(db: AsyncSession, ids: Iterable[uuid.UUID | None]) -> dict[uuid.UUID, Shop]:
    wanted = sorted({i for i in ids if i is not None})
    if not wanted:
        return {}
    return {s.id: s for s in (await db.scalars(select(Shop).where(Shop.id.in_(wanted)))).all()}


def shop_ref(shop: Shop | None) -> ShopRef | None:
    return ShopRef(id=shop.id, name=shop.name) if shop else None


def shop_conditions(
    shop_col: Any, platform: str | None, shop_id: uuid.UUID | None
) -> list[ColumnElement[bool]]:
    """`shop_col` = cột / biểu thức cho id shop của dòng (vd `Order.shop_id`)."""
    out: list[ColumnElement[bool]] = []
    if shop_id is not None:
        out.append(shop_col == shop_id)
    if platform is not None:
        out.append(shop_col.in_(select(Shop.id).where(Shop.platform == platform)))
    return out
