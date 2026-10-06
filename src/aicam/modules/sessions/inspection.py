"""Dòng kiểm + kết luận phiên hoàn (02 API-10 quy tắc khởi tạo, API-102; BR-22; 02a §2 `inspection.py`)."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.errors import AppError
from aicam.modules.orders.models import OrderItem
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import InspectionLine, PackSession
from aicam.modules.sessions.schemas import InspectionLineOut, InspectionOut

NOTE_MAX = 500
QUANTITY_MAX = 999


async def lines_of(session: AsyncSession, session_id: uuid.UUID) -> list[InspectionLine]:
    rows = await session.scalars(
        select(InspectionLine)
        .where(InspectionLine.session_id == session_id)
        .order_by(InspectionLine.position)
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


def _requested_by_item(case: ReturnCase) -> dict[str, int]:
    out: dict[str, int] = {}
    for line in case.requested_items or []:
        key = line.get("order_item_id")
        if key:
            out[str(key)] = out.get(str(key), 0) + int(line.get("quantity") or 0)
    return out


async def _received_before(session: AsyncSession, case_id: uuid.UUID, pack_id: uuid.UUID) -> dict[str, int]:
    """Số đã nhận ở các phiên `COMPLETED` khác của hồ sơ (dòng tham khảo — "phần chưa nhận", 02a §4.1)."""
    rows = (
        await session.execute(
            select(InspectionLine.order_item_id, func.sum(InspectionLine.quantity_received))
            .join(PackSession, PackSession.id == InspectionLine.session_id)
            .where(
                PackSession.return_case_id == case_id,
                PackSession.status == "COMPLETED",
                PackSession.id != pack_id,
                InspectionLine.order_item_id.is_not(None),
            )
            .group_by(InspectionLine.order_item_id)
        )
    ).all()
    return {str(item_id): int(total or 0) for item_id, total in rows}


async def init_lines(session: AsyncSession, pack: PackSession, case: ReturnCase, *, single: bool) -> str:
    """Khởi tạo dòng kiểm khi mở phiên (02 API-10) — trả `lines_mode`.

    - Không có đơn (chưa xác định) → không dòng, `FULL` (chỉ kết luận chung).
    - Hồ sơ nhiều phiên của đơn > 1 kiện → `REFERENCE`: dòng tham khảo, `quantity_requested` = phần chưa nhận.
    - Còn lại `FULL`: mọi dòng đơn; `quantity_requested` = số yêu cầu trả của dòng (0 nếu không trả); không có
      yêu cầu theo dòng (giao thất bại / về trước khi sàn báo) → = số đã gửi. `quantity_received` = yêu cầu,
      `condition = OK`.
    """
    items: Sequence[OrderItem] = []
    if case.order_id is not None:
        items = (
            await session.scalars(
                select(OrderItem).where(OrderItem.order_id == case.order_id).order_by(OrderItem.id)
            )
        ).all()
    multi = not single and await returns.order_package_count(session, case.order_id) > 1
    mode = "REFERENCE" if multi else "FULL"
    requested = _requested_by_item(case)
    received_before = await _received_before(session, case.id, pack.id) if multi else {}
    for position, item in enumerate(items, start=1):
        key = str(item.id)
        if multi:
            want = max(0, item.quantity - received_before.get(key, 0))
        elif requested:
            want = min(QUANTITY_MAX, requested.get(key, 0))
        else:
            want = item.quantity
        session.add(
            InspectionLine(
                session_id=pack.id,
                order_item_id=item.id,
                position=position,
                product_name=item.product_name,
                variation=item.variation,
                image_url=item.image_url,
                quantity_sent=min(QUANTITY_MAX, item.quantity),
                quantity_requested=min(QUANTITY_MAX, want),
                quantity_received=min(QUANTITY_MAX, want),
                condition="OK",
                note=None,
            )
        )
    pack.inspection_lines_mode = mode
    return mode


def inspection_out(pack: PackSession, lines: Sequence[InspectionLine]) -> InspectionOut:
    return InspectionOut(
        conclusion=pack.inspection_conclusion,
        note=pack.inspection_note or "",
        saved_at=pack.inspection_saved_at,
        lines_mode=pack.inspection_lines_mode or "FULL",
        lines=[
            InspectionLineOut(
                order_item_id=line.order_item_id,
                product_name=line.product_name,
                variation=line.variation,
                image_url=line.image_url,
                quantity_sent=line.quantity_sent,
                quantity_requested=line.quantity_requested,
                quantity_received=line.quantity_received,
                condition=line.condition,
                note=line.note,
            )
            for line in lines
        ],
    )


# ---------------------------------------------------------------- BR-22


@dataclass(frozen=True)
class LineInput:
    order_item_id: uuid.UUID | None
    quantity_received: int
    condition: str | None
    note: str | None


def _invalid(fields: dict[str, str]) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})


def validate(
    *,
    lines_mode: str,
    conclusion: str | None,
    note: str | None,
    current: Sequence[InspectionLine],
    lines: Sequence[LineInput],
) -> None:
    """BR-22 (02 API-102, 02a §5): `FULL` kiểm đủ dòng + nhất quán "Nguyên vẹn"; `REFERENCE` chỉ kết luận
    (+ ghi chú khi Khác), dòng gửi kèm chỉ kiểm khoảng giá trị.

    Lỗi định dạng → `422 VALIDATION_ERROR` (`details.fields`); "Nguyên vẹn" mâu thuẫn dòng →
    `422 CONCLUSION_INCONSISTENT`.
    """
    full = lines_mode != "REFERENCE"
    fields: dict[str, str] = {}
    if note is not None and len(note) > NOTE_MAX:
        fields["note"] = f"Ghi chú tối đa {NOTE_MAX} ký tự"
    if conclusion == "OTHER" and not (note and note.strip()):
        fields["note"] = "Nhập ghi chú khi chọn Khác"
    by_item = {line.order_item_id: line for line in current if line.order_item_id is not None}
    seen: set[uuid.UUID] = set()
    for index, line in enumerate(lines):
        if not 0 <= line.quantity_received <= QUANTITY_MAX:
            fields[f"lines.{index}.quantity_received"] = f"Số nhận 0–{QUANTITY_MAX}"
        if line.note is not None and len(line.note) > NOTE_MAX:
            fields[f"lines.{index}.note"] = f"Ghi chú tối đa {NOTE_MAX} ký tự"
        if not full:
            continue
        if line.order_item_id is None or line.order_item_id not in by_item:
            fields[f"lines.{index}.order_item_id"] = "Dòng không thuộc phiên"
            continue
        if line.order_item_id in seen:
            fields[f"lines.{index}.order_item_id"] = "Dòng bị lặp"
        seen.add(line.order_item_id)
        if line.condition == "OTHER" and not (line.note and line.note.strip()):
            fields[f"lines.{index}.note"] = "Nhập ghi chú khi chọn Khác"
    if full and set(by_item) - seen:
        fields["lines"] = "Thiếu dòng của phiên"
    if fields:
        raise _invalid(fields)
    if full and conclusion == "OK":
        for line in lines:
            original = by_item[line.order_item_id]  # type: ignore[index]
            if line.condition != "OK" or line.quantity_received != original.quantity_requested:
                raise AppError(
                    "CONCLUSION_INCONSISTENT",
                    "Có dòng thiếu / hỏng — chọn vấn đề, không chọn Nguyên vẹn.",
                    422,
                )


def replace_lines(current: Sequence[InspectionLine], lines: Sequence[LineInput]) -> None:
    """Ghi đè số nhận / tình trạng / ghi chú theo `order_item_id` (API-102 ghi đè toàn bộ)."""
    by_item = {line.order_item_id: line for line in lines if line.order_item_id is not None}
    for row in current:
        incoming = by_item.get(row.order_item_id) if row.order_item_id else None
        if incoming is None:
            continue
        row.quantity_received = incoming.quantity_received
        row.condition = incoming.condition
        row.note = incoming.note.strip() if incoming.note and incoming.note.strip() else None
