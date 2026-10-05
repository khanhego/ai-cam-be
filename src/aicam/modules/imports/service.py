"""Nhập đơn từ file CSV / Excel — API-50..54 (FR-05.09, 05.10, BR-17, EX-P11) + phần dọn dẹp của J-11.

File gốc lưu dưới `IMPORT_ROOT`, cột `file_path` giữ đường dẫn **tương đối** (DEC-105).
"""

import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import commit, rollback
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.pagination import Page
from aicam.core.settings import Settings
from aicam.modules.imports import parser
from aicam.modules.imports.models import CsvImport
from aicam.modules.imports.schemas import (
    Counts,
    ImportCommitOut,
    ImportItem,
    ImportPreviewOut,
    RowErrorOut,
    SampleRow,
    UserBrief,
)
from aicam.modules.orders import service as orders
from aicam.modules.platforms.base import PlatformItem
from aicam.modules.users.queries import get_user_ref

FILE_KEEP_DAYS = 90  # API-54: file gốc giữ 90 ngày
PREVIEW_MINUTES = 30
SAMPLE_ROWS = 20
MAX_ERRORS_KEPT = 1000  # lưu / trả tối đa 1.000 lỗi đầu; `counts.error` vẫn là tổng


@dataclass
class _Group:
    """Các dòng cùng một mã đơn."""

    rows: list[parser.Row] = field(default_factory=list)
    action: str = "NEW"


@dataclass
class Classified:
    groups: OrderedDict[str, _Group]
    errors: list[parser.RowError]

    @property
    def counts(self) -> Counts:
        c = Counts(error=len({e.row for e in self.errors}))
        for g in self.groups.values():
            if g.action == "NEW":
                c.new += 1
            elif g.action == "UPDATE":
                c.updated += 1
            else:
                c.skipped += 1
        return c


async def classify(session: AsyncSession, parsed: parser.Parsed) -> Classified:
    """Đơn chưa có → NEW; đơn nguồn CSV → UPDATE; đơn nguồn API → SKIP (BR-17).

    Lỗi thêm ngoài lỗi ô: một mã vận đơn thuộc 2 đơn trong file, hoặc đã thuộc đơn khác trong hệ thống.
    """
    errors = list(parsed.errors)
    groups: OrderedDict[str, _Group] = OrderedDict()
    for row in parsed.rows:
        groups.setdefault(row.platform_order_sn, _Group()).rows.append(row)

    existing = await orders.orders_by_sn(session, list(groups))
    packages = await orders.packages_by_code(session, list({r.tracking_number for r in parsed.rows}))
    owner_in_file: dict[str, tuple[str, int]] = {}
    for sn, group in groups.items():
        order = existing.get(sn)
        group.action = "NEW" if order is None else ("SKIP" if order.source == "API" else "UPDATE")
        if group.action == "SKIP":
            continue
        for row in group.rows:
            first = owner_in_file.setdefault(row.tracking_number, (sn, row.row))
            if first[0] != sn:
                errors.append(
                    parser.RowError(
                        row.row, "tracking_number", f"Mã vận đơn đã dùng cho đơn {first[0]} ở dòng {first[1]}"
                    )
                )
                continue
            known = packages.get(row.tracking_number)
            if known is not None and known[1] is not None and known[1] != sn:
                errors.append(
                    parser.RowError(row.row, "tracking_number", f"Mã vận đơn đã thuộc đơn {known[1]}")
                )
    errors.sort(key=lambda e: (e.row, parser.COLUMNS.index(e.column) if e.column in parser.COLUMNS else 99))
    return Classified(groups, errors)


def _sample(result: Classified) -> list[SampleRow]:
    rows = sorted(
        ((row, g.action) for g in result.groups.values() for row in g.rows), key=lambda item: item[0].row
    )
    return [
        SampleRow(
            row=r.row,
            tracking_number=r.tracking_number,
            platform_order_sn=r.platform_order_sn,
            product_name=r.product_name,
            variation=r.variation,
            quantity=r.quantity,
            action=action,
        )
        for r, action in rows[:SAMPLE_ROWS]
    ]


def _abs(root: Path, rel: str) -> Path:
    path = Path(rel)
    return path if path.is_absolute() else root / path


def _safe_name(file_name: str) -> str:
    name = file_name.replace("\\", "/").rsplit("/", 1)[-1].strip() or "file"
    return name[:200]


async def upload(
    session: AsyncSession, *, file_name: str, content: bytes, p: Principal, settings: Settings
) -> ImportPreviewOut:
    """API-50: kiểm định dạng → phân loại → lưu file gốc + bản xem trước (hạn 30 phút)."""
    name = _safe_name(file_name)
    ext = parser.extension_of(name)
    parsed = parser.parse(content, ext, settings.scan_code_regex)
    result = await classify(session, parsed)

    now = clock.now()
    row = CsvImport(
        status="PREVIEW",
        file_name=name,
        counts=result.counts.model_dump(),
        errors=[e.as_dict() for e in result.errors[:MAX_ERRORS_KEPT]],
        preview_rows=[s.model_dump() for s in _sample(result)],
        created_by=p.user_id,
        created_at=now,
        expires_at=now + timedelta(minutes=PREVIEW_MINUTES),
    )
    session.add(row)
    await session.flush()
    rel = Path(f"{now:%Y}") / f"{now:%m}" / f"{row.id}{ext}"
    target = settings.import_root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    row.file_path = rel.as_posix()
    try:
        await commit(session)
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return _preview_out(row)


def _preview_out(row: CsvImport) -> ImportPreviewOut:
    return ImportPreviewOut(
        id=row.id,
        status=row.status,
        file_name=row.file_name,
        counts=Counts(**row.counts),
        errors=[RowErrorOut(**e) for e in row.errors],
        sample=[SampleRow(**s) for s in row.preview_rows],
        expires_at=row.expires_at,
    )


def _has_errors() -> AppError:
    return AppError(
        "IMPORT_HAS_ERRORS", "File có dòng lỗi. Sửa file rồi tải lại; chưa có đơn nào được nhập.", 409
    )


def _expired() -> AppError:
    return AppError("IMPORT_EXPIRED", "Bản xem trước đã hết hạn. Tải file lại.", 409)


async def commit_import(
    session: AsyncSession, import_id: uuid.UUID, p: Principal, settings: Settings
) -> ImportCommitOut:
    """API-51: chỉ người tạo; một transaction; đọc lại file gốc và phân loại lại (dữ liệu có thể đã đổi
    trong 30 phút xem trước — vd đồng bộ Shopee vừa tạo đơn → dòng đó thành SKIP, BR-17)."""
    row = await session.scalar(select(CsvImport).where(CsvImport.id == import_id).with_for_update())
    if row is None:
        raise AppError("NOT_FOUND", "Không tìm thấy lần nhập.", 404)
    if row.created_by != p.user_id:
        raise AppError("FORBIDDEN", "Chỉ người tải file lên mới xác nhận nhập được.", 403)
    if row.status == "COMMITTED":  # bấm Nhập 2 lần: trả lại kết quả cũ
        return ImportCommitOut(id=row.id, status=row.status, counts=Counts(**row.counts))
    if row.status != "PREVIEW":
        raise _expired()
    if row.expires_at <= clock.now():
        row.status = "EXPIRED"
        await commit(session)
        raise _expired()
    if row.counts.get("error", 0) > 0:
        raise _has_errors()
    path = _abs(settings.import_root, row.file_path or "")
    if not row.file_path or not path.is_file():
        raise _expired()

    parsed = parser.parse(path.read_bytes(), parser.extension_of(row.file_name), settings.scan_code_regex)
    result = await classify(session, parsed)
    if result.errors:
        row.counts = result.counts.model_dump()
        row.errors = [e.as_dict() for e in result.errors[:MAX_ERRORS_KEPT]]
        await commit(session)
        raise _has_errors()

    counts = Counts()
    try:
        # Khóa mọi mã đơn sẽ ghi một lần, theo thứ tự (G3-F4) — tránh khóa chéo với J-04 / quét.
        await orders.lock_orders(session, [sn for sn, g in result.groups.items() if g.action != "SKIP"])
        for sn, group in result.groups.items():
            if group.action == "SKIP":
                counts.skipped += 1
                continue
            first_note = next((r.buyer_note for r in group.rows if r.buyer_note), None)
            data = orders.CsvOrder(
                platform_order_sn=sn,
                buyer_note=first_note,
                items=tuple(PlatformItem(r.product_name, r.quantity, r.sku, r.variation) for r in group.rows),
                tracking_numbers=tuple(dict.fromkeys(r.tracking_number for r in group.rows)),
            )
            created = await orders.apply_csv_order(
                session,
                data,
                import_id=row.id,
                shop_id=None,
                actor_user_id=p.user_id,
                expect_new=group.action == "NEW",
            )
            if created is None:
                counts.skipped += 1
            elif created:
                counts.new += 1
            else:
                counts.updated += 1
    except (IntegrityError, orders.CsvWriteConflict) as exc:
        # Đồng bộ Shopee / quét tạo cùng đơn hoặc mã vận đơn, hoặc kiện vừa được gắn vào đơn khác (G3-F5)
        # đúng lúc nhập: không nhập phần nào; bấm Nhập lại → phân loại lại báo lỗi dòng.
        await rollback(session)
        raise AppError(
            "IMPORT_CONFLICT", "Dữ liệu đơn vừa thay đổi trong lúc nhập. Bấm Nhập lại.", 409
        ) from exc
    row.status = "COMMITTED"
    row.committed_at = clock.now()
    row.counts = counts.model_dump()
    audit.record(
        session, "IMPORT_COMMIT", user_id=p.user_id, object_type="CSV_IMPORT", object_id=row.id, ip=p.ip,
        data={"file_name": row.file_name, "counts": row.counts},
    )  # fmt: skip
    await commit(session)
    return ImportCommitOut(id=row.id, status=row.status, counts=counts)


async def history(session: AsyncSession, page: int, page_size: int) -> Page[ImportItem]:
    """API-52: mới nhất trước."""
    total = int(await session.scalar(select(func.count()).select_from(CsvImport)) or 0)
    rows = (
        await session.scalars(
            select(CsvImport)
            .order_by(CsvImport.created_at.desc(), CsvImport.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    users: dict[uuid.UUID, UserBrief | None] = {}
    items = []
    for r in rows:
        if r.created_by not in users:
            ref = await get_user_ref(session, r.created_by)
            users[r.created_by] = UserBrief(id=ref.id, display_name=ref.display_name) if ref else None
        items.append(
            ImportItem(
                id=r.id,
                status=r.status,
                file_name=r.file_name,
                counts=Counts(**r.counts),
                created_by=users[r.created_by],
                created_at=r.created_at,
                committed_at=r.committed_at,
            )
        )
    return Page(items=items, page=page, page_size=page_size, total=total)


async def original_file(session: AsyncSession, import_id: uuid.UUID, settings: Settings) -> tuple[Path, str]:
    """API-54: file gốc; quá 90 ngày hoặc đã bị J-11 xóa → 410 FILE_EXPIRED."""
    row = await session.get(CsvImport, import_id)
    if row is None:
        raise AppError("NOT_FOUND", "Không tìm thấy lần nhập.", 404)
    expired = AppError("FILE_EXPIRED", f"File gốc chỉ giữ {FILE_KEEP_DAYS} ngày và đã bị xóa.", 410)
    if row.file_path is None or row.created_at < clock.now() - timedelta(days=FILE_KEEP_DAYS):
        raise expired
    path = _abs(settings.import_root, row.file_path)
    if not path.is_file():
        raise expired
    return path, row.file_name


TEMPLATE = Path(__file__).with_name("template.csv")


# ---------------------------------------------------------------- J-11


async def expire_previews(session: AsyncSession) -> int:
    """Bản xem trước quá `expires_at` (30 phút) → EXPIRED."""
    result = await session.execute(
        update(CsvImport)
        .where(CsvImport.status == "PREVIEW", CsvImport.expires_at < clock.now())
        .values(status="EXPIRED")
    )
    return int(result.rowcount or 0)  # type: ignore[attr-defined]


async def purge_old_files(session: AsyncSession, root: Path) -> int:
    """File gốc > 90 ngày → xóa, `file_path` = null. Đường dẫn tương đối tính từ `IMPORT_ROOT` (DEC-105)."""
    rows = (
        await session.scalars(
            select(CsvImport).where(
                CsvImport.file_path.is_not(None),
                CsvImport.created_at < clock.now() - timedelta(days=FILE_KEEP_DAYS),
            )
        )
    ).all()
    for row in rows:
        _abs(root, row.file_path or "").unlink(missing_ok=True)
        row.file_path = None
    return len(rows)
