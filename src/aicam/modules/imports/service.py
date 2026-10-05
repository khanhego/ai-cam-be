"""Nhập đơn CSV (T-17 làm API-50..54). Ở đây: phần dọn dẹp của J-11 (02a §7)."""

from datetime import timedelta
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.imports.models import CsvImport

FILE_KEEP_DAYS = 90  # API-54: file gốc giữ 90 ngày


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
        path = Path(row.file_path or "")
        (path if path.is_absolute() else root / path).unlink(missing_ok=True)
        row.file_path = None
    return len(rows)
