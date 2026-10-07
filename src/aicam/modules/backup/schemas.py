"""Schema API-180..188 (02 §6.2 "sao lưu")."""

from pydantic import BaseModel


class TestOut(BaseModel):
    """API-183: ghi → đọc → xóa một đối tượng 1 KB dưới `backup/_probe/` (≤ 10 giây)."""

    ok: bool
    elapsed_ms: int
