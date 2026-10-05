"""clip: deleted_at, timeline (T-14, DEC-102)

Chỉ thêm cột nullable — tương thích ngược (02a §3 "từ migration thứ 2").

- `deleted_at`: thời điểm retention xóa file (API-40/31 báo "Clip đã bị xóa ngày …").
- `timeline`: bảng ánh xạ giây trong clip → giờ thực của từng segment nguồn, để overlay giờ trên bản xuất
  đúng cả khi clip có khoảng thiếu (camera rớt) — concat demuxer gộp khe hở làm lệch giờ nếu chỉ dùng `start_at`.

Revision ID: 0002
Revises: 0001
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: Union[str, Sequence[str], None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("clip", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("clip", sa.Column("timeline", postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade() -> None:
    op.drop_column("clip", "timeline")
    op.drop_column("clip", "deleted_at")
