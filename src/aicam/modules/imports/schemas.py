"""Schema API-50..54 (02 §6 "API-50 / API-51", "Schema các API còn lại")."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

RowAction = Literal["NEW", "UPDATE", "SKIP"]


class Counts(BaseModel):
    new: int = 0
    updated: int = 0
    skipped: int = 0
    error: int = 0


class RowErrorOut(BaseModel):
    row: int
    column: str
    message: str


class SampleRow(BaseModel):
    row: int
    tracking_number: str
    platform_order_sn: str
    product_name: str
    variation: str | None
    quantity: int
    action: RowAction


class ImportPreviewOut(BaseModel):
    id: uuid.UUID
    status: str
    file_name: str
    counts: Counts
    errors: list[RowErrorOut]
    sample: list[SampleRow]
    expires_at: datetime


class ImportCommitOut(BaseModel):
    id: uuid.UUID
    status: str
    counts: Counts


class UserBrief(BaseModel):
    id: uuid.UUID
    display_name: str


class ImportItem(BaseModel):
    id: uuid.UUID
    status: str
    file_name: str
    counts: Counts
    created_by: UserBrief | None
    created_at: datetime
    committed_at: datetime | None
