"""Phân trang chung: `?page=1&page_size=20` (tối đa 100) — 02 §6."""

from pydantic import BaseModel, Field


class PageParams(BaseModel):
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class Page[T](BaseModel):
    items: list[T]
    page: int
    page_size: int
    total: int
