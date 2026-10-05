"""Đọc file nhập đơn (API-50, EX-P11) — không cần DB."""

import pytest

from aicam.core.errors import AppError
from aicam.modules.imports import parser

REGEX = r"^[A-Z0-9-]{8,40}$"
HEADER = "platform_order_sn,tracking_number,sku,product_name,variation,quantity,buyer_note\n"


def test_reads_rows_upper_tracking_and_skips_blank_lines() -> None:
    out = parser.parse((HEADER + "SN1,spx00000001,,Áo,Đen,2,\n,,,,,,\n").encode(), ".csv", REGEX)
    assert out.errors == []
    assert [(r.row, r.tracking_number, r.quantity, r.variation, r.sku) for r in out.rows] == [
        (2, "SPX00000001", 2, "Đen", None)
    ]


def test_cell_errors_reported_per_row_and_column() -> None:
    csv = HEADER + "SN1,SPX00000001,,,,0,\nSN2,SP X,,Áo,,1,\n"
    out = parser.parse(csv.encode(), ".csv", REGEX)
    assert [(e.row, e.column, e.message) for e in out.errors] == [
        (2, "product_name", "Bỏ trống"),
        (2, "quantity", "Phải là số nguyên từ 1 đến 10000"),
        (3, "tracking_number", "Mã vận đơn không hợp lệ"),
    ]
    assert out.rows == []


@pytest.mark.parametrize(
    ("content", "ext", "detail"),
    [
        (b"sku,product_name\nx,y\n", ".csv", "missing_columns"),
        (HEADER.encode(), ".csv", "max_rows"),
        ("platform_order_sn\n".encode("utf-16"), ".csv", "encoding"),
        (b"not a zip", ".xlsx", None),
    ],
)
def test_whole_file_invalid(content: bytes, ext: str, detail: str | None) -> None:
    with pytest.raises(AppError) as exc:
        parser.parse(content, ext, REGEX)
    assert exc.value.code == "FILE_INVALID"
    if detail:
        assert detail in exc.value.details


def test_extension() -> None:
    assert parser.extension_of("Đơn.XLSX") == ".xlsx"
    with pytest.raises(AppError):
        parser.extension_of("don.xls")
