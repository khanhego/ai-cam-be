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


# ---------------------------------------------------------------- G3-N5, P2-3, P2-9


def _xlsx(rows: list[list[object]]) -> bytes:
    import io

    from openpyxl import Workbook

    book = Workbook()
    sheet = book.active
    assert sheet is not None
    for r in rows:
        sheet.append(r)
    buf = io.BytesIO()
    book.save(buf)
    return buf.getvalue()


def _rewrite_entry(content: bytes, name: str, transform: object) -> bytes:
    import io
    import zipfile

    src, out = zipfile.ZipFile(io.BytesIO(content)), io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            data = src.read(info.filename)
            if info.filename == name:
                data = transform(data)  # type: ignore[operator]
            dst.writestr(info.filename, data)
    return out.getvalue()


def test_xlsx_zip_bomb_rejected() -> None:
    """Zip vài chục KB bung ra 60 MB → 422 FILE_INVALID trước khi openpyxl đọc."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", "<Types/>")
        z.writestr("xl/worksheets/sheet1.xml", b"\0" * (60 * 1024 * 1024))
    bomb = buf.getvalue()
    assert len(bomb) < parser.MAX_BYTES
    with pytest.raises(AppError) as err:
        parser.parse(bomb, ".xlsx", REGEX)
    assert (err.value.status_code, err.value.code) == (422, "FILE_INVALID")
    assert "bất thường" in err.value.message


def test_xlsx_huge_dimension_is_ignored() -> None:
    """`<dimension ref="A1:XFD6001">` (16.384 cột) không làm openpyxl đệm ~98 triệu ô."""
    import time

    content = _xlsx([HEADER.strip().split(","), ["SN1", "SPX00000001", None, "Áo", None, 2, None]])
    content = _rewrite_entry(
        content,
        "xl/worksheets/sheet1.xml",
        lambda d: __import__("re").sub(rb'<dimension ref="[^"]*"', b'<dimension ref="A1:XFD6001"', d),
    )
    assert b"XFD6001" in __import__("zipfile").ZipFile(__import__("io").BytesIO(content)).read(
        "xl/worksheets/sheet1.xml"
    )
    assert max(len(r) for r in parser._xlsx_table(content)) <= parser.XLSX_MAX_COLS  # không đệm tới XFD
    started = time.monotonic()
    out = parser.parse(content, ".xlsx", REGEX)
    assert time.monotonic() - started < 2
    assert [(r.platform_order_sn, r.quantity) for r in out.rows] == [("SN1", 2)]


def test_csv_field_too_long_is_file_invalid() -> None:
    content = (HEADER + 'SN1,SPX00000001,,"' + "a" * 200_000 + '",,1,\n').encode()
    with pytest.raises(AppError) as err:
        parser.parse(content, ".csv", REGEX)
    assert (err.value.status_code, err.value.code) == (422, "FILE_INVALID")


def test_defusedxml_installed_for_openpyxl() -> None:
    from openpyxl.xml import DEFUSEDXML

    assert DEFUSEDXML is True
