"""API-153 CSV tab báo cáo (FR-09.06, DEC-476): UTF-8 có BOM, dấu phẩy, mọi bảng của tab.

Mỗi bảng mở đầu bằng một dòng tiêu đề tiếng Việt, cách nhau một dòng trống; tỷ lệ dạng chuỗi `4,0%`, số tiền
nguyên (đồng)."""

import csv
import io
from collections.abc import Sequence
from datetime import date
from typing import Any

from pydantic import BaseModel

from aicam.modules.claims.service import CONCLUSION_LABELS
from aicam.modules.claims.service import STATUS_LABELS as CLAIM_STATUS_LABELS
from aicam.modules.reports import schemas as s

BOM = "\ufeff"  # Excel nhận UTF-8
FILE_PREFIX = {
    "returns": "bao-cao-hang-hoan",
    "claims": "bao-cao-khieu-nai",
    "productivity": "bao-cao-nang-suat",
}
TITLES = {"returns": "Báo cáo hàng hoàn", "claims": "Báo cáo khiếu nại", "productivity": "Báo cáo năng suất"}
PLATFORM_LABELS = {"SHOPEE": "Shopee", "TIKTOK": "TikTok Shop"}
KIND_LABELS = {
    "BUYER_RETURN": "Khách trả hàng",
    "FAILED_DELIVERY": "Giao thất bại",
    "UNANNOUNCED": "Về trước khi sàn báo",
    "UNIDENTIFIED": "Chưa xác định",
    "REFUND_ONLY": "Chỉ hoàn tiền",
}
CLAIM_TYPE_LABELS = {
    **CONCLUSION_LABELS,
    "BUYER_CLAIM": "Khách báo thiếu / sai",
    "LOST_IN_TRANSIT": "Thất lạc",
}
COUNTERPARTY_LABELS = {"PLATFORM": "Sàn", "CARRIER": "ĐVVC"}
GRANULARITY_LABELS = {"day": "Theo ngày", "week": "Theo tuần", "month": "Theo tháng"}
NO_NAME = "(Không ghi tên)"
NO_SHOP = "(Không có shop)"

Rows = list[Sequence[Any]]


def percent(value: float | None) -> str:
    """`0.04` → `4,0%`; không có → `—` (EX-B2)."""
    if value is None:
        return "—"
    return f"{value * 100:.1f}".replace(".", ",") + "%"


def _num(value: int | None) -> str | int:
    return "—" if value is None else value


def _vn(day: date) -> str:
    return day.strftime("%d/%m/%Y")


def filename(report: str, f: Any) -> str:
    return f"{FILE_PREFIX[report]}-{f.from_.isoformat()}_{f.to.isoformat()}.csv"


def _head(report: str, out: BaseModel, shop_name: str | None, station_name: str | None) -> Rows:
    period = out.period  # type: ignore[attr-defined]
    filters = out.filters  # type: ignore[attr-defined]
    rows: Rows = [
        [TITLES[report]],
        ["Kỳ", f"{_vn(period.from_)} – {_vn(period.to)}"],
        ["Sàn", PLATFORM_LABELS.get(filters.platform or "", "Tất cả")],
        ["Shop", shop_name or "Tất cả"],
    ]
    if report == "productivity":
        rows.append(["Station", station_name or "Tất cả"])
    return rows


def _shop_cells(platform: str | None, name: str | None) -> list[str]:
    return [PLATFORM_LABELS.get(platform or "", ""), name or NO_SHOP]


def _ratio_row(label: str, r: s.Ratio) -> list[Any]:
    return [label, percent(r.value), r.numerator, r.denominator]


def _series(out: s.ReturnsReportOut | s.ClaimsReportOut) -> list[tuple[str, Rows]]:
    if not out.series:
        return []
    return [
        (
            f"Biểu đồ — {GRANULARITY_LABELS[out.series_granularity].lower()}",
            [["Từ ngày", "Kiện đóng gói", "Hồ sơ hàng hoàn", "Hồ sơ khiếu nại"]]
            + [[_vn(r.bucket), r.packed, r.return_cases, r.claims] for r in out.series],
        )
    ]


def _returns_tables(out: s.ReturnsReportOut) -> list[tuple[str, Rows]]:
    c = out.cards
    conclusions = out.reason_by_conclusion.conclusions
    return [
        (
            "Chỉ số",
            [
                ["Chỉ số", "Giá trị", "Tử số", "Mẫu số"],
                _ratio_row("Tỷ lệ hoàn", c.return_rate),
                _ratio_row("Tỷ lệ có vấn đề", c.issue_rate),
                ["Chỉ hoàn tiền", c.refund_only.count, "", ""],
                ["Chỉ hoàn tiền / kiện bàn giao", percent(c.refund_only.rate_of_handed_over), "", ""],
                ["Đang về (hiện tại)", c.expected_now, "", ""],
            ],
        ),
        (
            "Theo loại hồ sơ",
            [["Loại", "Số hồ sơ", "Tỷ lệ"]]
            + [[KIND_LABELS.get(r.kind, r.kind), r.count, percent(r.share)] for r in out.by_kind],
        ),
        (
            "Lý do khách × kết luận kho",
            [["Lý do", *[CONCLUSION_LABELS.get(k, k) for k in conclusions], "Tổng"]]
            + [
                [r.reason_label, *[r.counts.get(k, 0) for k in conclusions], r.total]
                for r in out.reason_by_conclusion.rows
            ],
        ),
        (
            "Top sản phẩm bị trả",
            [["SKU", "Sản phẩm", "Phân loại", "Đã gửi", "Yêu cầu trả", "Tỷ lệ", "Có vấn đề"]]
            + [
                [
                    r.sku or "",
                    r.product_name,
                    r.variation or "",
                    r.shipped,
                    r.return_requests,
                    percent(r.rate),
                    r.issue,
                ]
                for r in out.top_products
            ],
        ),
        (
            "Theo sàn / shop",
            [["Sàn", "Shop", "Kiện bàn giao", "Hồ sơ hàng hoàn", "Tỷ lệ"]]
            + [
                [*_shop_cells(r.platform, r.shop_name), r.handed_over, r.return_cases, percent(r.rate)]
                for r in out.by_shop
            ],
        ),
        *_series(out),
    ]


def _claims_tables(out: s.ClaimsReportOut) -> list[tuple[str, Rows]]:
    c = out.cards
    return [
        (
            "Chỉ số",
            [
                ["Chỉ số", "Giá trị", "Tử số", "Mẫu số"],
                ["Hồ sơ tạo trong kỳ", c.created, "", ""],
                _ratio_row("Tỷ lệ thắng", c.win_rate),
                ["Giá trị thu hồi (đ)", c.recovered_amount, "", ""],
                _ratio_row("Gửi trước hạn", c.submitted_before_deadline),
                ["Quá hạn chưa gửi (hiện tại)", c.overdue_unsent_now, "", ""],
            ],
        ),
        (
            "Theo trạng thái",
            [["Trạng thái", "Số hồ sơ"]]
            + [[CLAIM_STATUS_LABELS.get(r.status, r.status), r.count] for r in out.by_status],
        ),
        (
            "Theo loại × kết quả",
            [["Loại", "Thắng", "Thua", "Đang xử lý"]]
            + [[CLAIM_TYPE_LABELS.get(r.type, r.type), r.won, r.lost, r.pending] for r in out.by_type_result],
        ),
        (
            "Theo bên nhận",
            [["Bên nhận", "Số hồ sơ", "Thắng", "Thua", "Giá trị thu hồi (đ)"]]
            + [
                [
                    COUNTERPARTY_LABELS.get(r.counterparty, r.counterparty),
                    r.count,
                    r.won,
                    r.lost,
                    r.recovered_amount,
                ]
                for r in out.by_counterparty
            ],
        ),
        (
            "Theo sàn / shop",
            [["Sàn", "Shop", "Số hồ sơ", "Thắng", "Thua", "Giá trị thu hồi (đ)"]]
            + [
                [*_shop_cells(r.platform, r.shop_name), r.count, r.won, r.lost, r.recovered_amount]
                for r in out.by_shop
            ],
        ),
        *_series(out),
    ]


def _productivity_tables(out: s.ProductivityReportOut) -> list[tuple[str, Rows]]:
    c = out.cards
    cols = ["Số kiện", "TB / kiện (giây)", "Lệch mã", "Bỏ dở", "Hủy", "Đóng gói lại"]

    def pack(r: s.StationRow | s.OperatorRow) -> list[Any]:
        return [r.packed, _num(r.avg_seconds), r.mismatch, r.abandoned, r.cancelled, r.repacked]

    return [
        (
            "Chỉ số",
            [
                ["Chỉ số", "Giá trị"],
                ["Kiện đã đóng gói", c.packed],
                ["TB / kiện (giây)", _num(c.pack_avg_seconds)],
                ["Kiện hoàn đã kiểm", c.returns_inspected],
                ["TB / kiện hoàn (giây)", _num(c.return_avg_seconds)],
            ],
        ),
        ("Theo station", [["Station", *cols]] + [[r.station_name, *pack(r)] for r in out.by_station]),
        (
            "Theo người đứng bàn",
            [["Người đứng bàn", *cols]] + [[r.operator_name or NO_NAME, *pack(r)] for r in out.by_operator],
        ),
        (
            "Bàn hoàn theo người kiểm",
            [["Người kiểm", "Số kiện", "TB / kiện (giây)", "Có vấn đề", "Tỷ lệ có vấn đề"]]
            + [
                [
                    r.operator_name or NO_NAME,
                    r.inspected,
                    _num(r.avg_seconds),
                    r.issue_rate.numerator,
                    percent(r.issue_rate.value),
                ]
                for r in out.return_by_operator
            ],
        ),
    ]


def render(
    report: str, out: BaseModel, *, shop_name: str | None = None, station_name: str | None = None
) -> bytes:
    if isinstance(out, s.ReturnsReportOut):
        tables = _returns_tables(out)
    elif isinstance(out, s.ClaimsReportOut):
        tables = _claims_tables(out)
    elif isinstance(out, s.ProductivityReportOut):
        tables = _productivity_tables(out)
    else:  # pragma: no cover — REPORTS chỉ có 3 loại
        raise TypeError(type(out))
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerows(_head(report, out, shop_name, station_name))
    for title, rows in tables:
        writer.writerow([])
        writer.writerow([title])
        writer.writerows(rows)
    return (BOM + buf.getvalue()).encode("utf-8")
