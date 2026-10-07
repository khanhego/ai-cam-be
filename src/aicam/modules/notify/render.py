"""Dựng nội dung tin (02 §6.2 "Mẫu tin", FR-06.09, BR-36 (2), DEC-472).

Dòng 1: `[CAO]` / `[TB]` / `[TIN]` + tên sự kiện (+ " — {n} mục" khi gom). Tối đa 10 dòng mục + "và {n} mục
khác". Dòng cuối "Xem: https://{SITE_ADDRESS}{màn}" (AS-16).

**Whitelist trường** (02a §3 "Dữ liệu nhạy cảm"): mỗi mã chỉ đọc đúng các khóa liệt kê dưới đây từ
`notify_event.data` — dữ liệu người mua (tên, SĐT, địa chỉ, ghi chú), lý do viết tay, số tiền, token, URL ký
không bao giờ có đường vào tin dù `data` có chứa.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from aicam.modules.notify import catalog

MAX_LINES = 10
PLATFORM_LABELS = {"SHOPEE": "Shopee", "TIKTOK": "TikTok"}
CAMERA_LABELS = {"CAM1": "Cam 1", "CAM2": "Cam 2"}
CLAIM_TYPE_LABELS = {
    "DAMAGED": "Hàng hỏng",
    "MISSING_ITEM": "Thiếu hàng",
    "WRONG_ITEM": "Sai hàng",
    "EMPTY_BOX": "Hộp rỗng",
    "OTHER": "Khác",
    "BUYER_CLAIM": "Người mua khiếu nại",
    "LOST_IN_TRANSIT": "Thất lạc vận chuyển",
}
APPROVAL_TYPE_LABELS = {"MISMATCH": "Lệch mã", "ASSIST": "Gọi quản lý", "REPACK": "Đóng gói lại"}
SHOP_ERROR_LABELS = {"AUTH_EXPIRED": "Hết hạn ủy quyền", "SYNC_FAILED": "Đồng bộ lỗi"}
N08_REASONS = ("DB_LATE", "DB_FAILED_TWICE", "EVIDENCE_LATE", "HASH_MISMATCH", "SOURCE_MISSING")
SUMMARY_TITLE = "Tóm tắt {n} thông báo"


def _parse(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


class _Fmt:
    def __init__(self, tz: str) -> None:
        self.zone = ZoneInfo(tz)

    def hm(self, value: Any) -> str:
        at = _parse(value)
        return at.astimezone(self.zone).strftime("%H:%M") if at else "?"

    def dm(self, value: Any) -> str:
        at = _parse(value)
        return at.astimezone(self.zone).strftime("%d/%m") if at else "?"

    def dmhm(self, value: Any) -> str:
        at = _parse(value)
        return at.astimezone(self.zone).strftime("%d/%m %H:%M") if at else "?"


def _s(data: Mapping[str, Any], key: str, default: str = "?") -> str:
    value = data.get(key)
    return str(value) if value not in (None, "") else default


def _platform(data: Mapping[str, Any]) -> str:
    return PLATFORM_LABELS.get(str(data.get("platform") or ""), "Chưa rõ sàn")


def _shop_part(data: Mapping[str, Any]) -> str:
    shop = data.get("shop")
    return f"{_platform(data)} · {shop}" if shop else _platform(data)


def item_line(code: str, data: Mapping[str, Any], f: _Fmt, recovered: Mapping[str, datetime]) -> str:
    """Một dòng mục (không có "• "). Chỉ đọc khóa trong whitelist của từng mã."""
    if code == "N01":
        role = CAMERA_LABELS.get(_s(data, "role"), _s(data, "role"))
        line = f"{_s(data, 'station')} · {role} · từ {f.hm(data.get('since'))}"
        back = recovered.get(str(data.get("camera_id")))
        return f"{line} (đã có lại {f.hm(back.isoformat())})" if back else line  # EX-N4
    if code == "N02":
        return f"{_s(data, 'tracking')} · {_shop_part(data)} · từ {f.dm(data.get('since'))}"
    if code == "N03":
        if data.get("kind") == "CASE":
            return f"{_s(data, 'tracking')} · Kiện hoàn chưa xác định · {f.hm(data.get('at'))}"
        status = "bỏ dở" if data.get("status") == "ABANDONED" else "hủy"
        return f"{_s(data, 'tracking')} · {_s(data, 'station')} · {status} {f.hm(data.get('at'))}"
    if code == "N04":
        stage = "mới" if data.get("stage") == "NEW" else "còn ≤ 12 giờ"
        return f"{_s(data, 'case_code')} · {_shop_part(data)} · hạn {f.dmhm(data.get('due'))} · {stage}"
    if code == "N05":
        stage = "quá hạn" if data.get("stage") == "OVERDUE" else "sắp hạn"
        kind = CLAIM_TYPE_LABELS.get(_s(data, "claim_type"), _s(data, "claim_type"))
        return f"{_s(data, 'claim_code')} · {kind} · hạn {f.dmhm(data.get('due'))} · {stage}"
    if code == "N06":
        if data.get("stage") == "EXPIRED":
            what = "Hết hạn ủy quyền — cần kết nối lại"
        else:
            what = (
                f"{SHOP_ERROR_LABELS.get(_s(data, 'error_code'), 'Đồng bộ lỗi')} từ {f.hm(data.get('since'))}"
            )
        return f"{_s(data, 'shop')} · {_platform(data)} · {what}"
    if code == "N07":
        return f"Ổ lưu video đã dùng {_s(data, 'percent')} %"
    if code == "N08":
        reason = _n08_lines([{"data": data}])
        return reason[0] if reason else catalog.label(code)
    if code == "N09":
        kind = APPROVAL_TYPE_LABELS.get(_s(data, "approval_type"), _s(data, "approval_type"))
        return f"{_s(data, 'station')} · {kind} · từ {f.hm(data.get('since'))}"
    return catalog.label(code)


def _n08_lines(items: Sequence[Mapping[str, Any]]) -> list[str]:
    """N08: một dòng / lý do (01 §7.5 v0.4) — gộp số tệp của cùng lý do."""
    by_reason: dict[str, list[Mapping[str, Any]]] = {}
    for item in items:
        by_reason.setdefault(str(item.get("data", {}).get("reason")), []).append(item.get("data", {}))
    lines = []
    for reason in N08_REASONS:
        rows = by_reason.get(reason)
        if not rows:
            continue
        if reason == "DB_LATE":
            hours = max(int(r.get("hours") or 0) for r in rows)
            lines.append(f"Sao lưu DB chưa thành công {hours} giờ")
        elif reason == "DB_FAILED_TWICE":
            count = max(int(r.get("count") or 2) for r in rows)
            lines.append(f"{count} lượt sao lưu DB liền không thành công")
        elif reason == "EVIDENCE_LATE":
            count = max(int(r.get("count") or 0) for r in rows)
            lines.append(f"{count} tệp bằng chứng chờ sao lưu quá 24 giờ")
        elif reason == "HASH_MISMATCH":
            lines.append(f"{len(rows)} tệp lệch mã băm")
        else:
            lines.append(f"{len(rows)} tệp không thấy tại kho")
    return lines


def n10_lines(data: Mapping[str, Any]) -> list[str]:
    """FR-06.11: số liệu như D2 hôm nay."""

    def n(key: str) -> int:
        try:
            return int(data.get(key) or 0)
        except (TypeError, ValueError):
            return 0

    return [
        f"Đã đóng gói: {n('packed')} · từng lệch mã: {n('had_mismatch')}",
        f"Hàng hoàn nhận: {n('returns_received')} · có vấn đề: {n('returns_received_issue')}",
        f"Hồ sơ khiếu nại mở: {n('claims_open')} · sắp hạn: {n('claims_due_soon')} · "
        f"quá hạn chưa gửi: {n('claims_overdue_unsent')}",
        f"Chỉ hoàn tiền chưa xử lý: {n('refund_only_pending')}",
    ]


def render(
    event_code: str,
    severity: str,
    items: Sequence[Mapping[str, Any]],
    *,
    tz: str,
    site_address: str,
    recovered: Mapping[str, datetime] | None = None,
) -> str:
    """Tin hoàn chỉnh từ danh sách mục `{code, severity, at, data}` của `notify_message.items`."""
    f = _Fmt(tz)
    rec = recovered or {}
    codes = Counter(str(i.get("code")) for i in items)
    summary = len(codes) > 1 or any(i.get("summary") for i in items)
    prefix = catalog.SEVERITY_PREFIX.get(severity, "[TB]")
    lines: list[str]
    if summary:  # thả tin HELD gộp nhiều mã (DEC-472)
        head = f"{prefix} " + SUMMARY_TITLE.format(n=len(items))
        lines = []
        for i in items:
            code = str(i.get("code"))
            lines.append(f"{catalog.label(code)}: {item_line(code, i.get('data', {}), f, rec)}")
        path = "/admin"
    elif event_code == "N10":
        data = items[-1].get("data", {}) if items else {}
        head = f"{prefix} {catalog.label('N10')} {f.dm(data.get('day_start'))}"
        lines = n10_lines(data)
        path = catalog.BY_CODE["N10"].path
    else:
        title = catalog.label(event_code)
        if event_code == "N08":
            lines = _n08_lines(items)
        else:
            lines = [item_line(event_code, i.get("data", {}), f, rec) for i in items]
        head = f"{prefix} {title}" + (f" — {len(lines)} mục" if len(lines) > 1 else "")
        path = catalog.BY_CODE[event_code].path if event_code in catalog.BY_CODE else "/admin"
    shown = [f"• {line}" for line in lines[:MAX_LINES]]
    if len(lines) > MAX_LINES:
        shown.append(f"và {len(lines) - MAX_LINES} mục khác")
    out = [head, *shown]
    site = site_address.strip().rstrip("/")
    if site:
        base = site if site.startswith(("http://", "https://")) else f"https://{site}"
        out.append(f"Xem: {base}{path}")
    return "\n".join(out)
