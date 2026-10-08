"""Danh mục sự kiện N01..N10 (01 §7.5, FR-06.07, DEC-413) — nhãn, mức, kênh gợi ý, màn dashboard trong tin.

API-170 trả `events[]` từ đây (FE không chép cứng nhãn). `path` = đường dẫn màn admin (AS-16, DEC-488: tham
số sẵn có của D3 / D14 / D16 / D23 …) — tin ghép `https://{SITE_ADDRESS}{path}`.
"""

from dataclasses import dataclass
from typing import Literal

EventCode = Literal["N01", "N02", "N03", "N04", "N05", "N06", "N07", "N08", "N09", "N10"]
Severity = Literal["HIGH", "MEDIUM", "INFO"]
ChannelType = Literal["TELEGRAM", "ZALO_OA"]

CHANNEL_TYPE_LABELS: dict[str, str] = {"TELEGRAM": "Telegram", "ZALO_OA": "Zalo OA"}
SEVERITY_PREFIX: dict[str, str] = {"HIGH": "[CAO]", "MEDIUM": "[TB]", "INFO": "[TIN]"}
SEVERITY_RANK: dict[str, int] = {"INFO": 0, "MEDIUM": 1, "HIGH": 2}


@dataclass(frozen=True)
class EventDef:
    code: str
    label: str
    severity: str  # mức mặc định; N07 theo mức thực của sự kiện (80 % TB, 90 % Cao)
    suggested_channel: str
    path: str


EVENTS: tuple[EventDef, ...] = (
    EventDef("N01", "Camera mất tín hiệu", "HIGH", "Kho", "/admin/live"),
    EventDef("N02", "Lệch trạng thái mức Cao", "HIGH", "Kho", "/admin/recon?severity=HIGH"),
    EventDef("N03", "Phiên mở hoàn bị hủy / bỏ dở", "MEDIUM", "Kho", "/admin/packages?return_dropped=true"),
    EventDef(
        "N04", "Chỉ hoàn tiền mới / sắp hạn", "HIGH", "CSKH", "/admin/returns?tab=NO_PARCEL&pending_only=true"
    ),
    EventDef("N05", "Hồ sơ khiếu nại sắp / quá hạn", "HIGH", "CSKH", "/admin/claims?due=overdue"),
    EventDef("N06", "Shop hết hạn ủy quyền / đồng bộ lỗi", "HIGH", "Quản trị", "/admin/settings/platforms"),
    EventDef("N07", "Ổ lưu video sắp đầy", "MEDIUM", "Quản trị", "/admin/settings/storage"),
    EventDef("N08", "Sao lưu cloud trễ / lỗi", "HIGH", "Quản trị", "/admin/settings/backup"),
    EventDef("N09", "Yêu cầu duyệt chờ lâu", "MEDIUM", "Kho", "/admin/approvals"),
    EventDef("N10", "Tóm tắt ngày", "INFO", "Chủ shop", "/admin"),
)
BY_CODE: dict[str, EventDef] = {e.code: e for e in EVENTS}
CODES: tuple[str, ...] = tuple(e.code for e in EVENTS)


def label(code: str) -> str:
    event = BY_CODE.get(code)
    return event.label if event else code
