"""W1 — trang người nhận link (02 §6.3, FR-07.07, DEC-411, DEC-428, DEC-506).

HTML tĩnh một file: `string.Template` + `html.escape` cho **mọi** giá trị (DEC-470); không `<script>`, không
cookie; CSP `default-src 'none'` chỉ cho ảnh / video từ origin kho lưu + style inline; `referrer no-referrer`
(URL ký không lọt qua Referer). Chỉ trường whitelist ở `W1Context` — không có "gửi cho", người tạo, ghi chú,
số tiền, tên shop, dữ liệu người mua (NFR-45).
"""

import html
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from string import Template
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

TEMPLATE = Template(Path(__file__).with_name("templates").joinpath("w1.html").read_text(encoding="utf-8"))

PLATFORM_LABELS = {"SHOPEE": "Shopee", "TIKTOK": "TikTok Shop"}
TYPE_LABELS = {"PACK": "Đóng gói", "RETURN": "Mở hàng hoàn"}
CONCLUSION_LABELS = {
    "OK": "Nguyên vẹn",
    "DAMAGED": "Hư hỏng",
    "MISSING_ITEM": "Thiếu hàng",
    "WRONG_ITEM": "Sai hàng / bị tráo",
    "EMPTY_BOX": "Hộp rỗng",
    "OTHER": "Khác",
}
STATUS_NOTES = {"ABANDONED": "Phiên bị bỏ dở", "CANCELLED": "Đã hủy"}
NO_PLAYBACK = "Trình duyệt không phát được video. Bấm Tải video để xem bằng ứng dụng khác."


@dataclass(frozen=True)
class W1Photo:
    url: str


@dataclass(frozen=True)
class W1Session:
    number: int
    type: str  # PACK | RETURN
    status: str
    started_at: datetime
    ended_at: datetime | None
    station: str | None
    operator: str | None  # chỉ phiên RETURN
    conclusion: str | None  # chỉ phiên RETURN
    video_url: str
    download_url: str
    size_bytes: int
    video_sha256: str
    source_sha256: dict[str, str | None]
    photos: list[W1Photo] = field(default_factory=list)


@dataclass(frozen=True)
class W1Context:
    origin: str  # scheme + host kho lưu (CSP)
    tracking_number: str
    platform_order_sn: str | None
    platform: str | None
    expires_at: datetime
    tz: str
    sessions: list[W1Session]


def origin_of(url: str) -> str:
    """`https://host[:port]` của URL ký (CSP chỉ cho đúng origin này)."""
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        raise ValueError("URL ký không có scheme / host")
    return f"{parts.scheme}://{parts.netloc}"


def _e(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _time(at: datetime | None, tz: ZoneInfo, fmt: str = "%H:%M:%S %d/%m/%Y") -> str:
    return at.astimezone(tz).strftime(fmt) if at else "—"


def short_hash(value: str | None) -> str:
    return f"{value[:4]}…{value[-4:]}" if value and len(value) > 8 else (value or "—")


def _hash_row(label: str, value: str | None) -> str:
    if not value:
        return f'<dt>{_e(label)}</dt><dd class="hash">—</dd>'
    return (
        f'<dt>{_e(label)}</dt><dd class="hash"><details><summary>{_e(short_hash(value))} · Xem đầy đủ'
        f"</summary>{_e(value)}</details></dd>"
    )


def _session_html(s: W1Session, tz: ZoneInfo) -> str:
    rows = [
        f"<dt>Loại phiên</dt><dd>{_e(TYPE_LABELS.get(s.type, s.type))}</dd>",
        f"<dt>Thời gian</dt><dd>{_e(_time(s.started_at, tz))} – {_e(_time(s.ended_at, tz))}</dd>",
        f"<dt>Station</dt><dd>{_e(s.station or '—')}</dd>",
    ]
    if s.type == "RETURN":
        rows.append(f"<dt>Người kiểm</dt><dd>{_e(s.operator or '—')}</dd>")
        conclusion = CONCLUSION_LABELS.get(s.conclusion or "", s.conclusion or "—")
        rows.append(f"<dt>Kết luận</dt><dd>{_e(conclusion)}</dd>")
    note = STATUS_NOTES.get(s.status)
    mb = max(1, round(s.size_bytes / 1_000_000)) if s.size_bytes else 0
    photos = ""
    if s.photos:
        cells = "".join(
            f'<a href="{_e(p.url)}"><img src="{_e(p.url)}" alt="Ảnh {i} phiên {s.number}" loading="lazy"></a>'
            for i, p in enumerate(s.photos, start=1)
        )
        photos = f'<h2>Ảnh ({len(s.photos)})</h2><div class="grid">{cells}</div>'
    return (
        f'<section class="card"><h2>Phiên {s.number}</h2><dl>{"".join(rows)}</dl>'
        + (f'<p class="note">{_e(note)}</p>' if note else "")
        # Câu "không phát được" chỉ là nội dung dự phòng trong `<video>` — không có đoạn luôn hiện dưới video
        # (G4, DEC-972); không script được (CSP), nút "Tải video" luôn hiện.
        + f'<video controls preload="metadata" playsinline src="{_e(s.video_url)}">{_e(NO_PLAYBACK)}</video>'
        + f'<a class="button" href="{_e(s.download_url)}">Tải video (MP4, {mb} MB)</a>'
        + photos
        + "</section>"
    )


def _integrity_html(sessions: list[W1Session]) -> str:
    blocks = []
    for s in sessions:
        rows = [
            _hash_row("Clip gốc Cam 1", s.source_sha256.get("CAM1")),
            _hash_row("Clip gốc Cam 2", s.source_sha256.get("CAM2")),
            _hash_row("Video chia sẻ", s.video_sha256),
        ]
        blocks.append(f"<h2>Phiên {s.number} — SHA-256</h2><dl>{''.join(rows)}</dl>")
    return (
        '<section class="card"><h2>Toàn vẹn</h2>'
        "<p>Video ghi liên tục, không cắt ghép; chữ trên hình gắn khi xuất.</p>"
        + "".join(blocks)
        + "</section>"
    )


def render(ctx: W1Context) -> str:
    tz = ZoneInfo(ctx.tz)
    return TEMPLATE.substitute(
        origin=_e(ctx.origin),
        tracking_number=_e(ctx.tracking_number),
        platform_order_sn=_e(ctx.platform_order_sn or "—"),
        platform=_e(PLATFORM_LABELS.get(ctx.platform or "", ctx.platform or "—")),
        expires_at=_e(_time(ctx.expires_at, tz, "%d/%m/%Y %H:%M")),
        sessions="\n".join(_session_html(s, tz) for s in ctx.sessions),
        integrity=_integrity_html(ctx.sessions),
    )
