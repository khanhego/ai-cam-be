"""W1 (02 §6.3, 02a §7.4, DEC-506, DEC-470): escape, whitelist trường, CSP + referrer, không `<script`, mọi
`src` / `href` cùng origin kho lưu."""

import re
from datetime import UTC, datetime, timedelta

import pytest

from aicam.modules.shares import w1

ORIGIN = "https://s3.example.vn"
T0 = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)


def _ctx(**kw: object) -> w1.W1Context:
    sessions = [
        w1.W1Session(
            number=1,
            type="PACK",
            status="COMPLETED",
            started_at=T0,
            ended_at=T0 + timedelta(minutes=2),
            station="Station 01",
            operator="Không hiện ở phiên đóng gói",
            conclusion=None,
            video_url=f"{ORIGIN}/aicam-share/share/tok/v1.mp4?X-Amz-Signature=a",
            download_url=f"{ORIGIN}/aicam-share/share/tok/v1.mp4?response-content-disposition=x&X-Amz-Signature=b",
            size_bytes=38_000_000,
            video_sha256="a41c" + "0" * 56 + "09ef",
            source_sha256={"CAM1": "3f9a" + "1" * 56 + "c21e", "CAM2": None},
        ),
        w1.W1Session(
            number=2,
            type="RETURN",
            status="ABANDONED",
            started_at=T0 + timedelta(days=3),
            ended_at=T0 + timedelta(days=3, minutes=4),
            station='<script>alert("x")</script>',
            operator="Lan <b>QA</b>",
            conclusion="EMPTY_BOX",
            video_url=f"{ORIGIN}/aicam-share/share/tok/v2.mp4?X-Amz-Signature=c&a=1",
            download_url=f"{ORIGIN}/aicam-share/share/tok/v2.mp4?X-Amz-Signature=d",
            size_bytes=12_400_000,
            video_sha256="b" * 64,
            source_sha256={"CAM1": "c" * 64, "CAM2": "d" * 64},
            photos=[w1.W1Photo(f"{ORIGIN}/aicam-share/share/tok/p2-1.jpg?X-Amz-Signature=e")],
        ),
    ]
    base: dict[str, object] = {
        "origin": ORIGIN,
        "tracking_number": "SPXVN0123456789",
        "platform_order_sn": "2410TST00061",
        "platform": "TIKTOK",
        "expires_at": T0 + timedelta(days=7),
        "tz": "Asia/Ho_Chi_Minh",
        "sessions": sessions,
    }
    base.update(kw)
    return w1.W1Context(**base)  # type: ignore[arg-type]


def test_csp_referrer_no_script_and_same_origin() -> None:
    page = w1.render(_ctx())
    assert (
        '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; '
        "img-src https://s3.example.vn; media-src https://s3.example.vn; style-src 'unsafe-inline'; "
        "base-uri 'none'; form-action 'none'\">"
    ) in page
    assert '<meta name="referrer" content="no-referrer">' in page
    assert '<meta name="robots" content="noindex, nofollow">' in page
    assert "<script" not in page.lower()
    links = re.findall(r'(?:src|href)="([^"]*)"', page)
    assert len(links) == 6  # 2 video + 2 nút tải + ảnh (src + href mở ảnh gốc)
    assert all(link.startswith(f"{ORIGIN}/") for link in links)
    assert "&amp;X-Amz-Signature=b" in page  # `&` trong URL được escape trong thuộc tính


def test_content_matches_whitelist() -> None:
    page = w1.render(_ctx())
    for text in (
        "Bằng chứng video — Hệ thống X",
        "SPXVN0123456789",
        "2410TST00061",
        "TikTok Shop",
        "Link hết hạn 13/10/2026 10:00 (giờ Việt Nam)",
        "Đóng gói",
        "Mở hàng hoàn",
        "Hộp rỗng",
        "Phiên bị bỏ dở",
        "Tải video (MP4, 38 MB)",
        "Tải video (MP4, 12 MB)",
        "a41c…09ef",
        "Video ghi liên tục, không cắt ghép; chữ trên hình gắn khi xuất.",
        "Trình duyệt không phát được video. Bấm Tải video để xem bằng ứng dụng khác.",
        "10:00:00 06/10/2026",
    ):
        assert text in page, text
    # Người kiểm chỉ hiện ở phiên hoàn (02 §6.3).
    assert "Không hiện ở phiên đóng gói" not in page
    # Escape mọi giá trị (DEC-470).
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;" in page
    assert "Lan &lt;b&gt;QA&lt;/b&gt;" in page
    assert "<b>QA</b>" not in page


def test_template_has_no_field_outside_whitelist() -> None:
    fields = set(w1.W1Context.__dataclass_fields__)
    assert fields == {
        "origin",
        "tracking_number",
        "platform_order_sn",
        "platform",
        "expires_at",
        "tz",
        "sessions",
    }
    session_fields = set(w1.W1Session.__dataclass_fields__)
    for forbidden in ("recipient", "created_by", "note", "amount", "shop_name", "buyer", "claim_code"):
        assert forbidden not in fields | session_fields


def test_origin_of() -> None:
    assert w1.origin_of("https://s3.example.vn/b/share/x/index.html?X=1") == "https://s3.example.vn"
    assert w1.origin_of("http://192.168.1.5:59000/b/k") == "http://192.168.1.5:59000"
    with pytest.raises(ValueError, match="scheme"):
        w1.origin_of("memory://")
