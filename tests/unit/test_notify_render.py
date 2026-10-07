"""T-227: dựng tin (02 §6.2 "Mẫu tin", FR-06.09, BR-36 (2)) — ≤ 10 dòng + "và {n} mục khác",
whitelist trường."""

from aicam.modules.notify.render import render

TZ = "Asia/Ho_Chi_Minh"


def _n02(i: int, **extra: object) -> dict[str, object]:
    data = {
        "tracking": f"SPXVN{i:010d}",
        "platform": "TIKTOK",
        "shop": "Áo Đẹp Official",
        "since": "2026-09-29T03:00:00Z",
        **extra,
    }
    return {"code": "N02", "severity": "HIGH", "data": data}


def test_max_10_lines_and_more() -> None:
    text = render("N02", "HIGH", [_n02(i) for i in range(13)], tz=TZ, site_address="x.local")
    lines = text.split("\n")
    assert lines[0] == "[CAO] Lệch trạng thái mức Cao — 13 mục"
    assert lines[1] == "• SPXVN0000000000 · TikTok · Áo Đẹp Official · từ 29/09"
    assert len([line for line in lines if line.startswith("• ")]) == 10
    assert lines[-2] == "và 3 mục khác"
    assert lines[-1] == "Xem: https://x.local/admin/recon?severity=HIGH"


def test_whitelist_drops_buyer_data_and_no_site_no_link() -> None:
    item = _n02(
        1,
        buyer_name="Nguyễn Văn A",
        phone="0912345678",
        buyer_note="giao giờ hành chính",
        amount=350000,
        url="https://s3/x?X-Amz-Signature=abc",
    )
    text = render("N02", "HIGH", [item], tz=TZ, site_address="")
    for secret in ("Nguyễn Văn A", "0912345678", "giao giờ", "350000", "X-Amz", "Xem:"):
        assert secret not in text
    assert text == "[CAO] Lệch trạng thái mức Cao\n• SPXVN0000000001 · TikTok · Áo Đẹp Official · từ 29/09"


def test_unknown_platform_and_n06_n07() -> None:
    n06 = {
        "code": "N06",
        "severity": "HIGH",
        "data": {
            "shop": "Áo Đẹp",
            "platform": "SHOPEE",
            "stage": "ERROR",
            "error_code": "SYNC_FAILED",
            "since": "2026-10-07T02:00:00Z",
        },
    }
    assert render("N06", "HIGH", [n06], tz=TZ, site_address="").split("\n")[1] == (
        "• Áo Đẹp · Shopee · Đồng bộ lỗi từ 09:00"
    )
    n07 = {"code": "N07", "severity": "MEDIUM", "data": {"percent": 82}}
    assert (
        render("N07", "MEDIUM", [n07], tz=TZ, site_address="")
        == "[TB] Ổ lưu video sắp đầy\n• Ổ lưu video đã dùng 82 %"
    )
    item = _n02(2)
    item["data"]["platform"] = None  # type: ignore[index]
    assert "· Chưa rõ sàn ·" in render("N02", "HIGH", [item], tz=TZ, site_address="")
