"""Sinh video mẫu cho camera giả (chạy lúc build image fake-cams).

cam1.mp4: cảnh bàn đóng gói giả (testsrc2) có nhãn "CAM 1".
cam2.mp4: khay phiếu nhìn từ trên, 60 giây lặp:
    0-20s  phiếu SPXTST0000001
    20-25s khay trống
    25-45s phiếu SPXTST0000002
    45-50s hai phiếu SPXTST0000002 + SPXTST0000003 (dùng thử MULTIPLE / DIFFERENT)
    50-60s khay trống
Mã khớp dữ liệu seed `aicam seed-demo --prefix TST` (04-test-cases §1).
"""

import subprocess
import sys
from pathlib import Path

import barcode
from barcode.writer import ImageWriter
from PIL import Image, ImageDraw, ImageFont

W, H = 1280, 720
FPS = 15
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"


def label(code: str) -> Image.Image:
    """Phiếu vận đơn giả: khung trắng, tiêu đề, barcode Code128."""
    bc = barcode.get("code128", code, writer=ImageWriter()).render(
        {"module_width": 0.25, "module_height": 9, "font_size": 7, "text_distance": 3, "quiet_zone": 4}
    )
    card = Image.new("RGB", (bc.width + 40, bc.height + 70), "white")
    draw = ImageDraw.Draw(card)
    draw.text((20, 15), "SPX Express - TEST", fill="black", font=ImageFont.truetype(FONT, 22))
    card.paste(bc, (20, 55))
    return card


def tray(codes: list[str]) -> Image.Image:
    img = Image.new("RGB", (W, H), (92, 84, 74))
    ImageDraw.Draw(img).rectangle((180, 90, W - 180, H - 90), fill=(140, 130, 118))
    cards = [label(code) for code in codes]
    gap = 24
    y = (H - sum(c.height for c in cards) - gap * (len(cards) - 1)) // 2
    for card in cards:
        img.paste(card, ((W - card.width) // 2, y))
        y += card.height + gap
    return img


def build_cam2(out: Path, work: Path) -> None:
    scenes = [
        (20, ["SPXTST0000001"]),
        (5, []),
        (20, ["SPXTST0000002"]),
        (5, ["SPXTST0000002", "SPXTST0000003"]),
        (10, []),
    ]
    concat = work / "cam2.txt"
    lines = []
    for i, (seconds, codes) in enumerate(scenes):
        png = work / f"scene{i}.png"
        tray(codes).save(png)
        lines += [f"file '{png}'", f"duration {seconds}"]
    lines.append(f"file '{work / f'scene{len(scenes) - 1}.png'}'")
    concat.write_text("\n".join(lines) + "\n")
    run(
        "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat),
        "-vf", f"fps={FPS},format=yuv420p", "-c:v", "libx264", "-preset", "veryfast",
        "-g", str(FPS * 2), "-t", "60", str(out),
    )  # fmt: skip


def build_cam1(out: Path) -> None:
    title = (
        f"drawtext=fontfile={FONT}:text='CAM 1 - BAN DONG GOI':x=40:y=40"
        ":fontsize=36:fontcolor=white:box=1:boxcolor=black@0.5"
    )
    run(
        "ffmpeg", "-y", "-f", "lavfi", "-i", f"testsrc2=size={W}x{H}:rate={FPS}", "-vf", title,
        "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
        "-g", str(FPS * 2), "-t", "60", str(out),
    )  # fmt: skip


def run(*cmd: str) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)  # noqa: S603


def main() -> int:
    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "/media")
    work = out_dir / "work"
    work.mkdir(parents=True, exist_ok=True)
    build_cam1(out_dir / "cam1.mp4")
    build_cam2(out_dir / "cam2.mp4", work)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
