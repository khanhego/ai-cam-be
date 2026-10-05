"""Đọc mã vạch trên một khung Cam 2 (ADR-005: OpenCV + zxing-cpp; 02a §7 Vision).

Logic thuần, không I/O mạng: nhận khung (numpy BGR / xám) + ROI, trả tập mã vận đơn thấy được.
"""

import re
from dataclasses import dataclass
from typing import Any

import cv2
import numpy as np
import zxingcpp

# Phiếu vận đơn sàn in Code128; QR để dành cho phiếu có mã 2D (02a §7).
FORMATS = zxingcpp.barcode_formats_from_str("Code128,QRCode")


@dataclass(frozen=True)
class Roi:
    """Vùng đọc mã, tỉ lệ 0..1 theo khung (API-64)."""

    x: float
    y: float
    w: float
    h: float

    @classmethod
    def from_json(cls, data: dict[str, Any] | None) -> "Roi | None":
        if not data:
            return None
        return cls(float(data["x"]), float(data["y"]), float(data["w"]), float(data["h"]))


def crop(frame: np.ndarray, roi: Roi | None) -> np.ndarray:
    """Cắt ROI; None = cả khung. Làm tròn ra ngoài để không mất mép phiếu."""
    if roi is None:
        return frame
    height, width = frame.shape[:2]
    x0 = max(0, int(roi.x * width))
    y0 = max(0, int(roi.y * height))
    x1 = min(width, int(np.ceil((roi.x + roi.w) * width)))
    y1 = min(height, int(np.ceil((roi.y + roi.h) * height)))
    return frame[y0:y1, x0:x1]


def to_gray(frame: np.ndarray) -> np.ndarray:
    if frame.ndim == 2:
        return frame
    return cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)


def decode(frame: np.ndarray, roi: Roi | None, code_pattern: re.Pattern[str]) -> tuple[str, ...]:
    """Tập mã (đã upper, sắp xếp) trong ROI khớp định dạng mã vận đơn (`SCAN_CODE_REGEX`).

    Mã không phải mã vận đơn (vd QR quảng cáo trên hộp) bị bỏ qua để không gây MISMATCH giả.
    """
    region = crop(frame, roi)
    if region.size == 0:
        return ()
    found = zxingcpp.read_barcodes(to_gray(region), formats=FORMATS)
    codes = {b.text.strip().upper() for b in found if b.valid}
    return tuple(sorted(c for c in codes if code_pattern.fullmatch(c)))
