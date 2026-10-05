"""Luồng đọc khung Cam 2 (mỗi camera một thread — 02a §7 Vision, DEC-14: đọc relay RTSP của MediaMTX).

Thread chỉ lấy khung + giải mã rồi đẩy `Observation` sang vòng lặp asyncio; mọi trạng thái (khử nhiễu, Redis)
nằm ở `runner.py`.
"""

import os
import re
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import cv2
import numpy as np
import structlog

from aicam.modules.vision.reader import Roi, decode

log = structlog.get_logger()

SAMPLE_INTERVAL_S = 0.25  # 4 khung / giây
REOPEN_DELAY_S = 1.0
OPEN_TIMEOUT_MS = 5000
READ_TIMEOUT_MS = 3000


@dataclass(frozen=True)
class Observation:
    camera_id: uuid.UUID
    codes: tuple[str, ...] | None  # None = không lấy được khung (mất stream)
    at: float  # time.monotonic()


class Capture(Protocol):
    def grab(self) -> bool: ...
    def retrieve(self) -> tuple[bool, np.ndarray | None]: ...
    def release(self) -> None: ...


def open_rtsp(url: str) -> Capture | None:
    """Mở RTSP qua FFmpeg của OpenCV, TCP, có timeout để thread không treo khi camera rớt."""
    os.environ.setdefault("OPENCV_FFMPEG_CAPTURE_OPTIONS", "rtsp_transport;tcp")
    params = [
        cv2.CAP_PROP_OPEN_TIMEOUT_MSEC,
        OPEN_TIMEOUT_MS,
        cv2.CAP_PROP_READ_TIMEOUT_MSEC,
        READ_TIMEOUT_MS,
    ]
    cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG, params)
    if not cap.isOpened():
        cap.release()
        return None
    return cap


class CameraReader(threading.Thread):
    """Đọc liên tục (xả buffer), giải mã mỗi `sample_interval_s`. ROI đổi được khi đang chạy."""

    def __init__(
        self,
        camera_id: uuid.UUID,
        url: str,
        roi: Roi | None,
        code_pattern: re.Pattern[str],
        emit: Callable[[Observation], None],
        *,
        opener: Callable[[str], Capture | None] = open_rtsp,
        sample_interval_s: float = SAMPLE_INTERVAL_S,
        reopen_delay_s: float = REOPEN_DELAY_S,
    ) -> None:
        super().__init__(name=f"vision-{camera_id}", daemon=True)
        self.camera_id = camera_id
        self.url = url
        self.roi = roi
        self._pattern = code_pattern
        self._emit = emit
        self._opener = opener
        self._interval = sample_interval_s
        self._reopen_delay = reopen_delay_s
        self._halt = threading.Event()

    def stop(self) -> None:
        self._halt.set()

    def _lost(self) -> None:
        self._emit(Observation(self.camera_id, None, time.monotonic()))

    def run(self) -> None:
        while not self._halt.is_set():
            cap = None
            try:
                cap = self._opener(self.url)
                if cap is None:
                    self._lost()
                else:
                    self._read_until_failure(cap)
            except Exception:  # lỗi bất ngờ của OpenCV / zxing: báo mất stream rồi mở lại
                log.exception("vision_reader_failed", camera_id=str(self.camera_id))
                self._lost()
            finally:
                if cap is not None:
                    cap.release()
            self._halt.wait(self._reopen_delay)

    def _read_until_failure(self, cap: Capture) -> None:
        last_decode = 0.0
        while not self._halt.is_set():
            if not cap.grab():
                self._lost()
                return
            now = time.monotonic()
            if now - last_decode < self._interval:
                continue
            last_decode = now
            ok, frame = cap.retrieve()
            if not ok or frame is None:
                self._lost()
                return
            self._emit(Observation(self.camera_id, decode(frame, self.roi, self._pattern), now))
