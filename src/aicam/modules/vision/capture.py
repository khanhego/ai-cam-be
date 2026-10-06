"""Luồng đọc khung camera (mỗi camera một thread — 02a §7 Vision, DEC-14: đọc relay RTSP của MediaMTX).

Thread chỉ lấy khung + giải mã mã khay (Cam 2) rồi đẩy `Observation` sang vòng lặp asyncio; T-121 (DEC-320):
mọi camera (Cam 1 + Cam 2) còn nén JPEG khung mới nhất mỗi `frame_interval_s` và đẩy qua `on_frame` (ghi Redis
ở `runner.py`). Mọi trạng thái (khử nhiễu, Redis) nằm ở `runner.py`.
"""

import os
import re
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import cv2
import numpy as np
import structlog

from aicam.core import clock
from aicam.modules.vision.reader import Roi, decode

log = structlog.get_logger()

SAMPLE_INTERVAL_S = 0.25  # 4 khung / giây
FRAME_INTERVAL_S = 1.0  # T-121: JPEG khung mới nhất ~1 lần / giây
JPEG_QUALITY = 85  # = SNAPSHOT_JPEG_QUALITY (bằng chứng — 02a §9)

FrameSink = Callable[[uuid.UUID, bytes, datetime], None]
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


def encode_jpeg(frame: np.ndarray, quality: int = JPEG_QUALITY) -> bytes | None:
    ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, int(quality)])
    return buf.tobytes() if ok else None


class CameraReader(threading.Thread):
    """Đọc liên tục (xả buffer), giải mã mã khay mỗi `sample_interval_s` (khi có `code_pattern` — Cam 2), nén
    JPEG khung mới nhất mỗi `frame_interval_s` (khi có `on_frame` — T-121). ROI đổi được khi đang chạy."""

    def __init__(
        self,
        camera_id: uuid.UUID,
        url: str,
        roi: Roi | None,
        code_pattern: re.Pattern[str] | None,
        emit: Callable[[Observation], None],
        *,
        opener: Callable[[str], Capture | None] = open_rtsp,
        sample_interval_s: float = SAMPLE_INTERVAL_S,
        reopen_delay_s: float = REOPEN_DELAY_S,
        on_frame: FrameSink | None = None,
        frame_interval_s: float = FRAME_INTERVAL_S,
        jpeg_quality: int = JPEG_QUALITY,
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
        self._on_frame = on_frame
        self._frame_interval = frame_interval_s
        self._jpeg_quality = jpeg_quality
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
        last_decode = last_frame = 0.0
        while not self._halt.is_set():
            if not cap.grab():
                self._lost()
                return
            now = time.monotonic()
            want_decode = self._pattern is not None and now - last_decode >= self._interval
            want_frame = self._on_frame is not None and now - last_frame >= self._frame_interval
            if not (want_decode or want_frame):
                continue
            ok, frame = cap.retrieve()
            if not ok or frame is None:
                self._lost()
                return
            if want_decode and self._pattern is not None:
                last_decode = now
                self._emit(Observation(self.camera_id, decode(frame, self.roi, self._pattern), now))
            if want_frame and self._on_frame is not None:
                last_frame = now
                taken_at = clock.now()
                jpeg = encode_jpeg(frame, self._jpeg_quality)
                if jpeg:
                    self._on_frame(self.camera_id, jpeg, taken_at)
