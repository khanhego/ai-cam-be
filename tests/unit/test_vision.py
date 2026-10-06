"""Vision Cam 2 (T-12): đọc mã trong ROI, khử nhiễu, thread đọc khung — 02a §7 Vision, §11 Unit.

Ảnh mã vạch tổng hợp bằng zxing-cpp (không cần camera). Tỉ lệ đọc với phiếu thật (AC-04) cần T-4.
"""

import re
import threading
import time
import uuid

import numpy as np
import zxingcpp

from aicam.modules.vision.capture import CameraReader, Observation
from aicam.modules.vision.reader import Roi, crop, decode
from aicam.modules.vision.tray import TrayDebouncer

PATTERN = re.compile(r"^[A-Z0-9-]{8,40}$")


def _barcode(text: str, fmt: str = "Code128") -> np.ndarray:
    image = zxingcpp.create_barcode(text, zxingcpp.barcode_format_from_str(fmt)).to_image(scale=3)
    return np.array(image)


def _tray(*placed: tuple[str, int, int], fmt: str = "Code128") -> np.ndarray:
    """Khay xám 1280×720, đặt phiếu (mã, x, y)."""
    frame = np.full((720, 1280), 140, dtype=np.uint8)
    for text, x, y in placed:
        code = _barcode(text, fmt)
        padded = np.full((code.shape[0] + 40, code.shape[1] + 40), 255, dtype=np.uint8)
        padded[20:-20, 20:-20] = code
        frame[y : y + padded.shape[0], x : x + padded.shape[1]] = padded
    return frame


# ---------------------------------------------------------------- reader


def test_decode_single_label() -> None:
    """TC-03.20 (phần logic): một phiếu trên khay → đọc đúng mã."""
    assert decode(_tray(("SPXTST0000014", 380, 280)), None, PATTERN) == ("SPXTST0000014",)


def test_decode_two_labels() -> None:
    """2 phiếu → 2 mã (api suy ra MULTIPLE)."""
    frame = _tray(("SPXTST0000002", 380, 80), ("SPXTST0000003", 380, 420))
    assert decode(frame, None, PATTERN) == ("SPXTST0000002", "SPXTST0000003")


def test_decode_bgr_and_qr() -> None:
    frame = np.stack([_tray(("SPXTST0000005", 500, 200), fmt="QRCode")] * 3, axis=-1)
    assert decode(frame, None, PATTERN) == ("SPXTST0000005",)


def test_label_outside_roi_not_read() -> None:
    """TC-03.35 (phần logic): phiếu ở góc khay ngoài ROI x=0.2, y=0.2, w=0.6, h=0.6 → không đọc."""
    roi = Roi(0.2, 0.2, 0.6, 0.6)
    outside = _tray(("SPXTST0000014", 0, 0))
    inside = _tray(("SPXTST0000014", 380, 280))
    assert decode(outside, roi, PATTERN) == ()
    assert decode(inside, roi, PATTERN) == ("SPXTST0000014",)


def test_non_tracking_codes_ignored() -> None:
    """Mã không đúng định dạng vận đơn (vd QR quảng cáo) không tính — tránh MISMATCH giả."""
    assert decode(_tray(("https://shop.vn/km", 400, 200), fmt="QRCode"), None, PATTERN) == ()


def test_empty_tray() -> None:
    assert decode(_tray(), None, PATTERN) == ()


def test_crop_bounds() -> None:
    frame = np.zeros((100, 200), dtype=np.uint8)
    assert crop(frame, Roi(0.5, 0.5, 0.5, 0.5)).shape == (50, 100)
    assert crop(frame, None).shape == (100, 200)
    assert Roi.from_json(None) is None
    assert Roi.from_json({"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}) == Roi(0.1, 0.2, 0.3, 0.4)


# ---------------------------------------------------------------- khử nhiễu


def test_new_code_needs_two_frames() -> None:
    deb = TrayDebouncer()
    assert deb.observe(("A1234567",)) is False
    assert deb.stable is None
    assert deb.observe(("A1234567",)) is True
    assert deb.stable == ("A1234567",)
    assert deb.observe(("A1234567",)) is False


def test_empty_needs_four_frames_and_flicker_is_ignored() -> None:
    """Tay che phiếu 1–3 khung không làm khay 'trống'."""
    deb = TrayDebouncer(stable=("A1234567",))
    for _ in range(3):
        assert deb.observe(()) is False
    assert deb.observe(("A1234567",)) is False  # thấy lại → bỏ đếm
    assert [deb.observe(()) for _ in range(4)] == [False, False, False, True]
    assert deb.stable == ()


def test_alternating_sets_do_not_flip() -> None:
    """Đọc chập chờn giữa hai tập mã khác nhau → giữ trạng thái cũ."""
    deb = TrayDebouncer(stable=("A1234567",))
    results = [deb.observe(c) for c in [("B1234567",), ("C1234567",), ("B1234567",), ("C1234567",)]]
    assert results == [False] * 4
    assert deb.stable == ("A1234567",)


def test_lose_resets_state() -> None:
    deb = TrayDebouncer(stable=("A1234567",))
    assert deb.lose() is True
    assert deb.stable is None
    assert deb.lose() is False


# ---------------------------------------------------------------- thread đọc khung


class _FakeCapture:
    def __init__(self, frames: list[np.ndarray]) -> None:
        self.frames = frames
        self.released = False

    def grab(self) -> bool:
        return bool(self.frames)

    def retrieve(self) -> tuple[bool, np.ndarray | None]:
        return True, self.frames.pop(0)

    def release(self) -> None:
        self.released = True


def test_reader_emits_codes_then_lost_and_reopens() -> None:
    camera_id = uuid.uuid4()
    got: list[Observation] = []
    done = threading.Event()
    opens: list[_FakeCapture] = []

    def opener(url: str) -> _FakeCapture | None:
        assert url == "rtsp://mediamtx:8554/cam-x"
        if opens:
            done.set()
            return None  # lần mở lại thất bại → báo mất stream
        cap = _FakeCapture([_tray(("SPXTST0000001", 380, 280)), _tray()])
        opens.append(cap)
        return cap

    reader = CameraReader(
        camera_id, "rtsp://mediamtx:8554/cam-x", None, PATTERN, got.append,
        opener=opener, sample_interval_s=0, reopen_delay_s=0.01,
    )  # fmt: skip
    reader.start()
    assert done.wait(5)
    reader.stop()
    reader.join(5)

    codes = [o.codes for o in got]
    assert codes[:3] == [("SPXTST0000001",), (), None]
    assert None in codes[3:]
    assert all(o.camera_id == camera_id for o in got)
    assert opens[0].released


def test_reader_samples_at_interval() -> None:
    """Đọc mọi khung (xả buffer) nhưng chỉ giải mã mỗi `sample_interval_s`."""
    got: list[Observation] = []

    class _Endless(_FakeCapture):
        def grab(self) -> bool:
            time.sleep(0.01)
            return True

        def retrieve(self) -> tuple[bool, np.ndarray | None]:
            return True, _tray()

    reader = CameraReader(uuid.uuid4(), "u", None, PATTERN, got.append, opener=lambda _: _Endless([]),
                          sample_interval_s=0.1)  # fmt: skip
    reader.start()
    time.sleep(0.55)
    reader.stop()
    reader.join(5)
    assert 3 <= len(got) <= 7


# ---------------------------------------------------------------- T-121: khung mới nhất cho ảnh chụp


def test_cam1_reader_only_frames_no_decode() -> None:
    """Cam 1 (không `code_pattern`): không giải mã / không Observation khi đọc được; JPEG ~mỗi
    `frame_interval_s` kèm giờ chụp; JPEG giải nén lại được (đủ làm bằng chứng)."""
    import cv2

    got: list[Observation] = []
    frames: list[tuple[uuid.UUID, bytes, object]] = []

    class _Endless(_FakeCapture):
        def grab(self) -> bool:
            time.sleep(0.01)
            return True

        def retrieve(self) -> tuple[bool, np.ndarray | None]:
            return True, np.full((360, 640, 3), 90, dtype=np.uint8)

    camera_id = uuid.uuid4()
    reader = CameraReader(
        camera_id, "u", None, None, got.append, opener=lambda _: _Endless([]),
        on_frame=lambda cid, jpeg, at: frames.append((cid, jpeg, at)), frame_interval_s=0.1,
    )  # fmt: skip
    reader.start()
    time.sleep(0.55)
    reader.stop()
    reader.join(5)
    assert got == []
    assert 3 <= len(frames) <= 7
    cid, jpeg, _ = frames[-1]
    assert cid == camera_id
    image = cv2.imdecode(np.frombuffer(jpeg, dtype=np.uint8), cv2.IMREAD_COLOR)
    assert image.shape == (360, 640, 3)


def test_cam2_reader_decodes_and_sends_frames() -> None:
    """Cam 2: vẫn giải mã mỗi `sample_interval_s` và thêm JPEG mỗi `frame_interval_s` (thưa hơn)."""
    got: list[Observation] = []
    frames: list[bytes] = []

    class _Endless(_FakeCapture):
        def grab(self) -> bool:
            time.sleep(0.01)
            return True

        def retrieve(self) -> tuple[bool, np.ndarray | None]:
            return True, _tray()

    reader = CameraReader(
        uuid.uuid4(), "u", None, PATTERN, got.append, opener=lambda _: _Endless([]), sample_interval_s=0.05,
        on_frame=lambda _cid, jpeg, _at: frames.append(jpeg), frame_interval_s=0.2,
    )  # fmt: skip
    reader.start()
    time.sleep(0.65)
    reader.stop()
    reader.join(5)
    assert len(got) > len(frames) >= 2
