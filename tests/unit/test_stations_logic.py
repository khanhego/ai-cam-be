import pytest
from pydantic import ValidationError

from aicam.modules.stations.health import HealthTracker
from aicam.modules.stations.mediamtx import PathStat, mask, mediamtx_path, with_credentials
from aicam.modules.stations.probe import classify_ffmpeg_error, parse_onvif_utc
from aicam.modules.stations.schemas import Roi


def _paths(**bytes_by_name: int) -> dict[str, PathStat]:
    return {n: PathStat(n, True, b) for n, b in bytes_by_name.items()}


def test_camera_goes_online_when_bytes_flow() -> None:
    tracker = HealthTracker()

    assert tracker.update(0, _paths(cam=100), ["cam"]) == {"cam": "ONLINE"}
    assert tracker.update(2, _paths(cam=200), ["cam"]) == {}


def test_camera_offline_after_six_seconds_without_new_bytes() -> None:
    """AC-10: phát hiện ≤ 8 giây (vòng 2 giây + ngưỡng 6 giây)."""
    tracker = HealthTracker()
    tracker.update(0, _paths(cam=100), ["cam"])

    assert tracker.update(4, _paths(cam=100), ["cam"]) == {}
    assert tracker.update(6, _paths(cam=100), ["cam"]) == {"cam": "OFFLINE"}
    assert tracker.update(8, _paths(cam=150), ["cam"]) == {"cam": "ONLINE"}


def test_missing_or_not_ready_path_is_offline() -> None:
    tracker = HealthTracker()

    assert tracker.update(0, {}, ["cam"]) == {"cam": "OFFLINE"}
    assert tracker.update(2, {"cam": PathStat("cam", False, 0)}, ["cam"]) == {}


def test_unwatched_paths_are_forgotten() -> None:
    tracker = HealthTracker()
    tracker.update(0, _paths(a=1, b=1), ["a", "b"])

    tracker.update(2, _paths(a=2), ["a"])

    assert tracker.update(4, _paths(a=3, b=5), ["a", "b"]) == {"b": "ONLINE"}


def test_credentials_are_embedded_and_masked() -> None:
    url = with_credentials("rtsp://192.168.20.12:554/stream1", "admin", "p@ss:word")

    assert url == "rtsp://admin:p%40ss%3Aword@192.168.20.12:554/stream1"
    assert mask(url) == "rtsp://192.168.20.12:554/stream1"
    assert with_credentials("rtsp://h/s", None, None) == "rtsp://h/s"


def test_mediamtx_path_name() -> None:
    assert mediamtx_path("abc") == "cam-abc"


@pytest.mark.parametrize(
    ("stderr", "reason"),
    [
        ("method DESCRIBE failed: 401 Unauthorized", "AUTH"),
        ("Connection to tcp://10.0.0.9:554 failed: Connection refused", "TIMEOUT"),
        ("Operation timed out", "TIMEOUT"),
        ("Invalid data found when processing input", "STREAM"),
    ],
)
def test_classify_ffmpeg_error(stderr: str, reason: str) -> None:
    assert classify_ffmpeg_error(stderr) == reason


def test_parse_onvif_utc() -> None:
    xml = (
        "<tds:SystemDateAndTime><tt:UTCDateTime><tt:Time><tt:Hour>7</tt:Hour><tt:Minute>27</tt:Minute>"
        "<tt:Second>5</tt:Second></tt:Time><tt:Date><tt:Year>2026</tt:Year><tt:Month>10</tt:Month>"
        "<tt:Day>4</tt:Day></tt:Date></tt:UTCDateTime></tds:SystemDateAndTime>"
    )

    parsed = parse_onvif_utc(xml)

    assert parsed is not None
    assert parsed.isoformat() == "2026-10-04T07:27:05+00:00"
    assert parse_onvif_utc("<x/>") is None


@pytest.mark.parametrize(
    "roi",
    [
        {"x": -0.1, "y": 0, "w": 0.5, "h": 0.5},
        {"x": 0.6, "y": 0, "w": 0.5, "h": 0.5},
        {"x": 0, "y": 0, "w": 0.04, "h": 0.5},
    ],
)
def test_roi_rejects_invalid(roi: dict[str, float]) -> None:
    with pytest.raises(ValidationError):
        Roi(**roi)


def test_roi_accepts_edge() -> None:
    assert Roi(x=0.5, y=0.5, w=0.5, h=0.5).w == 0.5
