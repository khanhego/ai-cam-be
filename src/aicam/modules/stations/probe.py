"""Lấy một khung hình (API-62, API-63) và đọc giờ camera qua ONVIF (J-09)."""

import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from aicam.core import clock


class CameraUnreachable(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason  # TIMEOUT | AUTH | STREAM


def classify_ffmpeg_error(stderr: str) -> str:
    text = stderr.lower()
    if "401" in text or "unauthorized" in text or "authorization" in text:
        return "AUTH"
    if (
        "timed out" in text
        or "connection refused" in text
        or "no route" in text
        or "could not resolve" in text
    ):
        return "TIMEOUT"
    return "STREAM"


async def grab_frame(rtsp_url: str, timeout_s: float = 8.0) -> bytes:
    """Trả JPEG một khung. Lỗi → CameraUnreachable(reason)."""
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-rtsp_transport", "tcp",
        "-timeout", str(int(timeout_s * 1_000_000)), "-i", rtsp_url,
        "-frames:v", "1", "-f", "image2", "-c:v", "mjpeg", "-q:v", "4", "pipe:1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_s + 2)
    except TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise CameraUnreachable("TIMEOUT") from exc
    if proc.returncode != 0 or not out:
        stderr = err.decode(errors="replace")
        raise CameraUnreachable(classify_ffmpeg_error(stderr), stderr[-300:])
    return out


_ONVIF_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">
  <s:Body><GetSystemDateAndTime xmlns="http://www.onvif.org/ver10/device/wsdl"/></s:Body>
</s:Envelope>"""


@dataclass(frozen=True)
class _UtcParts:
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int


def parse_onvif_utc(xml: str) -> datetime | None:
    """Lấy UTCDateTime trong response GetSystemDateAndTime (không cần thư viện SOAP)."""
    block = re.search(r"UTCDateTime>(.*?)</[\w:]*UTCDateTime", xml, re.S)
    if not block:
        return None
    values: dict[str, int] = {}
    for tag in ("Year", "Month", "Day", "Hour", "Minute", "Second"):
        m = re.search(rf"<[\w:]*{tag}>(\d+)</", block.group(1))
        if not m:
            return None
        values[tag.lower()] = int(m.group(1))
    p = _UtcParts(**values)
    return datetime(p.year, p.month, p.day, p.hour, p.minute, p.second, tzinfo=UTC)


async def onvif_clock_offset_ms(host: str, port: int = 80, timeout_s: float = 5.0) -> int | None:
    """Lệch giờ camera − server (ms). Camera không có ONVIF → None (DEC-33: rủi ro spike)."""
    url = f"http://{host}:{port}/onvif/device_service"
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            sent = clock.now()
            res = await client.post(
                url, content=_ONVIF_BODY, headers={"Content-Type": "application/soap+xml"}
            )
            received = clock.now()
    except httpx.HTTPError:
        return None
    if res.status_code != 200:
        return None
    camera_time = parse_onvif_utc(res.text)
    if camera_time is None:
        return None
    midpoint = sent + (received - sent) / 2
    return int((camera_time - midpoint).total_seconds() * 1000)
