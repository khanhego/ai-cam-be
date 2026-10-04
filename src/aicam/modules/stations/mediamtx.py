"""Client API MediaMTX v3 (ADR-003, 02a API-61, J-08). Đã kiểm với MediaMTX v1.21.1."""

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote, urlsplit, urlunsplit

import httpx


@dataclass(frozen=True)
class PathStat:
    name: str
    ready: bool
    inbound_bytes: int


class MediaMTXError(Exception):
    pass


class MediaMTX(Protocol):
    async def upsert_path(self, name: str, source: str) -> None: ...
    async def delete_path(self, name: str) -> None: ...
    async def list_paths(self) -> dict[str, PathStat]: ...


def mediamtx_path(camera_id: object) -> str:
    """Path MediaMTX của một camera: `cam-{camera_id}` (02a API-61)."""
    return f"cam-{camera_id}"


def with_credentials(rtsp_url: str, username: str | None, password: str | None) -> str:
    """Gắn user:pass vào URL RTSP (dùng cho MediaMTX / ffmpeg, không bao giờ trả ra API)."""
    if not username:
        return rtsp_url
    parts = urlsplit(rtsp_url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    auth = quote(username, safe="")
    if password:
        auth += ":" + quote(password, safe="")
    return urlunsplit((parts.scheme, f"{auth}@{host}", parts.path, parts.query, parts.fragment))


def mask(rtsp_url: str) -> str:
    """Bỏ user:pass khỏi URL trước khi trả ra API / ghi log (02a §3 dữ liệu nhạy cảm)."""
    parts = urlsplit(rtsp_url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


class HttpMediaMTX:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    async def _request(self, method: str, path: str, json: dict[str, Any] | None = None) -> httpx.Response:
        try:
            async with httpx.AsyncClient(base_url=self._base, timeout=self._timeout) as client:
                return await client.request(method, path, json=json)
        except httpx.HTTPError as exc:
            raise MediaMTXError(f"MediaMTX không phản hồi: {exc}") from exc

    async def upsert_path(self, name: str, source: str) -> None:
        body = {"source": source, "sourceOnDemand": False}
        res = await self._request("PATCH", f"/v3/config/paths/patch/{name}", body)
        if res.status_code == 404:
            res = await self._request("POST", f"/v3/config/paths/add/{name}", body)
        if res.status_code >= 400:
            raise MediaMTXError(f"Không cấu hình được path {name}: {res.text}")

    async def delete_path(self, name: str) -> None:
        res = await self._request("DELETE", f"/v3/config/paths/delete/{name}")
        if res.status_code not in (200, 404):
            raise MediaMTXError(f"Không xóa được path {name}: {res.text}")

    async def list_paths(self) -> dict[str, PathStat]:
        res = await self._request("GET", "/v3/paths/list?itemsPerPage=1000")
        if res.status_code >= 400:
            raise MediaMTXError(res.text)
        out: dict[str, PathStat] = {}
        for item in res.json().get("items", []):
            inbound = item.get("inboundBytes", item.get("bytesReceived", 0)) or 0
            out[item["name"]] = PathStat(item["name"], bool(item.get("ready")), int(inbound))
        return out
