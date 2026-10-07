"""NFR-42 / AC-52 / AC-53 trên MinIO tạm (02a §11 `test_share_security_nfr42`): link mở được không đăng nhập
(`text/html`, `no-store`, video phát / tải được), đổi 1 ký tự token / chữ ký → không mở, không liệt kê được
bucket (không ký / dùng chữ ký của `index.html`), thu hồi → `NoSuchKey` ≤ 60 giây (đồng hồ thật), bucket link
không còn phiên bản nào.

Chỉ chạy khi có `TEST_S3_ENDPOINT` (container MinIO riêng — không phải stack dev). Nhà cung cấp S3 thật
(Q20) — HTML mở thẳng trong trình duyệt, URL ký 7 ngày: chưa test — thiếu tài nguyên.
"""

import os
import re
import secrets
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import S3Store
from aicam.modules.shares import build, cleanup, service
from aicam.modules.shares.models import ShareLink

from .backup_fixtures import S3_ENDPOINT, needs_minio
from .shares_fixtures import ShareWorld, login, make_share_settings, make_share_world, share_api
from .test_shares_jobs import fake_render

__all__ = ["share_api"]

pytestmark = [pytest.mark.integration, needs_minio]


@pytest.fixture
def share_settings(tmp_path: Path) -> Settings:
    return make_share_settings(
        tmp_path,
        s3_endpoint=S3_ENDPOINT,
        s3_public_endpoint="",
        s3_access_key_id=os.environ.get("TEST_S3_ACCESS_KEY", ""),
        s3_secret_access_key=os.environ.get("TEST_S3_SECRET_KEY", ""),
        s3_bucket=os.environ.get("TEST_S3_BUCKET", "aicam-test-backup"),
        s3_share_bucket=os.environ.get("TEST_S3_SHARE_BUCKET", "aicam-test-share"),
    )


def _root() -> Any:
    import boto3

    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=os.environ["TEST_S3_ROOT_KEY"],
        aws_secret_access_key=os.environ["TEST_S3_ROOT_SECRET"], region_name="us-east-1",
    )  # fmt: skip


def _flip(ch: str) -> str:
    return "A" if ch != "A" else "B"


def test_token_256_bit_unique() -> None:
    prefixes = {service.new_object_prefix() for _ in range(1000)}
    assert len(prefixes) == 1000
    for p in list(prefixes)[:50]:
        token = p.removeprefix("share/").removesuffix("/")
        assert len(token) == 43  # 32 byte = 256 bit, base64url
        assert re.fullmatch(r"[A-Za-z0-9_-]{43}", token)
    assert secrets.token_urlsafe(32) != secrets.token_urlsafe(32)


async def test_link_open_tamper_list_revoke(
    share_api: AsyncClient, db: AsyncSession, share_settings: Settings
) -> None:
    w: ShareWorld = await make_share_world(db, share_settings, n=71)
    store = cloud.share_store(share_settings)
    assert isinstance(store, S3Store)
    headers, _ = await login(share_api, db, "CSKH")
    body = {
        "source_type": "CLAIM",
        "claim_id": str(w.claim.id),
        "session_ids": [str(w.ret_a.id), str(w.pack.id)],
        "layout": "SIDE_BY_SIDE",
        "include_snapshots": True,
        "recipient": "CSKH Shopee – phiếu 1",
        "expires_days": 7,
    }
    res = await share_api.post("/api/v1/shares", headers=headers, json=body)
    assert res.status_code == 202, res.text
    share_id = res.json()["id"]
    assert await build.build(db, share_id, share_settings, render=fake_render) == "ACTIVE"
    link = await db.get(ShareLink, share_id, populate_existing=True)
    assert link is not None
    url = Cipher(share_settings.fernet_key).decrypt(link.url_enc or b"")
    assert "X-Amz-Expires=604" in url  # 7 ngày (≤ 604.800 giây — giới hạn SigV4)

    async with httpx.AsyncClient(timeout=10) as client:
        # Người nhận mở không đăng nhập.
        page = await client.get(url)
        assert page.status_code == 200
        assert page.headers["content-type"].startswith("text/html")
        assert page.headers.get("cache-control") == "no-store"
        assert w.package.tracking_number in page.text
        assert "<script" not in page.text
        media = re.findall(r'(?:src|href)="([^"]+)"', page.text)
        assert media
        origin = f"{urlsplit(url).scheme}://{urlsplit(url).netloc}"
        assert all(m.replace("&amp;", "&").startswith(origin) for m in media)
        video = media[0].replace("&amp;", "&")
        part = await client.get(video, headers={"Range": "bytes=0-3"})
        assert part.status_code in (200, 206)
        assert part.content.startswith(b"MP4-")
        download = next(m.replace("&amp;", "&") for m in media if "attachment" in m)
        got = await client.get(download)
        assert got.status_code == 200
        assert 'filename="phien-1.mp4"' in got.headers["content-disposition"]

        # Đổi 1 ký tự token trong đường dẫn → không mở (chữ ký sai / không có đối tượng).
        parts = urlsplit(url)
        token = link.object_prefix.removeprefix("share/").removesuffix("/")
        bad_token = token[:10] + _flip(token[10]) + token[11:]
        bad_path = parts.path.replace(token, bad_token)
        assert (await client.get(urlunsplit(parts._replace(path=bad_path)))).status_code in (403, 404)
        # Đổi 1 ký tự chữ ký → 403.
        query = dict(parse_qsl(parts.query))
        sig = query["X-Amz-Signature"]
        query["X-Amz-Signature"] = _flip(sig[0]) + sig[1:]
        assert (await client.get(urlunsplit(parts._replace(query=urlencode(query))))).status_code == 403
        # Không liệt kê được bucket: không ký, và dùng lại chữ ký của index.html.
        bucket_url = f"{S3_ENDPOINT}/{store.bucket}"
        assert (
            await client.get(bucket_url, params={"list-type": "2", "prefix": "share/"})
        ).status_code == 403
        assert (
            await client.get(f"{bucket_url}/{link.object_prefix}index.html")
        ).status_code == 403  # thiếu chữ ký
        listed = await client.get(f"{bucket_url}?list-type=2&prefix=share/&{parts.query}")
        assert listed.status_code == 403
        assert link.object_prefix not in listed.text

        # Thu hồi → J-25 ngay → link chết ≤ 60 giây (đồng hồ thật).
        started = time.monotonic()
        res = await share_api.post(f"/api/v1/shares/{share_id}/revoke", headers=headers)
        assert res.status_code == 200
        await cleanup.cleanup(db, share_settings, link.id)
        dead = await client.get(url)
        elapsed = time.monotonic() - started
        assert dead.status_code == 404
        assert "NoSuchKey" in dead.text
        assert elapsed < 60
        assert (await client.get(video)).status_code == 404

    versions = _root().list_object_versions(Bucket=store.bucket, Prefix=link.object_prefix)
    assert versions.get("Versions", []) == []
    assert versions.get("DeleteMarkers", []) == []
    link = await db.get(ShareLink, share_id, populate_existing=True)
    assert link is not None
    assert link.cloud_deleted_at is not None
