"""J-24 dựng + tải link, J-25 hết hạn / thu hồi / treo (T-225; FR-07.05, 07.07, 07.08; BR-34; NFR-42).

Kho link `MemoryStore` (URL ký giả `https://s3.test.vn/...`); encode video thay bằng renderer giả (máy dev
thiếu `drawtext` — encode thật ở QA live trong `worker-export`). An toàn: link chỉ có đối tượng của phiên /
ảnh đã chốt; tệp gốc lệch / mất → không dựng; thu hồi giữa lúc dựng → không bao giờ `ACTIVE`, đối tượng
bị xóa.
"""

import asyncio
import hashlib
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import UNREACHABLE, CloudError, MemoryStore
from aicam.modules.media import ffmpeg
from aicam.modules.media.exports import Rendered
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.sessions.models import PackSession
from aicam.modules.shares import build, cleanup
from aicam.modules.shares.models import ShareItem, ShareLink

from .shares_fixtures import NOW, ShareWorld, login, make_share_world, share_api, share_settings

__all__ = ["share_api", "share_settings"]

pytestmark = pytest.mark.integration


class SignedStore(MemoryStore):
    """MemoryStore + URL ký dạng S3 path-style (W1 cần origin `https://`)."""

    def presign_get(
        self, key: str, expires_s: int, *, filename: str | None = None, content_type: str | None = None
    ) -> str:
        extra = f"&response-content-disposition=attachment%3B%20filename%3D{filename}" if filename else ""
        return f"https://s3.test.vn/{self.bucket}/{key}?X-Amz-Expires={expires_s}{extra}&X-Amz-Signature=sig"


@pytest.fixture
def store() -> Any:
    s = SignedStore("test-share", versioning=False)
    cloud.use_store(cloud.SHARE, s)
    yield s
    cloud.use_store(cloud.SHARE, None)


async def fake_render(
    db: AsyncSession, pack: PackSession, sources: list[Clip], video: Path, settings: Settings, progress: Any
) -> Rendered:
    video.parent.mkdir(parents=True, exist_ok=True)
    video.write_bytes(b"MP4-" + str(pack.id).encode() + b"".join(c.camera_role.encode() for c in sources))  # noqa: ASYNC240
    if progress is not None:
        await progress(50)
    return Rendered(ffmpeg.sha256_file(video), pack.started_at, pack.ended_at or pack.started_at, [])


@pytest.fixture
async def w(db: AsyncSession, share_settings: Settings) -> ShareWorld:
    return await make_share_world(db, share_settings)


def _body(w: ShareWorld, ids: list[Any], **kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "source_type": "CLAIM",
        "claim_id": str(w.claim.id),
        "session_ids": [str(i) for i in ids],
        "layout": "SIDE_BY_SIDE",
        "include_snapshots": True,
        "recipient": "CSKH Shopee – phiếu 98765",
        "expires_days": 3,
    }
    base.update(kw)
    return base


async def _create(
    api: AsyncClient, headers: dict[str, str], w: ShareWorld, ids: list[Any], **kw: Any
) -> ShareLink:
    res = await api.post("/api/v1/shares", headers=headers, json=_body(w, ids, **kw))
    assert res.status_code == 202, res.text
    return ShareLink(id=res.json()["id"])


async def _link(db: AsyncSession, share_id: Any) -> ShareLink:
    link = await db.get(ShareLink, share_id, populate_existing=True)
    assert link is not None
    return link


async def test_build_publishes_only_selected_evidence(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id, w.ret_b.id, w.wrong.id])
    assert await build.build(db, created.id, share_settings, render=fake_render) == "ACTIVE"
    link = await _link(db, created.id)
    prefix = link.object_prefix
    assert link.status == "ACTIVE"
    assert link.progress == 100
    assert link.step is None
    # Đúng đối tượng: 3 video + 2 ảnh ret_a + 1 ảnh wrong (phiên được chọn) + index — không ảnh phiên đã bỏ.
    assert sorted(store.keys(prefix)) == sorted(
        f"{prefix}{k}"
        for k in ("v1.mp4", "v2.mp4", "v3.mp4", "p1-1.jpg", "p1-2.jpg", "p2-1.jpg", "index.html")
    )
    assert store.keys() == store.keys(prefix)  # không ghi gì ngoài thư mục link
    assert store.headers(f"{prefix}index.html") == ("text/html; charset=utf-8", "no-store")
    # G3-SH-4: video / ảnh qua URL ký — không cho proxy / CDN dùng chung bộ nhớ đệm
    assert store.headers(f"{prefix}v1.mp4") == ("video/mp4", "private, no-store")
    assert store.headers(f"{prefix}p1-1.jpg") == ("image/jpeg", "private, no-store")
    assert (
        store.raw(f"{prefix}p1-1.jpg")
        == (share_settings.video_root / (w.snaps["ret_a"][0].path or "")).read_bytes()
    )
    url = Cipher(share_settings.fernet_key).decrypt(link.url_enc or b"")
    assert url.startswith(f"https://s3.test.vn/test-share/{prefix}index.html?")
    assert f"X-Amz-Expires={3 * 86400}" in url  # URL ký hạn = hạn link (BR-34)
    html = store.raw(f"{prefix}index.html").decode()
    assert '<meta name="referrer" content="no-referrer">' in html
    assert "img-src https://s3.test.vn;" in html
    assert "<script" not in html
    assert w.package.tracking_number in html
    assert "2410TST00061" in html
    for secret in ("CSKH Shopee", "phiếu 98765", "CSKH QA", w.claim.code, "Nhầm kiện"):
        assert secret not in html  # không "gửi cho", người tạo, mã hồ sơ, ghi chú (02 §6.3)
    assert "filename%3Dphien-2.mp4" in html
    items = (
        await db.scalars(select(ShareItem).where(ShareItem.share_id == link.id).order_by(ShareItem.ord))
    ).all()
    clips_b = {
        c.camera_role: c for c in (await db.scalars(select(Clip).where(Clip.session_id == w.ret_b.id)))
    }
    assert [i.session_id for i in items] == [w.ret_a.id, w.wrong.id, w.ret_b.id]  # phiên chính trước
    assert items[2].source_sha256 == {"CAM1": clips_b["CAM1"].sha256}  # Cam 2 lỗi → chỉ Cam 1
    assert items[2].video_sha256 == hashlib.sha256(store.raw(f"{prefix}v3.mp4")).hexdigest()
    assert items[2].size_bytes == len(store.raw(f"{prefix}v3.mp4"))
    order = ("v1.mp4", "p1-1.jpg", "p1-2.jpg", "v2.mp4", "p2-1.jpg", "v3.mp4", "index.html")
    assert link.object_keys == [f"{prefix}{k}" for k in order]
    # API trả URL khi ACTIVE; chạy lại J-24 → SKIPPED.
    detail = (await share_api.get(f"/api/v1/shares/{link.id}", headers=headers)).json()
    assert detail["url"] == url
    assert detail["items"][0]["snapshot_count"] == 2
    assert await build.build(db, link.id, share_settings, render=fake_render) == "SKIPPED"


async def test_source_tampered_or_missing_never_published(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id, w.ret_b.id])
    clip = await db.scalar(select(Clip).where(Clip.session_id == w.ret_b.id, Clip.camera_role == "CAM1"))
    assert clip is not None
    (share_settings.video_root / (clip.path or "")).write_bytes(b"da-bi-sua")  # tệp gốc bị sửa (ADR-008)
    assert await build.build(db, created.id, share_settings, render=fake_render) == "FAILED"
    link = await _link(db, created.id)
    assert link.error_code == "RENDER_FAILED"
    assert link.error_message == build.MESSAGES["RENDER_FAILED"]
    assert link.url_enc is None
    assert store.keys() == []  # v1 đã tải bị xóa — không link dở dang (EX-S2)
    assert link.cloud_deleted_at is not None
    # Clip thành MISSING sau khi tạo link (EX-K9) → không dựng.
    created2 = await _create(share_api, headers, w, [w.ret_a.id])
    cam1 = await db.scalar(select(Clip).where(Clip.session_id == w.ret_a.id, Clip.camera_role == "CAM1"))
    assert cam1 is not None
    cam1.status = "MISSING"
    await db.flush()
    assert await build.build(db, created2.id, share_settings, render=fake_render) == "FAILED"
    assert (await _link(db, created2.id)).error_code == "RENDER_FAILED"
    assert store.keys() == []


async def test_snapshot_no_longer_ready_is_skipped(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id])
    snap = await db.get(Snapshot, w.snaps["ret_a"][0].id)
    assert snap is not None
    snap.status = "MISSING"  # §5.2 #10: J-24 bỏ ảnh ≠ READY
    await db.flush()
    assert await build.build(db, created.id, share_settings, render=fake_render) == "ACTIVE"
    link = await _link(db, created.id)
    assert sorted(store.keys()) == sorted(
        f"{link.object_prefix}{k}" for k in ("v1.mp4", "p1-1.jpg", "index.html")
    )
    item = await db.scalar(select(ShareItem).where(ShareItem.share_id == link.id))
    assert item is not None
    assert item.snapshot_ids == [w.snaps["ret_a"][1].id]


async def test_upload_failure_then_cleanup_after_network_back(
    share_api: AsyncClient,
    db: AsyncSession,
    w: ShareWorld,
    store: SignedStore,
    share_settings: Settings,
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id, w.ret_b.id])
    calls = {"n": 0}

    async def render_then_cut(*args: Any) -> Rendered:
        calls["n"] += 1
        out = await fake_render(*args)
        if calls["n"] == 2:
            store.fail = CloudError(UNREACHABLE)
        return out

    assert await build.build(db, created.id, share_settings, render=render_then_cut) == "FAILED"
    link = await _link(db, created.id)
    assert link.error_code == "UPLOAD_FAILED"
    assert link.error_message == "Không tải được lên kho lưu cloud. Kiểm tra Internet rồi bấm Thử lại."
    assert link.cloud_deleted_at is None  # xóa cũng lỗi → J-25 dọn sau
    assert any(k.startswith(link.object_prefix) for k in store._objects)  # v1 + ảnh còn trên kho
    out = await cleanup.cleanup(db, share_settings)
    assert out["pending"] == 1
    store.fail = None
    out = await cleanup.cleanup(db, share_settings)
    assert out["deleted"] == 1
    assert store.keys() == []
    assert (await _link(db, created.id)).cloud_deleted_at is not None


async def test_revoked_while_building_is_never_active(
    share_api: AsyncClient,
    db: AsyncSession,
    w: ShareWorld,
    store: SignedStore,
    share_settings: Settings,
    sent_jobs: list[tuple[str, list[Any], str, float]],
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id, w.ret_b.id])

    async def render_and_revoke(db_: AsyncSession, pack: PackSession, *rest: Any) -> Rendered:
        out = await fake_render(db_, pack, *rest)
        if pack.id == w.ret_b.id:  # sau khi v1 đã tải lên
            res = await share_api.post(f"/api/v1/shares/{created.id}/revoke", headers=headers)
            assert res.status_code == 200
        return out

    status = await build.build(db, created.id, share_settings, render=render_and_revoke)
    assert status == "REVOKED"
    link = await _link(db, created.id)
    assert link.url_enc is None
    assert store.keys() == []  # v1 đã tải bị xóa
    assert sent_jobs[-1][0] == "shares.cleanup"
    await cleanup.cleanup(db, share_settings, link.id)
    link = await _link(db, created.id)
    assert link.cloud_deleted_at is not None
    detail = (await share_api.get(f"/api/v1/shares/{link.id}", headers=headers)).json()
    assert detail["status"] == "REVOKED"
    assert detail["revoke_pending"] is False


async def test_timeout(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id])

    async def slow(*args: Any) -> Rendered:
        await asyncio.sleep(5)
        return await fake_render(*args)

    share_settings.share_build_timeout_s = 1
    assert await build.build(db, created.id, share_settings, render=slow) == "FAILED"
    link = await _link(db, created.id)
    assert link.error_code == "TIMEOUT"
    assert store.keys() == []


async def test_revoke_and_expiry_delete_cloud_objects(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    """AC-52 (đồng hồ giả): thu hồi → xóa ngay; hết hạn 3 ngày → `EXPIRED` + audit + file mất ≤ 1 giờ."""
    headers, _ = await login(share_api, db, "CSKH")
    a = await _create(share_api, headers, w, [w.ret_a.id])
    b = await _create(share_api, headers, w, [w.pack.id], expires_days=3)
    for sid in (a.id, b.id):
        assert await build.build(db, sid, share_settings, render=fake_render) == "ACTIVE"
    link_a, link_b = await _link(db, a.id), await _link(db, b.id)
    res = await share_api.post(f"/api/v1/shares/{a.id}/revoke", headers=headers)
    assert res.json()["revoke_pending"] is True
    await cleanup.cleanup(db, share_settings, link_a.id)  # J-25 ngay sau API-163
    assert store.keys(link_a.object_prefix) == []
    assert store.keys(link_b.object_prefix) != []  # chỉ đúng thư mục của link bị thu hồi
    res = await share_api.get(f"/api/v1/shares/{a.id}", headers=headers)
    assert res.json()["revoke_pending"] is False
    clock.freeze(NOW + timedelta(days=3, minutes=1))
    out = await cleanup.cleanup(db, share_settings)
    assert out["expired"] == 1
    assert out["deleted"] == 1
    link_b = await _link(db, b.id)
    assert link_b.status == "EXPIRED"
    assert store.keys() == []
    entry = await db.scalar(
        select(AuditLog).where(AuditLog.action == "SHARE_EXPIRE", AuditLog.object_id == str(b.id))
    )
    assert entry is not None
    assert entry.user_id is None
    # Chạy lại: không làm gì thêm (idempotent).
    assert await cleanup.cleanup(db, share_settings) == {
        "expired": 0,
        "stuck_failed": 0,
        "deleted": 0,
        "pending": 0,
    }


async def test_stuck_creating_fails_and_versions_purged(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_settings: Settings
) -> None:
    versioned = SignedStore("test-share-versioned", versioning=True)  # nhà cung cấp buộc versioning
    cloud.use_store(cloud.SHARE, versioned)
    try:
        headers, _ = await login(share_api, db, "CSKH")
        created = await _create(share_api, headers, w, [w.ret_a.id])
        link = await _link(db, created.id)
        import io

        versioned.put_stream(f"{link.object_prefix}v1.mp4", io.BytesIO(b"x"))  # worker chết giữa chừng
        versioned.put_stream(f"{link.object_prefix}v1.mp4", io.BytesIO(b"y"))
        clock.freeze(NOW + timedelta(minutes=16))
        out = await cleanup.cleanup(db, share_settings)
        assert out["stuck_failed"] == 1
        assert out["deleted"] == 1
        link = await _link(db, created.id)
        assert link.status == "FAILED"
        assert link.error_code == "TIMEOUT"
        assert versioned.versions(f"{link.object_prefix}v1.mp4") == []  # mọi phiên bản bị xóa
    finally:
        cloud.use_store(cloud.SHARE, None)


async def test_timeout_leaves_cloud_cleanup_to_j25_g3_sh1(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    """G3-SH-1: hết thời gian dựng → `FAILED TIMEOUT` **không** đặt `cloud_deleted_at` (lời tải đang chạy
    trong luồng có thể ghi xong sau lần xóa) — J-25 xóa lại + kiểm danh sách rỗng rồi mới đặt."""
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id])

    async def slow(*args: Any) -> Rendered:
        await asyncio.sleep(5)
        return await fake_render(*args)

    share_settings.share_build_timeout_s = 1
    assert await build.build(db, created.id, share_settings, render=slow) == "FAILED"
    link = await _link(db, created.id)
    assert (link.error_code, link.cloud_deleted_at) == ("TIMEOUT", None)
    out = await cleanup.cleanup(db, share_settings, created.id)
    assert out["deleted"] == 1
    assert (await _link(db, created.id)).cloud_deleted_at is not None


async def test_fail_deletes_without_holding_row_lock_g3_sh2(
    share_api: AsyncClient,
    db: AsyncSession,
    w: ShareWorld,
    store: SignedStore,
    share_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G3-SH-2: dựng lỗi → xóa đối tượng đã tải **không** giữ khóa dòng link (không transaction mở)."""
    headers, _ = await login(share_api, db, "CSKH")
    created = await _create(share_api, headers, w, [w.ret_a.id, w.ret_b.id])
    calls = {"n": 0}
    in_tx: list[bool] = []
    real_delete = store.delete_prefix

    def spy_delete(prefix: str, *, all_versions: bool = False) -> int:
        in_tx.append(db.in_transaction())
        return real_delete(prefix, all_versions=all_versions)

    monkeypatch.setattr(store, "delete_prefix", spy_delete)

    async def render_then_fail(*args: Any) -> Rendered:
        calls["n"] += 1
        if calls["n"] == 2:
            raise ffmpeg.FFmpegError("hỏng")
        return await fake_render(*args)

    assert await build.build(db, created.id, share_settings, render=render_then_fail) == "FAILED"
    assert in_tx == [False]
    link = await _link(db, created.id)
    assert link.error_code == "RENDER_FAILED"
    assert store.keys(link.object_prefix) == []
