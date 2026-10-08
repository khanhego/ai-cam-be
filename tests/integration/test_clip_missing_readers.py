"""`MISSING` ở mọi điểm đọc (T-286; 02a §5.2 #2–#14; FR-02.16, EX-K8, EX-K9; DEC-520, 524).

Clip / ảnh `MISSING` = DB có dòng nhưng máy chủ không có tệp: không phát, không giữ, không cắt lại đè, không
vào link / gói (ghi thiếu rõ ràng), không lỗi serialize; retention / J-11 / J-21 không đụng.
"""

import uuid
from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import sign
from aicam.core.settings import Settings
from aicam.modules.claims import pack as packs
from aicam.modules.claims.models import EvidencePack
from aicam.modules.media import service as media
from aicam.modules.media import signing
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.sessions.models import PackSession

from .shares_fixtures import NOW, ShareWorld, login, make_share_world, share_api, share_settings, share_store
from .test_shares_jobs import fake_render

__all__ = ["share_api", "share_settings", "share_store"]

pytestmark = pytest.mark.integration


@pytest.fixture
async def w(db: AsyncSession, share_settings: Settings) -> ShareWorld:
    return await make_share_world(db, share_settings)


async def _clips(db: AsyncSession, s: PackSession) -> dict[str, Clip]:
    return {c.camera_role: c for c in (await db.scalars(select(Clip).where(Clip.session_id == s.id))).all()}


async def _mark_missing(db: AsyncSession, w: ShareWorld) -> tuple[Clip, Snapshot]:
    clip = (await _clips(db, w.ret_a))["CAM1"]
    clip.status = "MISSING"
    snap = await db.get(Snapshot, w.snaps["ret_a"][0].id)
    assert snap is not None
    snap.status = "MISSING"
    await db.flush()
    return clip, snap


async def test_api_132_and_31_serialize_missing(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    """§5.2 #2: Literal có `MISSING` → 200; ảnh `MISSING` → `url = null`, `protection = null`."""
    clip, snap = await _mark_missing(db, w)
    pack_close = Snapshot(
        session_id=w.pack.id, kind="PACK_CLOSE", camera_role="CAM1", taken_at=w.pack.started_at,
        path="snapshots/pack.jpg", sha256="ab" * 32, size_bytes=1, status="MISSING",
    )  # fmt: skip
    db.add(pack_close)
    await db.flush()
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.get(f"/api/v1/claims/{w.claim.id}", headers=headers)
    assert res.status_code == 200, res.text
    clips = [c for e in res.json()["evidence"] if e["session"] for c in e["session"]["clips"]]
    assert next(c for c in clips if c["id"] == str(clip.id))["status"] == "MISSING"
    snaps = [e["snapshot"] for e in res.json()["evidence"] if e["snapshot"]]
    missing = next(s for s in snaps if s["id"] == str(snap.id))
    assert missing["status"] == "MISSING"
    assert missing["url"] is None
    res = await share_api.get(f"/api/v1/packages/{w.package.id}", headers=headers)
    assert res.status_code == 200, res.text
    sessions = {s["id"]: s for s in res.json()["sessions"]}
    manual = next(x for x in sessions[str(w.ret_a.id)]["snapshots"] if x["id"] == str(snap.id))
    assert manual["status"] == "MISSING"
    assert manual["url"] is None
    assert manual["protection"] is None
    assert sessions[str(w.pack.id)]["pack_snapshot"] == {
        "id": str(pack_close.id),
        "url": None,
        "status": "MISSING",
    }
    ret_clips = {c["camera_role"]: c for c in sessions[str(w.ret_a.id)]["clips"]}
    assert ret_clips["CAM1"]["status"] == "MISSING"


async def test_play_hold_export_rebuild_refuse_missing(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_settings: Settings, share_store: object
) -> None:
    """§5.2 #5–#8: API-40 / 41 / 42 / 43 → 409 `CLIP_NOT_READY` `MISSING`; API-46 → 409 `CLIP_NOT_FAILED`."""
    clip, _ = await _mark_missing(db, w)
    admin, admin_id = await login(share_api, db, "ADMIN")
    res = await share_api.get(f"/api/v1/clips/{clip.id}/play-url", headers=admin)
    assert res.status_code == 409
    err = res.json()["error"]
    assert err["code"] == "CLIP_NOT_READY"
    assert err["details"]["status"] == "MISSING"
    assert err["message"] == "Thiếu tệp clip trên máy chủ — không phát được."
    exp = signing.expiry(600)
    sig = sign(share_settings.media_signing_key, signing.clip_message(clip.id, admin_id, exp))
    res = await share_api.get(
        f"/api/v1/media/clips/{clip.id}", params={"uid": str(admin_id), "exp": exp, "sig": sig}
    )
    assert res.status_code == 409
    assert res.json()["error"]["details"]["status"] == "MISSING"
    res = await share_api.put(f"/api/v1/clips/{clip.id}/hold", headers=admin, json={"held": True})
    assert res.status_code == 409
    assert res.json()["error"]["details"]["status"] == "MISSING"
    res = await share_api.post(
        f"/api/v1/sessions/{w.ret_a.id}/exports", headers=admin, json={"layout": "CAM1"}
    )
    assert res.status_code == 409
    assert res.json()["error"]["details"] == {"status": "MISSING", "camera_role": "CAM1"}
    res = await share_api.post(f"/api/v1/sessions/{w.ret_a.id}/clips/rebuild", headers=admin)
    assert res.status_code == 409
    err = res.json()["error"]
    assert err["code"] == "CLIP_NOT_FAILED"
    assert err["details"]["status"] == "MISSING"
    assert err["message"] == "Clip thiếu tệp trên máy chủ — không cắt lại được."
    # Phiên không có clip lỗi, không MISSING → 409 như cũ, kèm trạng thái.
    res = await share_api.post(f"/api/v1/sessions/{w.pack.id}/clips/rebuild", headers=admin)
    assert res.status_code == 409
    assert res.json()["error"]["details"]["status"] == "READY"
    # Phiên có cả clip lỗi: chỉ cắt lại clip FAILED (Cam 2 ret_b), không đụng clip khác.
    res = await share_api.post(f"/api/v1/sessions/{w.ret_b.id}/clips/rebuild", headers=admin)
    assert res.status_code == 202
    assert (await db.get(Clip, clip.id, populate_existing=True)).status == "MISSING"  # type: ignore[union-attr]


async def test_j01_j11_j02_leave_missing_untouched(
    db: AsyncSession, w: ShareWorld, share_settings: Settings, sent_jobs: list[Any]
) -> None:
    """§5.2 #3, #4, #12: J-01 không cắt lại đè, J-11 không đẩy lại, J-02 không xóa dòng `MISSING`."""
    clips = await _clips(db, w.wrong)
    for c in clips.values():
        c.status = "MISSING"
    await db.flush()
    before = {c.id: (c.path, c.sha256) for c in clips.values()}
    result = await media.build_session_clips(db, w.wrong.id, share_settings, final=True)
    assert result.ready == []
    after = await _clips(db, w.wrong)
    assert {c.id: (c.path, c.sha256) for c in after.values()} == before
    assert all(c.status == "MISSING" for c in after.values())
    missing = await media.sessions_missing_clips(db, timedelta(minutes=5), timedelta(days=30))
    assert w.wrong.id not in missing
    clock.freeze(NOW + timedelta(days=400))  # mọi clip quá hạn giữ
    await media.enforce_retention(db, share_settings)
    assert all(
        c.status == "MISSING" for c in (await _clips(db, w.wrong)).values()
    )  # không đổi dòng MISSING (không tệp để xóa)


async def test_j16_pack_lists_missing(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_settings: Settings, share_store: object
) -> None:
    """§5.2 #9: gói bằng chứng ghi thiếu `CLIP_MISSING` / `SNAPSHOT_MISSING` (README giải thích)."""
    clip, snap = await _mark_missing(db, w)
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.post(f"/api/v1/claims/{w.claim.id}/evidence-packs", headers=headers)
    assert res.status_code == 202, res.text
    pack_id = uuid.UUID(res.json()["id"])
    assert await packs.build_evidence_pack(db, pack_id, share_settings, render=fake_render) == "READY"
    pack = await db.get(EvidencePack, pack_id, populate_existing=True)
    assert pack is not None
    reasons = {(m["session_id"], m["camera_role"], m["reason"]) for m in pack.missing}
    assert (str(w.ret_a.id), "CAM1", "CLIP_MISSING") in reasons
    assert any(
        m["reason"] == "SNAPSHOT_MISSING" and m.get("snapshot_id") == str(snap.id) for m in pack.missing
    )
    import zipfile

    with zipfile.ZipFile(share_settings.video_root / (pack.path or "")) as zf:
        readme = next(n for n in zf.namelist() if n.endswith("README.txt"))
        assert "Thiếu tệp" in zf.read(readme).decode()
        assert not any(n.endswith(f"{w.snaps['ret_a'][0].id}.jpg") for n in zf.namelist())
    assert clip.status == "MISSING"


async def test_shares_refuse_missing_cam1(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    """§5.2 #10: API-164 `CLIP_MISSING`, API-160 → 409 `SESSION_CLIP_UNAVAILABLE` `reason = CLIP_MISSING`."""
    await _mark_missing(db, w)
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.get("/api/v1/shares/options", headers=headers, params={"claim_id": str(w.claim.id)})
    row = next(s for s in res.json()["sessions"] if s["id"] == str(w.ret_a.id))
    assert row["selectable"] is False
    assert row["unavailable_reason"] == "CLIP_MISSING"
    assert row["default_selected"] is False
    assert row["snapshot_count"] == 1  # ảnh MISSING không đếm
    body = {
        "source_type": "CLAIM", "claim_id": str(w.claim.id), "session_ids": [str(w.ret_a.id)],
        "layout": "CAM1", "include_snapshots": True, "recipient": "CSKH Shopee", "expires_days": 7,
    }  # fmt: skip
    res = await share_api.post("/api/v1/shares", headers=headers, json=body)
    assert res.status_code == 409
    assert res.json()["error"]["details"] == {"session_id": str(w.ret_a.id), "reason": "CLIP_MISSING"}


async def test_backup_queue_skips_missing(db: AsyncSession, w: ShareWorld, share_settings: Settings) -> None:
    """§5.2 #14: J-21 không xếp clip / ảnh `MISSING`."""
    from aicam.modules.media import protection

    clip, snap = await _mark_missing(db, w)
    cutoff = clock.now() - timedelta(days=90)
    clip_ids = {r[0] for r in (await db.execute(protection.evidence_clip_targets(clock.now(), cutoff))).all()}
    snap_ids = {
        r[0] for r in (await db.execute(protection.evidence_snapshot_targets(clock.now(), cutoff))).all()
    }
    assert clip.id not in clip_ids
    assert snap.id not in snap_ids
    assert (await _clips(db, w.ret_a))["CAM2"].id in clip_ids  # cùng phiên, clip READY vẫn được xếp
