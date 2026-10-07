"""API-160..164 + `shares[]` API-31 / API-132 (T-224; FR-07.05, 07.08, 07.09; BR-35, BR-39; AC-52, AC-53).

An toàn bằng chứng: link chỉ chứa phiên được chọn thuộc `evidence[]` đang dùng của đúng hồ sơ; phiên bị loại /
cần soát không chọn sẵn; ảnh chỉ của phiên được chọn; Cam 1 không `READY` → không tạo được.
"""

from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.shares.models import ShareItem, ShareLink

from .factories import make_user
from .shares_fixtures import NOW, ShareWorld, login, make_share_world, share_api, share_settings, share_store

__all__ = ["share_api", "share_settings", "share_store"]

pytestmark = pytest.mark.integration


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
        "recipient": "  CSKH Shopee –   phiếu 98765 ",
        "expires_days": 3,
    }
    base.update(kw)
    return base


async def test_options_claim_order_flags_and_defaults(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.get("/api/v1/shares/options", headers=headers, params={"claim_id": str(w.claim.id)})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["storage_configured"] is True
    assert body["source"]["type"] == "CLAIM"
    assert body["source"]["claim_code"] == w.claim.code
    assert body["source"]["tracking_number"] == w.package.tracking_number
    assert body["limits"] == {"max_sessions": 4, "max_total_seconds": 1800, "max_snapshots": 20}
    rows = {r["id"]: r for r in body["sessions"]}
    ids = [r["id"] for r in body["sessions"]]
    # Bằng chứng đã bỏ (BR-38) không có; phiên chính đứng đầu.
    assert str(w.removed.id) not in rows
    assert ids[0] == str(w.ret_a.id)
    assert rows[str(w.ret_a.id)]["primary"] is True
    assert set(ids) == {str(s.id) for s in (w.pack, w.ret_a, w.ret_b, w.wrong, w.review, w.failed)}
    # BR-39: phiên bị loại / cần soát không chọn sẵn (DEC-668); clip lỗi không chọn được (EX-S3).
    assert rows[str(w.wrong.id)]["excluded"] is True
    assert rows[str(w.wrong.id)]["default_selected"] is False
    assert rows[str(w.review.id)]["review_needed"] is True
    assert rows[str(w.review.id)]["default_selected"] is False
    assert rows[str(w.failed.id)]["selectable"] is False
    assert rows[str(w.failed.id)]["unavailable_reason"] == "CLIP_FAILED"
    defaults = [r["id"] for r in body["sessions"] if r["default_selected"]]
    assert defaults == [str(w.ret_a.id), str(w.pack.id), str(w.ret_b.id)]
    assert rows[str(w.ret_b.id)]["cameras"] == ["CAM1"]  # Cam 2 lỗi → bản ghép chỉ Cam 1
    assert rows[str(w.ret_b.id)]["duration_s"] == 120
    assert rows[str(w.ret_a.id)]["snapshot_count"] == 2
    assert rows[str(w.wrong.id)]["snapshot_count"] == 1
    assert body["snapshot_count"] == 3


async def test_options_session_source_and_errors(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    headers, _ = await login(share_api, db, "SUPERVISOR")
    res = await share_api.get(
        "/api/v1/shares/options", headers=headers, params={"session_id": str(w.ret_a.id)}
    )
    assert res.status_code == 200
    body = res.json()
    assert body["source"]["type"] == "SESSION"
    assert body["source"]["claim_id"] is None
    assert [r["default_selected"] for r in body["sessions"]] == [True]
    assert body["snapshot_count"] == 2
    both = {"claim_id": str(w.claim.id), "session_id": str(w.ret_a.id)}
    assert (await share_api.get("/api/v1/shares/options", headers=headers, params=both)).status_code == 422
    assert (await share_api.get("/api/v1/shares/options", headers=headers)).status_code == 422
    missing = {"session_id": "0192aaaa-0000-7000-8000-000000000000"}
    res = await share_api.get("/api/v1/shares/options", headers=headers, params=missing)
    assert res.status_code == 404


async def test_options_storage_not_configured(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.get("/api/v1/shares/options", headers=headers, params={"claim_id": str(w.claim.id)})
    assert res.json()["storage_configured"] is False  # EX-S1
    res = await share_api.post("/api/v1/shares", headers=headers, json=_body(w, [w.ret_a.id]))
    assert res.status_code == 503
    assert res.json()["error"]["code"] == "CLOUD_NOT_CONFIGURED"
    assert (await db.scalar(select(ShareLink).where(ShareLink.claim_id == w.claim.id))) is None


async def test_create_records_exact_selection(
    share_api: AsyncClient,
    db: AsyncSession,
    w: ShareWorld,
    share_store: object,
    sent_jobs: list[tuple[str, list[Any], str, float]],
) -> None:
    headers, uid = await login(share_api, db, "CSKH")
    res = await share_api.post(
        "/api/v1/shares", headers=headers, json=_body(w, [w.pack.id, w.ret_a.id, w.wrong.id])
    )
    assert res.status_code == 202, res.text
    assert res.json()["status"] == "CREATING"
    link = await db.get(ShareLink, res.json()["id"])
    assert link is not None
    assert link.status == "CREATING"
    assert link.claim_id == w.claim.id
    assert link.package_id == w.package.id
    assert link.recipient == "CSKH Shopee – phiếu 98765"  # trim + gộp khoảng trắng
    assert link.expires_at == NOW + timedelta(days=3)
    # Token 256 bit: `share/` + 43 ký tự base64url + `/` (NFR-42).
    token = link.object_prefix.removeprefix("share/").removesuffix("/")
    assert len(token) == 43
    assert link.object_prefix.startswith("share/")
    assert "/" not in token
    items = (
        await db.scalars(select(ShareItem).where(ShareItem.share_id == link.id).order_by(ShareItem.ord))
    ).all()
    # Thứ tự: phiên chính trước, rồi theo giờ bắt đầu.
    assert [i.session_id for i in items] == [w.ret_a.id, w.pack.id, w.wrong.id]
    # Ảnh chỉ của phiên được chọn, có trong bằng chứng, READY (DEC-667) — không có ảnh của phiên đã bỏ.
    assert set(items[0].snapshot_ids) == {s.id for s in w.snaps["ret_a"]}
    assert items[1].snapshot_ids == []
    assert items[2].snapshot_ids == [w.snaps["wrong"][0].id]
    assert sent_jobs[-1][:3] == ("shares.build", [str(link.id)], "export")
    entry = await db.scalar(
        select(AuditLog).where(AuditLog.action == "SHARE_CREATE", AuditLog.object_id == str(link.id))
    )
    assert entry is not None
    assert entry.user_id == uid
    assert entry.data is not None
    assert entry.data["session_count"] == 3
    assert token not in str(entry.data)
    assert "share/" not in str(entry.data)  # không token / URL trong audit


async def test_create_without_snapshots_and_session_source(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    headers, _ = await login(share_api, db, "ADMIN")
    res = await share_api.post(
        "/api/v1/shares", headers=headers, json=_body(w, [w.ret_a.id], include_snapshots=False)
    )
    assert res.status_code == 202
    item = await db.scalar(select(ShareItem).where(ShareItem.share_id == res.json()["id"]))
    assert item is not None
    assert item.snapshot_ids == []
    body = {
        "source_type": "SESSION",
        "session_id": str(w.ret_b.id),
        "session_ids": [str(w.ret_b.id)],
        "layout": "CAM1",
        "include_snapshots": True,
        "recipient": "Bưu cục Q7",
        "expires_days": 1,
    }
    res = await share_api.post("/api/v1/shares", headers=headers, json=body)
    assert res.status_code == 202
    link = await db.get(ShareLink, res.json()["id"])
    assert link is not None
    assert link.claim_id is None
    assert link.source_type == "SESSION"
    # Nguồn phiên: chỉ đúng phiên đó.
    body["session_ids"] = [str(w.ret_b.id), str(w.ret_a.id)]
    res = await share_api.post("/api/v1/shares", headers=headers, json=body)
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"]["session_ids"] == "Phiên không thuộc hồ sơ này."


@pytest.mark.parametrize(
    ("patch", "field", "message"),
    [
        ({"session_ids": []}, "session_ids", "Chọn ít nhất 1 phiên."),
        ({"recipient": " ab "}, "recipient", "Ghi rõ gửi cho ai (3–100 ký tự)."),
        ({"recipient": "x" * 101}, "recipient", "Ghi rõ gửi cho ai (3–100 ký tự)."),
        ({"expires_days": 2}, "expires_days", "Hạn link chỉ 1, 3 hoặc 7 ngày."),
    ],
)
async def test_create_validation(
    share_api: AsyncClient,
    db: AsyncSession,
    w: ShareWorld,
    share_store: object,
    patch: dict[str, Any],
    field: str,
    message: str,
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    res = await share_api.post("/api/v1/shares", headers=headers, json={**_body(w, [w.ret_a.id]), **patch})
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"][field] == message


async def test_create_limits_and_unavailable(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    headers, _ = await login(share_api, db, "CSKH")
    five = [w.pack.id, w.ret_a.id, w.ret_b.id, w.wrong.id, w.review.id]
    res = await share_api.post("/api/v1/shares", headers=headers, json=_body(w, five))
    assert res.json()["error"]["details"]["fields"]["session_ids"] == "Chọn tối đa 4 phiên."  # BR-35
    # Phiên đã bỏ khỏi bằng chứng (BR-38) / phiên kiện khác → không thuộc hồ sơ.
    res = await share_api.post("/api/v1/shares", headers=headers, json=_body(w, [w.removed.id]))
    assert res.status_code == 422
    res = await share_api.post("/api/v1/shares", headers=headers, json=_body(w, [w.failed.id]))
    assert res.status_code == 409
    err = res.json()["error"]
    assert err["code"] == "SESSION_CLIP_UNAVAILABLE"
    assert err["details"] == {"session_id": str(w.failed.id), "reason": "CLIP_FAILED"}
    # Tổng thời lượng > 30 phút.
    for clip in [*await _clips(db, w.ret_a.id), *await _clips(db, w.ret_b.id)]:
        clip.duration_s = 1000  # type: ignore[assignment]
    await db.flush()
    res = await share_api.post("/api/v1/shares", headers=headers, json=_body(w, [w.ret_a.id, w.ret_b.id]))
    assert res.json()["error"]["details"]["fields"]["session_ids"] == "Tổng thời lượng tối đa 30 phút."
    other = await db.scalar(select(ShareLink).where(ShareLink.claim_id == w.claim.id))
    assert other is None  # không tạo dở dang
    res = await share_api.post(
        "/api/v1/shares", headers=headers,
        json=_body(w, [w.ret_a.id], claim_id="0192aaaa-0000-7000-8000-000000000000"),
    )  # fmt: skip
    assert res.status_code == 404


async def _clips(db: AsyncSession, session_id: object) -> list[Any]:
    from aicam.modules.media.models import Clip

    return list((await db.scalars(select(Clip).where(Clip.session_id == session_id))).all())


async def _activate(
    db: AsyncSession, share_id: str, settings: Settings, url: str = "https://s3.test.vn/x"
) -> ShareLink:
    link = await db.get(ShareLink, share_id)
    assert link is not None
    link.status, link.progress = "ACTIVE", 100
    link.url_enc = Cipher(settings.fernet_key).encrypt(url)
    await db.flush()
    return link


async def test_list_get_counts_and_url_only_when_active(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object, share_settings: Settings
) -> None:
    cskh, cskh_id = await login(share_api, db, "CSKH")
    sup, _ = await login(share_api, db, "SUPERVISOR")
    ids = []
    for who in (cskh, cskh, sup, sup):
        res = await share_api.post("/api/v1/shares", headers=who, json=_body(w, [w.ret_a.id]))
        ids.append(res.json()["id"])
    await _activate(db, ids[0], share_settings, "https://s3.test.vn/share/abc/index.html?X-Amz-Signature=1")
    expired = await _activate(db, ids[1], share_settings)
    expired.expires_at = NOW - timedelta(minutes=1)  # J-25 chưa chạy → hiện EXPIRED, không URL
    failed = await db.get(ShareLink, ids[3])
    assert failed is not None
    failed.status, failed.error_code, failed.error_message = "FAILED", "UPLOAD_FAILED", "Không tải được"
    await db.flush()

    res = await share_api.get("/api/v1/shares", headers=cskh)
    body = res.json()
    assert body["counts"] == {"ACTIVE": 2, "REVOKED": 0, "EXPIRED": 1, "ALL": 4}
    assert {i["id"] for i in body["items"]} == {ids[0], ids[2]}
    first = next(i for i in body["items"] if i["id"] == ids[0])
    assert first["url"].endswith("X-Amz-Signature=1")
    assert first["can_revoke"] is True
    assert first["items"] is None
    assert first["session_count"] == 1
    other = next(i for i in body["items"] if i["id"] == ids[2])
    assert other["url"] is None
    assert other["status"] == "CREATING"
    assert other["can_revoke"] is False
    res = await share_api.get("/api/v1/shares", headers=cskh, params={"status": "EXPIRED"})
    assert [i["id"] for i in res.json()["items"]] == [ids[1]]
    assert res.json()["items"][0]["url"] is None
    res = await share_api.get("/api/v1/shares", headers=cskh, params={"status": "ALL", "mine": "true"})
    assert {i["id"] for i in res.json()["items"]} == {ids[0], ids[1]}
    res = await share_api.get("/api/v1/shares", headers=cskh, params={"status": "ALL", "q": w.claim.code})
    assert res.json()["total"] == 4
    res = await share_api.get("/api/v1/shares", headers=cskh, params={"status": "ALL", "q": "phiếu 98"})
    assert res.json()["total"] == 4
    res = await share_api.get("/api/v1/shares", headers=cskh, params={"status": "ALL", "q": "%"})
    assert res.json()["total"] == 0
    detail = (await share_api.get(f"/api/v1/shares/{ids[3]}", headers=cskh)).json()
    assert detail["status"] == "FAILED"
    assert detail["error"] == {"code": "UPLOAD_FAILED", "message": "Không tải được"}
    assert detail["items"][0]["session_id"] == str(w.ret_a.id)
    assert detail["items"][0]["snapshot_count"] == 2
    assert detail["created_by"]["display_name"] == "SUPERVISOR QA"
    assert "object_prefix" not in detail
    assert "share/" not in str({k: v for k, v in detail.items() if k != "url"})
    assert cskh_id is not None
    res = await share_api.get("/api/v1/shares/0192aaaa-0000-7000-8000-000000000000", headers=cskh)
    assert res.status_code == 404


async def test_revoke_permissions_and_state(
    share_api: AsyncClient,
    db: AsyncSession,
    w: ShareWorld,
    share_store: object,
    share_settings: Settings,
    sent_jobs: list[tuple[str, list[Any], str, float]],
) -> None:
    cskh, cskh_id = await login(share_api, db, "CSKH")
    cskh2, _ = await login(share_api, db, "CSKH")
    admin, admin_id = await login(share_api, db, "ADMIN")
    mine = (await share_api.post("/api/v1/shares", headers=cskh, json=_body(w, [w.ret_a.id]))).json()["id"]
    await _activate(db, mine, share_settings)
    res = await share_api.post(f"/api/v1/shares/{mine}/revoke", headers=cskh2)
    assert res.status_code == 403  # CSKH thu hồi link người khác (FR-07.08)
    res = await share_api.post(f"/api/v1/shares/{mine}/revoke", headers=cskh)
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["status"] == "REVOKED"
    assert out["revoke_pending"] is True
    assert out["url"] is None
    assert out["revoked_by"]["id"] == str(cskh_id)
    assert out["can_revoke"] is False
    assert sent_jobs[-1][:3] == ("shares.cleanup", [mine], "default")
    res = await share_api.post(f"/api/v1/shares/{mine}/revoke", headers=cskh)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "SHARE_NOT_ACTIVE"
    entry = await db.scalar(
        select(AuditLog).where(AuditLog.action == "SHARE_REVOKE", AuditLog.object_id == mine)
    )
    assert entry is not None
    assert entry.user_id == cskh_id
    # ADMIN thu hồi link CSKH tạo (kể cả khi còn CREATING).
    other = (await share_api.post("/api/v1/shares", headers=cskh, json=_body(w, [w.ret_a.id]))).json()["id"]
    res = await share_api.post(f"/api/v1/shares/{other}/revoke", headers=admin)
    assert res.status_code == 200
    assert res.json()["revoked_by"]["id"] == str(admin_id)
    assert (await share_api.post("/api/v1/shares/0192aaaa-0000-7000-8000-000000000000/revoke",
                                 headers=admin)).status_code == 404  # fmt: skip


async def test_package_and_claim_shares_blocks(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object, share_settings: Settings
) -> None:
    cskh, _ = await login(share_api, db, "CSKH")
    ids = []
    for _ in range(5):
        clock.freeze(clock.now() + timedelta(minutes=1))
        ids.append(
            (await share_api.post("/api/v1/shares", headers=cskh, json=_body(w, [w.ret_a.id]))).json()["id"]
        )
    await _activate(db, ids[4], share_settings)
    newest_failed = await db.get(ShareLink, ids[3])
    assert newest_failed is not None
    newest_failed.status = "FAILED"
    await db.flush()
    claim = (await share_api.get(f"/api/v1/claims/{w.claim.id}", headers=cskh)).json()
    assert [s["id"] for s in claim["shares"]] == [ids[4], ids[2], ids[1]]  # ≤ 3, mới nhất, bỏ FAILED
    assert claim["shares_active_count"] == 4
    assert claim["shares"][0]["url"] == "https://s3.test.vn/x"
    assert claim["shares"][0]["can_revoke"] is True
    assert all(x["revoke_pending"] is False for x in claim["shares"])
    # EX-S7: thu hồi khi kho mất mạng → `revoke_pending` ở khối D4 / D17 (FE DEC-702) tới khi J-25 xóa xong.
    res = await share_api.post(f"/api/v1/shares/{ids[4]}/revoke", headers=cskh)
    assert res.status_code == 200
    claim = (await share_api.get(f"/api/v1/claims/{w.claim.id}", headers=cskh)).json()
    assert claim["shares"][0]["status"] == "REVOKED"
    assert claim["shares"][0]["revoke_pending"] is True
    assert claim["shares_active_count"] == 3
    package = (await share_api.get(f"/api/v1/packages/{w.package.id}", headers=cskh)).json()
    assert package["shares"][0]["revoke_pending"] is True
    assert [s["id"] for s in package["shares"]] == [ids[4], ids[2], ids[1]]
    assert package["shares_active_count"] == 3


async def test_roles(share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object) -> None:
    from aicam.modules.users.permissions import PERMISSIONS

    assert {"shares.create", "shares.read", "shares.revoke_any"} <= set(PERMISSIONS["ADMIN"])
    assert {"shares.create", "shares.read", "shares.revoke_any"} <= set(PERMISSIONS["SUPERVISOR"])
    assert {"shares.create", "shares.read"} <= set(PERMISSIONS["CSKH"])
    assert "shares.revoke_any" not in PERMISSIONS["CSKH"]
    station_user = await make_user(db, "tst_share_station", "STATION")
    assert station_user.role == "STATION"
    res = await share_api.get("/api/v1/shares")
    assert res.status_code == 401


async def test_session_source_excluded_or_review_needed_409_g3_fe1(
    share_api: AsyncClient, db: AsyncSession, w: ShareWorld, share_store: object
) -> None:
    """G3-FE-1: API-160 nguồn PHIÊN (D4) cho phiên RETURN bị loại BR-39 (hủy WRONG_SCAN / đánh dấu quét nhầm)
    hoặc "Cần soát" → 409 `SESSION_EXCLUDED` `details.session_id`; xác nhận "Là phiên hoàn thật"
    (`review_confirmed_at`) → tạo được. Nguồn hồ sơ giữ nguyên (thêm tay có chủ đích)."""
    headers, _ = await login(share_api, db, "ADMIN")

    def body(sid: Any) -> dict[str, Any]:
        return {"source_type": "SESSION", "session_id": str(sid), "session_ids": [str(sid)], "layout": "CAM1",
                "include_snapshots": False, "recipient": "Bưu cục Q7", "expires_days": 1}  # fmt: skip

    for s in (w.wrong, w.review):
        res = await share_api.post("/api/v1/shares", headers=headers, json=body(s.id))
        assert res.status_code == 409, res.text
        err = res.json()["error"]
        assert (err["code"], err["details"]["session_id"]) == ("SESSION_EXCLUDED", str(s.id))
    w.ret_a.wrong_scan_at, w.ret_a.wrong_scan_code = NOW, "WRONG_SCAN"  # API-189 MARK
    await db.flush()
    res = await share_api.post("/api/v1/shares", headers=headers, json=body(w.ret_a.id))
    assert res.status_code == 409
    for s in (w.wrong, w.review):
        s.review_confirmed_at = NOW  # CONFIRM_RETURN
    await db.flush()
    for s in (w.wrong, w.review):
        res = await share_api.post("/api/v1/shares", headers=headers, json=body(s.id))
        assert res.status_code == 202, res.text
    res = await share_api.post("/api/v1/shares", headers=headers, json=_body(w, [w.wrong.id]))
    assert res.status_code == 202  # nguồn hồ sơ: phiên loại đã được thêm tay vào bằng chứng
