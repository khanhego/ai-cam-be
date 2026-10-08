# ruff: noqa: E501 — docstring tiếng Việt mô tả case dài (như test_migration_0006_0007)
"""Lượt sửa G4 item 03 — test cho các case `04-test-cases.md` còn ⬜ viết được ở mức INT (DEC-974).

Chỉ thêm test. Mỗi test ghi TC trong docstring:
- TC-05.82 (kỳ vọng sửa theo G3-N8 — DEC-973): J-12 TikTok làm mới gặp 5xx → không thử lại, shop vẫn
  `CONNECTED`, lượt J-12 sau làm mới được.
- TC-04.70: API-21 ghi chú biên 4 / 5 (sau trim) / 500 / 501 ký tự.
- TC-08.65: clip bị bỏ ở KN-1 nhưng là bằng chứng của KN-2 đang mở → J-02 giữ.
- TC-02.68: J-22 tải bằng chứng của hồ sơ khiếu nại chưa đóng trước.
- TC-02.96: kho từ chối khi PUT (`AccessDenied` / đầy) → `FAILED`, giãn cách, API-180 `last_error`.
- TC-MS.10: API-32 `CLIP_FAILED` không đếm clip `MISSING`.
- TC-07.55: 22 ảnh `READY` + 1 `MISSING` → API-164 `snapshot_count` 22; link 20 ảnh.
- TC-07.67 (EX-S5): đóng hồ sơ → link vẫn `ACTIVE`, có trong API-132 `shares[]`, thu hồi được.
- TC-07.68 (EX-S6): J-02 xóa clip gốc → video của link (bản dựng riêng) vẫn còn trên kho.
- TC-P3.19: WS `share.updated` qua kênh `ws:dashboard` (Supervisor / CSKH nhận), không qua `ws:admin`.
"""

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from alembic import command
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import sessionmaker
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.schema_guard import SCHEMA_HEAD
from aicam.core.security import AccessClaims, Cipher
from aicam.core.settings import Settings
from aicam.modules.backup import jobs as bk_jobs
from aicam.modules.backup.models import BackupObject
from aicam.modules.claims import review
from aicam.modules.claims import router as claims_router
from aicam.modules.claims import service as claims_service
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.claims.schemas import EvidenceIn, ReviewIn
from aicam.modules.cloud.store import MemoryStore, classify
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Package, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.mock.tiktok import MockTikTokAdapter
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.shares import build
from aicam.modules.shares.models import ShareItem
from aicam.realtime.hub import channels_for

from . import backup_fixtures as bk
from . import shares_fixtures as shares_fx
from . import test_cancel_rule_br37 as br37
from . import test_evidence_remove_br38 as br38
from . import test_migration_0006_0007 as mig
from . import test_share_mark_race as race
from . import test_shares_jobs as shares_jobs
from .backup_fixtures import World, backup_api, backup_settings, memory_store, use_settings, world
from .conftest import alembic_config
from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import Desk, pack_session_with_clips
from .shares_fixtures import (
    ShareWorld,
    add_snapshot,
    make_share_world,
    share_api,
    share_settings,
    share_store,
)
from .test_cancel_rule_br37 import adapter, desk
from .test_evidence_remove_br38 import media_settings
from .test_g4_phase3_gaps import _no_sleep, _seeded_tiktok
from .test_migration_0006_0007 import mig_db
from .test_share_mark_race import committed
from .test_shares_jobs import SignedStore, store

__all__ = [
    "adapter", "backup_api", "backup_settings", "committed", "desk", "media_settings", "memory_store", "mig_db",
    "share_api", "share_settings", "share_store", "store", "world",
]  # fmt: skip

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------- TC-05.82 (DEC-973)


async def test_tc_05_82_j12_refresh_5xx_no_retry_shop_stays_connected(
    db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-05.82 (sửa theo G3-N8): token TikTok A / B (một grant) còn < 1 giờ; J-12 gọi làm mới gặp 502 một lần
    → **không** thử lại trong client (refresh token dùng một lần), `failed = 1`, shop vẫn `CONNECTED`,
    `last_error.code = REFRESH_FAILED`, token cũ giữ nguyên; lượt J-12 sau (sàn hết lỗi) làm mới cả 2 shop,
    vẫn `CONNECTED`."""
    shops = await _seeded_tiktok(db, test_settings)
    a, b = shops["TTMOCKA"], shops["TTMOCKB"]
    a_id, b_id = a.id, b.id
    soon = clock.now() + timedelta(minutes=30)
    for shop in (a, b):
        shop.auth_expires_at = soon
    await db.commit()
    cipher = Cipher(test_settings.fernet_key)
    old_creds = platforms.credentials(a, cipher)
    assert old_creds is not None
    tiktok = MockTikTokAdapter(sleep=_no_sleep)
    real_token = tiktok.data._token
    refresh_calls: list[int] = []

    def flaky_token(path: str, q: dict[str, str]) -> Any:
        import httpx

        if path.endswith("/refresh"):
            refresh_calls.append(1)
            if len(refresh_calls) == 1:
                return httpx.Response(502, json={"code": 50002, "message": "bad gateway (mock)"})
        return real_token(path, q)

    tiktok.data._token = flaky_token  # type: ignore[method-assign]

    out = await sync.refresh_tokens(db, tiktok, test_settings)

    assert out == {"refreshed": 0, "expired": 0, "failed": 1, "skipped": 0}
    assert len(refresh_calls) == 1  # không thử lại
    for shop_id in (a_id, b_id):
        shop = await db.get(Shop, shop_id, populate_existing=True)
        assert shop is not None
        assert shop.auth_status == "CONNECTED"
    a = await db.get(Shop, a_id, populate_existing=True)  # type: ignore[assignment]
    assert a.last_error is not None
    assert a.last_error["code"] == "REFRESH_FAILED"
    assert a.auth_expires_at == soon
    same = platforms.credentials(a, cipher)
    assert same is not None
    assert same.refresh_token == old_creds.refresh_token

    clock.advance(timedelta(minutes=30))  # lượt J-12 kế (30 phút)
    out = await sync.refresh_tokens(db, tiktok, test_settings)

    assert out["refreshed"] == 2, out
    assert out["failed"] == 0
    assert len(refresh_calls) == 2
    for shop_id in (a_id, b_id):
        shop = await db.get(Shop, shop_id, populate_existing=True)
        assert shop is not None
        assert shop.auth_status == "CONNECTED"
        assert shop.auth_expires_at is not None
        assert shop.auth_expires_at > clock.now() + timedelta(days=1)


# ---------------------------------------------------------------- TC-04.70


async def _sup_cancel_request(desk: Desk, db: AsyncSession, n: int, sup: dict[str, str]) -> tuple[str, str]:
    """Mở phiên RETURN mới (kiện `n`), quá 60 giây → yêu cầu hỗ trợ; trả (approval_id, session_id)."""
    session = await br37._open_return(desk, db, n)
    clock.advance(timedelta(minutes=2))
    created = await desk.api.post(
        "/api/v1/station/approval-requests",
        headers=desk.headers,
        json={"type": "ASSIST", "session_id": session["id"]},
    )
    assert created.status_code == 201, created.text
    return created.json()["approval_request"]["id"], session["id"]


async def test_tc_04_70_api21_note_boundaries(desk: Desk, db: AsyncSession) -> None:
    """TC-04.70 (FR-04.14, DEC-447): API-21 `CANCEL_SESSION` phiên RETURN — ghi chú 4 ký tự / 501 ký tự → 422
    `fields.note = "Nhập ghi chú (5–500 ký tự)."`; "  abcde  " (5 sau trim) → 200 (lưu "abcde"); đúng 500 ký
    tự (kể cả có khoảng trắng hai đầu) → 200. Trước sửa: 501 ký tự trả 422 lời nhắn tiếng Anh của pydantic
    "String should have at most 500 characters" (DEC-974)."""
    await make_user(db, "tst_sup_g4fix", "SUPERVISOR")
    login = await desk.api.post(
        "/api/v1/auth/login", json={"username": "tst_sup_g4fix", "password": PASSWORD, "client": "DASHBOARD"}
    )
    sup = {"Authorization": f"Bearer {login.json()['access_token']}"}

    async def decide(approval_id: str, note: str) -> Any:
        return await desk.api.post(
            f"/api/v1/approval-requests/{approval_id}/decision",
            headers=sup,
            json={"action": "CANCEL_SESSION", "note": note, "reason_code": "WRONG_SCAN"},
        )

    approval_id, session_id = await _sup_cancel_request(desk, db, 51, sup)
    for bad in ("abcd", "x" * 501):
        res = await decide(approval_id, bad)
        assert res.status_code == 422, (len(bad), res.text)
        assert res.json()["error"]["details"]["fields"] == {"note": "Nhập ghi chú (5–500 ký tự)."}, len(bad)
    res = await decide(approval_id, "  abcde  ")
    assert res.status_code == 200, res.text
    pack = await db.get(PackSession, uuid.UUID(session_id), populate_existing=True)
    assert pack is not None
    assert (pack.status, pack.note) == ("CANCELLED", "abcde")

    approval_id, session_id = await _sup_cancel_request(desk, db, 52, sup)
    note500 = "Quét nhầm " + "x" * 490
    assert len(note500) == 500
    res = await decide(approval_id, f"  {note500}  ")  # trim trước khi đếm (504 → 500)
    assert res.status_code == 200, res.text
    pack = await db.get(PackSession, uuid.UUID(session_id), populate_existing=True)
    assert pack is not None
    assert (pack.status, pack.note) == ("CANCELLED", note500)


# ---------------------------------------------------------------- TC-08.65 (BR-38)


async def test_tc_08_65_removed_in_one_claim_kept_by_other_open_claim(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-08.65 (BR-38, ADR-009 a): phiên đóng gói (clip 01/05) là bằng chứng của KN-1 và KN-2 (cùng kiện, khác
    loại). KN-1 bỏ bằng chứng 06/10 → hạn giữ theo KN-1 = 04/01/2027; qua hạn đó J-02 **không** xóa vì KN-2
    vẫn mở dùng phiên này. Đối chứng: không có KN-2 thì đúng đêm 04/01/2027 đã xóa
    (`test_remove_is_soft_and_keeps_clip_until_deadline`)."""
    claim1, pack, headers = await br38._claim_with_pack(api, db, media_settings)
    claim2 = await api.post(
        "/api/v1/claims",
        headers=headers,
        json={"package_id": str(pack.package_id), "type": "LOST_IN_TRANSIT", "counterparty": "CARRIER"},
    )
    assert claim2.status_code == 201, claim2.text
    assert {e["session"]["id"] for e in claim2.json()["evidence"] if e["kind"] == "SESSION"} == {str(pack.id)}

    clock.freeze(br38.REMOVED_AT)
    headers = await br38._login(api)
    detail = (await api.get(f"/api/v1/claims/{claim1['id']}", headers=headers)).json()
    res = await api.put(
        f"/api/v1/claims/{claim1['id']}/evidence",
        headers=headers,
        json={"version": detail["version"], "session_ids": [], "snapshot_ids": [], "note": br38.REASON},
    )
    assert res.status_code == 200, res.text
    assert res.json()["evidence"] == []

    for at in (datetime(2027, 1, 4, 19, 0, tzinfo=UTC), datetime(2027, 6, 1, 19, 0, tzinfo=UTC)):
        clock.freeze(at)
        await media.enforce_retention(db, media_settings)
        assert await br38._states(db, pack.id) == ["READY", "READY", "READY"], at
    headers = await br38._login(api)
    kn2 = (await api.get(f"/api/v1/claims/{claim2.json()['id']}", headers=headers)).json()
    assert kn2["status"] == "NEW"
    assert [e["kind"] for e in kn2["evidence"]].count("SESSION") == 1


# ---------------------------------------------------------------- TC-02.68, TC-02.96 (J-22)


async def _loose_packs(db: AsyncSession, w: World, n: int) -> list[PackSession]:
    """Thêm `n` phiên đóng gói không thuộc hồ sơ nào, có tệp clip / ảnh thật trên đĩa."""
    _, station = await make_station_account(db, "tst_bk_extra", "TST BK extra")
    out = []
    for i in range(n):
        package = Package(tracking_number=f"SPXTSTBKX{i:04d}", warehouse_status="PACKED")
        db.add(package)
        await db.flush()
        pack = await pack_session_with_clips(db, station, package, bk.NOW - timedelta(days=2))
        rows = [
            *(await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all(),
            *(await db.scalars(select(Snapshot).where(Snapshot.session_id == pack.id))).all(),
        ]
        for row in rows:
            assert row.path is not None
            content = f"{row.path}-x".encode() * 20
            path = w.settings.video_root / row.path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            row.sha256, row.size_bytes = hashlib.sha256(content).hexdigest(), len(content)
        out.append(pack)
    await db.flush()
    return out


async def _queue(db: AsyncSession, packs: list[PackSession], created_at: datetime) -> list[str]:
    """Xếp `backup_object` cùng lý do `ALL_PACK` cho mọi clip / ảnh của các phiên (tách riêng tiêu chí "hồ sơ
    mở" khỏi tiêu chí lý do `EVIDENCE`); trả `object_key`."""
    keys = []
    for pack in packs:
        rows: list[tuple[str, Any]] = [
            ("CLIP", c) for c in (await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all()
        ] + [
            ("SNAPSHOT", s)
            for s in (await db.scalars(select(Snapshot).where(Snapshot.session_id == pack.id)))
        ]
        for kind, row in rows:
            key = bk_jobs.evidence_key(kind, row.id)
            db.add(
                BackupObject(
                    kind=kind, clip_id=row.id if kind == "CLIP" else None,
                    snapshot_id=row.id if kind == "SNAPSHOT" else None, object_key=key, status="PENDING",
                    sha256=row.sha256, size_bytes=row.size_bytes, next_attempt_at=created_at, reason="ALL_PACK",
                    created_at=created_at, updated_at=created_at,
                )
            )  # fmt: skip
            keys.append(key)
    await db.flush()
    return keys


async def test_tc_02_68_open_claim_evidence_uploaded_first(
    db: AsyncSession, world: World, memory_store: MemoryStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TC-02.68 (02a J-21 / J-22, DEC-656): 21 đối tượng chờ cùng lý do — 18 của 6 phiên không thuộc hồ sơ
    (xếp **trước**, cũ hơn 1 giờ) + 3 của phiên là bằng chứng hồ sơ khiếu nại chưa đóng (xếp sau) → J-22 tải 3
    đối tượng của hồ sơ mở **trước tiên**, rồi tới phần còn lại theo thứ tự cũ trước; cả 21 `UPLOADED`."""
    loose = [world.loose, *await _loose_packs(db, world, 5)]
    loose_keys = await _queue(db, loose, bk.NOW - timedelta(hours=2))
    claim_keys = await _queue(db, [world.protected], bk.NOW - timedelta(hours=1))
    assert (len(loose_keys), len(claim_keys)) == (18, 3)
    order: list[str] = []
    real_put = memory_store.put_stream

    def spy(key: str, *args: Any, **kw: Any) -> int:
        order.append(key)
        return real_put(key, *args, **kw)

    monkeypatch.setattr(memory_store, "put_stream", spy)

    out = await bk_jobs.upload_evidence(db, world.settings, store=memory_store)

    assert out.get("UPLOADED") == 21, out
    assert set(order[:3]) == set(claim_keys)
    assert order[3:] == [k for k in loose_keys if k in order[3:]]  # còn lại: cũ trước, giữ thứ tự xếp
    rows = (await db.scalars(select(BackupObject))).all()
    assert {r.status for r in rows} == {"UPLOADED"}


@pytest.mark.parametrize(
    ("code", "message", "expected_code", "expected_message"),
    [
        ("AccessDenied", "Access Denied.", "CLOUD_AUTH_FAILED", "Kho lưu từ chối: sai khóa truy cập."),
        (
            "XMinioStorageFull",
            "Storage backend has reached its minimum free drive threshold.",
            "CLOUD_ERROR",
            "Kho lưu báo lỗi: Storage backend has reached its minimum free drive threshold.",
        ),
    ],
)
async def test_tc_02_96_store_rejects_put_failed_backoff_and_api180_error(
    backup_api: AsyncClient,
    db: AsyncSession,
    world: World,
    memory_store: MemoryStore,
    code: str,
    message: str,
    expected_code: str,
    expected_message: str,
) -> None:
    """TC-02.96 (EX-K4): kho từ chối PUT (S3 `AccessDenied` / MinIO đầy) — lỗi boto3 qua `classify` như S3Store
    → J-22: mọi đối tượng `FAILED`, `attempts = 1`, thử lại sau 5 phút (rồi 15); API-180 `last_error {code,
    message, at}` (D23); kho hết lỗi → lượt sau `UPLOADED`."""
    from botocore.exceptions import ClientError

    use_settings(backup_api, world.settings)
    headers, _ = await bk.login(backup_api, db)
    await bk_jobs.enqueue_evidence(db, world.settings)
    memory_store.fail = classify(
        ClientError({"Error": {"Code": code, "Message": message}}, "PutObject")  # type: ignore[arg-type]
    )

    out = await bk_jobs.upload_evidence(db, world.settings, store=memory_store)

    rows = (await db.scalars(select(BackupObject))).all()
    assert out["FAILED"] == len(rows) == 3, out
    for row in rows:
        await db.refresh(row)
        assert (row.status, row.attempts) == ("FAILED", 1)
        assert row.next_attempt_at == bk.NOW + timedelta(minutes=5)
        assert row.last_error == f"{expected_code}: {expected_message}"
    body = (await backup_api.get("/api/v1/backup", headers=headers)).json()
    assert body["last_error"] == {
        "code": expected_code,
        "message": expected_message,
        "at": "2026-10-07T03:00:00Z",
    }

    clock.advance(timedelta(minutes=6))
    out = await bk_jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["FAILED"] == 3
    for row in rows:
        await db.refresh(row)
        assert row.attempts == 2
        assert row.next_attempt_at == clock.now() + timedelta(minutes=15)

    memory_store.fail = None
    clock.advance(timedelta(minutes=16))
    out = await bk_jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["UPLOADED"] == 3


# ---------------------------------------------------------------- TC-MS.10 (API-32)


async def test_tc_ms_10_daily_clip_failed_ignores_missing(
    api: AsyncClient, db: AsyncSession, redis_client: Any
) -> None:
    """TC-MS.10 (02a §5.2 #13): clip `MISSING` (thiếu tệp) **không** đếm vào mục D2 `CLIP_FAILED` (chỉ clip cắt
    lỗi `FAILED` trong 7 ngày). 1 `MISSING` + 0 `FAILED` → không có mục; thêm 1 `FAILED` → `count = 1`."""
    clock.freeze(datetime(2026, 10, 7, 3, 0, tzinfo=UTC))
    headers, _ = await bk.login(api, db, "ADMIN")
    _, station = await make_station_account(db, "tst_ms10", "TST MS10")
    package = Package(tracking_number="SPXTSTMS1001", warehouse_status="PACKED")
    db.add(package)
    await db.flush()
    pack = await pack_session_with_clips(db, station, package, clock.now() - timedelta(hours=1))
    clips = {c.camera_role: c for c in (await db.scalars(select(Clip).where(Clip.session_id == pack.id)))}
    clips["CAM1"].status = "MISSING"
    await db.flush()

    async def attention() -> dict[str, Any]:
        await redis_client.flushdb()  # cache API-32 5 giây theo ngày
        res = await api.get("/api/v1/reports/daily", headers=headers)
        assert res.status_code == 200, res.text
        return {a["kind"]: a for a in res.json()["attention"]}

    assert "CLIP_FAILED" not in await attention()
    clips["CAM2"].status = "FAILED"
    await db.flush()
    assert (await attention())["CLIP_FAILED"] == {"kind": "CLIP_FAILED", "count": 1}


# ---------------------------------------------------------------- link chia sẻ: TC-07.55, 07.67, 07.68, P3.19


@pytest.fixture
async def sw(db: AsyncSession, share_settings: Settings) -> ShareWorld:
    return await make_share_world(db, share_settings)


async def test_tc_07_55_snapshot_count_excludes_missing_and_link_caps_20(
    share_api: AsyncClient, db: AsyncSession, sw: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    """TC-07.55 (BR-35, 02a §5.2 #10): phiên chính `ret_a` có 22 ảnh `READY` + 1 ảnh `MISSING` trong bằng chứng →
    API-164 `sessions[ret_a].snapshot_count = 22` (không đếm `MISSING`), `limits.max_snapshots = 20`; tạo link
    chỉ phiên đó kèm ảnh → J-24 tải đúng 20 ảnh (không có ảnh `MISSING`)."""
    extra = [await add_snapshot(db, share_settings, sw.ret_a, n) for n in range(3, 24)]  # 21 ảnh
    extra[-1].status = "MISSING"
    for snap in extra:
        db.add(ClaimEvidence(claim_id=sw.claim.id, kind="SNAPSHOT", snapshot_id=snap.id, auto=False))
    await db.flush()
    headers, _ = await shares_fx.login(share_api, db, "CSKH")

    opts = (
        await share_api.get("/api/v1/shares/options", headers=headers, params={"claim_id": str(sw.claim.id)})
    ).json()

    row = next(s for s in opts["sessions"] if s["id"] == str(sw.ret_a.id))
    assert row["snapshot_count"] == 22
    assert opts["limits"]["max_snapshots"] == 20
    created = await shares_jobs._create(share_api, headers, sw, [sw.ret_a.id])
    assert await build.build(db, created.id, share_settings, render=shares_jobs.fake_render) == "ACTIVE"
    link = await shares_jobs._link(db, created.id)
    photos = store.keys(f"{link.object_prefix}p")
    assert len(photos) == 20, photos
    item = await db.scalar(select(ShareItem).where(ShareItem.share_id == link.id))
    assert item is not None
    assert len(item.snapshot_ids) == 20
    assert extra[-1].id not in item.snapshot_ids


async def test_tc_07_67_close_claim_link_stays_active_and_revocable(
    share_api: AsyncClient, db: AsyncSession, sw: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    """TC-07.67 (EX-S5): link `ACTIVE` của hồ sơ → đóng hồ sơ (API-133 `CLOSED`) → link vẫn `ACTIVE`, còn trong
    API-132 `shares[]`; API-163 thu hồi được (`REVOKED`)."""
    headers, _ = await shares_fx.login(share_api, db, "CSKH")
    created = await shares_jobs._create(share_api, headers, sw, [sw.ret_a.id])
    assert await build.build(db, created.id, share_settings, render=shares_jobs.fake_render) == "ACTIVE"
    detail = (await share_api.get(f"/api/v1/claims/{sw.claim.id}", headers=headers)).json()

    closed = await share_api.patch(
        f"/api/v1/claims/{sw.claim.id}",
        headers=headers,
        json={"version": detail["version"], "status": "CLOSED", "reason": "Khách rút khiếu nại"},
    )

    assert closed.status_code == 200, closed.text
    body = closed.json()
    assert body["status"] == "CLOSED"
    assert [(s["id"], s["status"]) for s in body["shares"]] == [(str(created.id), "ACTIVE")]
    assert (await shares_jobs._link(db, created.id)).status == "ACTIVE"
    revoked = await share_api.post(f"/api/v1/shares/{created.id}/revoke", headers=headers)
    assert revoked.status_code == 200, revoked.text
    assert revoked.json()["status"] == "REVOKED"
    again = (await share_api.get(f"/api/v1/claims/{sw.claim.id}", headers=headers)).json()
    assert [(s["id"], s["status"]) for s in again["shares"]] == [(str(created.id), "REVOKED")]


async def test_tc_07_68_retention_deletes_source_clip_link_video_kept(
    share_api: AsyncClient, db: AsyncSession, sw: ShareWorld, store: SignedStore, share_settings: Settings
) -> None:
    """TC-07.68 (EX-S6): link `ACTIVE` dựng từ phiên `ret_a`; hồ sơ đóng lâu, hồ sơ hàng hoàn đã xong, clip gốc
    quá hạn giữ → J-02 xóa clip gốc (`DELETED`, tệp xóa) → link vẫn `ACTIVE`, video của link (bản dựng riêng)
    còn nguyên trên kho, URL ký trong W1 vẫn trỏ tới nó."""
    headers, _ = await shares_fx.login(share_api, db, "CSKH")
    created = await shares_jobs._create(share_api, headers, sw, [sw.ret_a.id], expires_days=7)
    assert await build.build(db, created.id, share_settings, render=shares_jobs.fake_render) == "ACTIVE"
    link = await shares_jobs._link(db, created.id)
    video_key = f"{link.object_prefix}v1.mp4"
    video = store.get_stream(video_key).read()
    assert video.startswith(b"MP4-")

    old = shares_fx.NOW - timedelta(days=200)
    await db.execute(update(Claim).where(Claim.id == sw.claim.id).values(status="CLOSED", closed_at=old))
    await db.execute(
        update(ReturnCase)
        .where(ReturnCase.id == sw.claim.return_case_id)
        .values(status="RECEIVED_OK", received_at=old)
    )
    await db.execute(
        update(Clip)
        .where(Clip.session_id == sw.ret_a.id)
        .values(start_at=old, end_at=old + timedelta(minutes=1))
    )
    await db.execute(
        update(PackSession)
        .where(PackSession.id == sw.ret_a.id)
        .values(started_at=old, ended_at=old + timedelta(minutes=2))
    )
    await db.flush()
    sources = (await db.scalars(select(Clip).where(Clip.session_id == sw.ret_a.id))).all()
    paths = [share_settings.video_root / c.path for c in sources if c.path]
    assert paths
    assert all(p.exists() for p in paths)

    await media.enforce_retention(db, share_settings)

    for clip in sources:
        await db.refresh(clip)
        assert clip.status == "DELETED", clip.camera_role
    assert not any(p.exists() for p in paths)
    link = await shares_jobs._link(db, created.id)
    assert link.status == "ACTIVE"
    assert store.get_stream(video_key).read() == video
    page = store.get_stream(f"{link.object_prefix}index.html").read().decode()
    assert f"/{store.bucket}/{video_key}?" in page


async def test_tc_p3_19_share_updated_on_dashboard_channel_not_admin(
    share_api: AsyncClient, db: AsyncSession, sw: ShareWorld, store: SignedStore, redis_client: Any
) -> None:
    """TC-P3.19 (WS-02, ma trận quyền): kết nối WS dashboard của Supervisor / CSKH nghe `ws:dashboard` (không
    `ws:admin`); API-160 (CSKH tạo link) phát `share.updated {share_id, status, progress, step}` trên
    `ws:dashboard` → Supervisor / CSKH / Admin đều nhận; **không** phát trên kênh chỉ-Admin `ws:admin`;
    STATION không vào được endpoint dashboard."""
    exp = datetime.now(UTC) + timedelta(minutes=5)

    def claims(role: str) -> AccessClaims:
        return AccessClaims(user_id=uuid.uuid4(), role=role, station_id=None, expires_at=exp)

    for role in ("SUPERVISOR", "CSKH"):
        channels = channels_for("dashboard", claims(role))
        assert channels is not None
        assert "ws:dashboard" in channels
        assert "ws:admin" not in channels
    admin_channels = channels_for("dashboard", claims("ADMIN"))
    assert admin_channels is not None
    assert {"ws:dashboard", "ws:admin"} <= set(admin_channels)
    station = AccessClaims(user_id=uuid.uuid4(), role="STATION", station_id=uuid.uuid4(), expires_at=exp)
    assert channels_for("dashboard", station) is None

    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ws:dashboard", "ws:admin")
    for _ in range(2):
        await pubsub.get_message(timeout=1)  # xác nhận subscribe
    headers, _ = await shares_fx.login(share_api, db, "CSKH")
    created = await shares_jobs._create(share_api, headers, sw, [sw.ret_a.id])
    got: list[tuple[str, dict[str, Any]]] = []
    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if msg:
            got.append((msg["channel"].decode() if isinstance(msg["channel"], bytes) else msg["channel"],
                        json.loads(msg["data"])))  # fmt: skip
        elif got:
            break
        await asyncio.sleep(0)
    await pubsub.aclose()

    shares_msgs = [(ch, m) for ch, m in got if m["type"] == "share.updated"]
    assert shares_msgs, got
    assert {ch for ch, _ in shares_msgs} == {"ws:dashboard"}
    data = shares_msgs[0][1]["data"]
    assert data["share_id"] == str(created.id)
    assert set(data) == {"share_id", "status", "progress", "step"}


# ---------------------------------------------------------------- TC-08.58 (API-189 ∥ API-134, 2 connection)


async def _evidence_rows(claim_id: uuid.UUID) -> list[ClaimEvidence]:
    async with sessionmaker()() as db:
        return list(
            (
                await db.scalars(
                    select(ClaimEvidence).where(ClaimEvidence.claim_id == claim_id).order_by(ClaimEvidence.id)
                )
            ).all()
        )


async def _api134_drop_snapshot(
    claim_id: uuid.UUID, version: int, p: Principal, settings: Settings, drop: uuid.UUID
) -> str:
    """API-134 (hàm route thật, session riêng): giữ mọi bằng chứng đang dùng trừ ảnh `drop` (có lý do)."""
    active = [e for e in await _evidence_rows(claim_id) if e.removed_at is None]
    body = EvidenceIn(
        version=version,
        session_ids=[e.session_id for e in active if e.kind == "SESSION" and e.session_id],
        snapshot_ids=[
            e.snapshot_id for e in active if e.kind == "SNAPSHOT" and e.snapshot_id and e.snapshot_id != drop
        ],
        note="Ảnh chụp nhầm kiện",
    )
    async with sessionmaker()() as db:
        try:
            await claims_router.set_evidence(claim_id, body, p, db, settings)
            return "200"
        except AppError as exc:
            await db.rollback()
            return exc.code


async def _api189_mark(
    claim_id: uuid.UUID, session_id: uuid.UUID, version: int, p: Principal, settings: Settings
) -> str:
    body = ReviewIn(
        version=version, action="MARK_WRONG_SCAN", reason_code="WRONG_SCAN", note="Quét nhầm kiện"
    )
    async with sessionmaker()() as db:
        try:
            await claims_router.review_return_session(claim_id, session_id, body, p, db, settings)
            return "200"
        except AppError as exc:
            await db.rollback()
            return exc.code


@pytest.mark.parametrize("first", ["API-189", "API-134"])
async def test_tc_08_58_mark_and_set_evidence_same_version(
    committed: Any, share_settings: Settings, monkeypatch: pytest.MonkeyPatch, first: str
) -> None:
    """TC-08.58 (02a §6): API-189 `MARK_WRONG_SCAN` phiên `ret_a` ∥ API-134 bỏ một ảnh, **cùng `version`**, hai
    connection (dữ liệu commit thật). Bên khóa hồ sơ trước được giữ transaction 0,5 giây; bên kia chờ khóa
    rồi gặp `version` đã tăng → một 200, một 409 `VERSION_CONFLICT`. Không mất dòng: số dòng `claim_evidence`
    không đổi, chỉ thay đổi của bên thắng được ghi, `version` tăng đúng 1."""
    claim_id, sid, version, p = await race._world(share_settings, 73 if first == "API-189" else 74)
    before = await _evidence_rows(claim_id)
    async with sessionmaker()() as db:  # ảnh của phiên khác `ret_a` (MARK bỏ kèm ảnh của chính phiên đó)
        snap_session = dict((await db.execute(select(Snapshot.id, Snapshot.session_id))).all())
    drop = next(
        e.snapshot_id
        for e in before
        if e.kind == "SNAPSHOT"
        and e.removed_at is None
        and e.snapshot_id
        and snap_session[e.snapshot_id] != sid
    )
    locked, release = asyncio.Event(), asyncio.Event()
    if first == "API-189":
        real_aff = review.share_queries.affected_shares

        async def hold_mark(db: Any, *args: Any, **kw: Any) -> Any:
            out = await real_aff(db, *args, **kw)
            locked.set()
            await release.wait()
            return out

        monkeypatch.setattr(review.share_queries, "affected_shares", hold_mark)
        first_task = asyncio.create_task(_api189_mark(claim_id, sid, version, p, share_settings))
    else:
        real_set = claims_service.set_evidence

        async def hold_set(*args: Any, **kw: Any) -> Any:
            out = await real_set(*args, **kw)
            locked.set()
            await release.wait()
            return out

        monkeypatch.setattr(claims_service, "set_evidence", hold_set)
        first_task = asyncio.create_task(_api134_drop_snapshot(claim_id, version, p, share_settings, drop))
    await asyncio.wait_for(asyncio.shield(locked.wait()), timeout=10)
    if first == "API-189":
        second_task = asyncio.create_task(_api134_drop_snapshot(claim_id, version, p, share_settings, drop))
    else:
        second_task = asyncio.create_task(_api189_mark(claim_id, sid, version, p, share_settings))
    try:
        await asyncio.sleep(race.HOLD)
        assert not second_task.done()  # đang chờ khóa hồ sơ
    finally:
        release.set()
    results = await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=20)

    assert results == ["200", "VERSION_CONFLICT"]
    after = await _evidence_rows(claim_id)
    assert len(after) == len(before)
    by_snap = {e.snapshot_id: e for e in after if e.kind == "SNAPSHOT"}
    by_sess = {e.session_id: e for e in after if e.kind == "SESSION"}
    async with sessionmaker()() as db:
        claim = await db.get(Claim, claim_id)
        pack = await db.get(PackSession, sid)
    assert claim is not None
    assert pack is not None
    assert claim.version == version + 1
    if first == "API-189":
        assert pack.wrong_scan_at is not None
        assert by_sess[sid].removed_at is not None
        assert by_snap[drop].removed_at is None  # API-134 thua: ảnh còn
    else:
        assert pack.wrong_scan_at is None  # API-189 thua: phiên không bị đánh dấu
        assert by_snap[drop].removed_at is not None
        assert by_sess[sid].removed_at is None


# ---------------------------------------------------------------- TC-MG3.16 (DEC-498)


def test_tc_mg3_16_0006_down_up_twice_is_stable(mig_db: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """TC-MG3.16 (DEC-498): head có dữ liệu Phase 3 → `downgrade 0005` → `upgrade` → `downgrade 0005` →
    `upgrade` lần hai: không nhân đôi `claim_evidence` (backfill `ON CONFLICT DO NOTHING`), số dòng và nội dung
    mọi bảng so sánh y hệt sau vòng 1 và vòng 2 (và như trước khi lùi); schema khớp model."""
    cfg = alembic_config()
    mig.seed_phase2_platform_data()
    command.upgrade(cfg, SCHEMA_HEAD)
    mig.seed_phase3()
    monkeypatch.setenv("AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS", "1")
    original = mig.dump()
    rounds = []
    for _ in range(2):
        command.downgrade(cfg, "0005")
        assert mig.run("SELECT version_num FROM alembic_version") == [("0005",)]
        command.upgrade(cfg, SCHEMA_HEAD)
        assert mig.run("SELECT to_regnamespace('phase3_archive')") == [(None,)]
        rounds.append(mig.dump())
    first, second = rounds
    assert len(second["claim_evidence"]) == len(first["claim_evidence"]) == len(original["claim_evidence"])
    for table in mig.COMPARED:
        assert second[table] == first[table], table
        assert first[table] == original[table], table
    command.check(cfg)
