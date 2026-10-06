"""BR-38 (L15, FR-08.09, AC-58; T-214): bỏ bằng chứng = bỏ mềm (`removed_*`), lý do 5–500 cho **mọi** bằng
chứng, clip / ảnh bị bỏ giữ tới max(lúc kết thúc, lúc bỏ) + số ngày giữ (J-02 không xóa đêm đó, xóa đúng sau
hạn); API-132 `removed_evidence[]`, `removal_keep_until`; thêm lại = khôi phục dòng; audit
`CLAIM_EVIDENCE_REMOVE`."""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.claims.models import ClaimEvidence
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings.models import Setting
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import make_order, pack_session_with_clips

pytestmark = pytest.mark.integration

CLIP_END = datetime(2026, 5, 1, 3, 0, tzinfo=UTC)  # clip 01/05
REMOVED_AT = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)  # bỏ 06/10
KEEP_UNTIL = "2027-01-04T03:00:00Z"  # 06/10 + 90 ngày (BR-38 ví dụ)
REASON = "Nhầm kiện của đơn khác"


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


async def _packed(db: AsyncSession, settings: Settings, station: Station, package: Package) -> PackSession:
    pack = await pack_session_with_clips(db, station, package, CLIP_END - timedelta(seconds=5))
    rels = [c.path for c in (await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all()]
    rels += [s.path for s in (await db.scalars(select(Snapshot).where(Snapshot.session_id == pack.id))).all()]
    for rel in rels:
        assert rel is not None
        path = settings.video_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\x00" * 16)
    return pack


async def _states(db: AsyncSession, session_id: uuid.UUID) -> list[str]:
    rows = [
        *(
            await db.scalars(select(Clip).where(Clip.session_id == session_id).order_by(Clip.camera_role))
        ).all(),
        *(await db.scalars(select(Snapshot).where(Snapshot.session_id == session_id))).all(),
    ]
    for row in rows:
        await db.refresh(row)
    return [r.status for r in rows]


async def _login(api: AsyncClient) -> dict[str, str]:
    """Đăng nhập lại sau mỗi lần đổi đồng hồ giả (token theo giờ server)."""
    res = await api.post(
        "/api/v1/auth/login", json={"username": "tst_cskh_br38", "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _claim_with_pack(
    api: AsyncClient, db: AsyncSession, settings: Settings
) -> tuple[Any, PackSession, Any]:
    await db.execute(
        update(Setting).where(Setting.id == 1).values(retention_raw_days=7, retention_clip_days=90)
    )
    clock.freeze(CLIP_END)
    _, station = await make_station_account(db)
    _, (package,) = await make_order(db, 11)
    pack = await _packed(db, settings, station, package)
    await make_user(db, "tst_cskh_br38", "CSKH", display_name="Hoa")
    clock.freeze(REMOVED_AT - timedelta(days=1))
    headers = await _login(api)
    claim = await api.post(
        "/api/v1/claims",
        headers=headers,
        json={"package_id": str(package.id), "type": "BUYER_CLAIM", "counterparty": "PLATFORM"},
    )
    assert claim.status_code == 201, claim.text
    return claim.json(), pack, headers


async def test_remove_is_soft_and_keeps_clip_until_deadline(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """AC-58 / BR-38 ví dụ: clip 01/05, giữ 90 ngày, bỏ 06/10 → giữ tới 04/01/2027; J-02 không xóa đêm đó,
    xóa sau hạn."""
    claim, pack, headers = await _claim_with_pack(api, db, media_settings)
    url = f"/api/v1/claims/{claim['id']}/evidence"
    detail = (await api.get(f"/api/v1/claims/{claim['id']}", headers=headers)).json()
    clock.freeze(REMOVED_AT)
    headers = await _login(api)
    preview = (await api.get(f"/api/v1/claims/{claim['id']}", headers=headers)).json()
    assert {e["removal_keep_until"] for e in preview["evidence"]} == {KEEP_UNTIL}

    no_note = await api.put(
        url,
        headers=headers,
        json={"version": detail["version"], "session_ids": [], "snapshot_ids": [], "note": "abc"},
    )
    assert no_note.status_code == 422
    res = await api.put(
        url,
        headers=headers,
        json={"version": detail["version"], "session_ids": [], "snapshot_ids": [], "note": REASON},
    )

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["evidence"] == []
    removed = {e["kind"]: e["removed"] for e in body["removed_evidence"]}
    assert set(removed) == {"SESSION", "SNAPSHOT"}
    for info in removed.values():
        assert (info["at"], info["reason"], info["by"]["display_name"]) == (
            "2026-10-06T03:00:00Z",
            REASON,
            "Hoa",
        )
    assert removed["SESSION"]["keep_until"] == KEEP_UNTIL
    rows = (
        await db.scalars(select(ClaimEvidence).where(ClaimEvidence.claim_id == uuid.UUID(claim["id"])))
    ).all()
    assert len(rows) == 2
    assert all(r.removed_at == REMOVED_AT and r.removed_reason == REASON for r in rows)
    audits = (
        await db.scalars(
            select(AuditLog).where(AuditLog.action == "CLAIM_EVIDENCE_REMOVE").order_by(AuditLog.id)
        )
    ).all()
    assert len(audits) == 2
    assert {a.data["reason"] for a in audits if a.data} == {REASON}
    assert {a.data["session_id"] for a in audits if a.data} == {str(pack.id), None}

    # Đêm đó (đã quá 90 ngày kể từ clip) J-02 không xóa: được bảo vệ theo lúc bỏ.
    clock.freeze(REMOVED_AT + timedelta(hours=15))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    clock.freeze(datetime(2027, 1, 3, 19, 0, tzinfo=UTC))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    clock.freeze(datetime(2027, 1, 4, 19, 0, tzinfo=UTC))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["DELETED", "DELETED", "DELETED"]


async def test_readd_removed_evidence_restores_row(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """Thêm lại phiên / ảnh đã bỏ = xóa `removed_*` của dòng cũ (không thêm dòng mới, giữ `auto`)."""
    claim, pack, headers = await _claim_with_pack(api, db, media_settings)
    url = f"/api/v1/claims/{claim['id']}/evidence"
    clock.freeze(REMOVED_AT)
    headers = await _login(api)
    first = await api.put(
        url, headers=headers, json={"version": 1, "session_ids": [], "snapshot_ids": [], "note": REASON}
    )
    assert first.status_code == 200, first.text
    snap_id = next(e["snapshot"]["id"] for e in first.json()["removed_evidence"] if e["snapshot"])

    res = await api.put(
        url, headers=headers, json={"version": 2, "session_ids": [str(pack.id)], "snapshot_ids": [snap_id]}
    )

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["removed_evidence"] == []
    assert [(e["kind"], e["auto"]) for e in body["evidence"]] == [("SESSION", True), ("SNAPSHOT", True)]
    assert body["notes"][-1]["text"] == "Cập nhật bằng chứng: thêm lại 2."
    rows = (
        await db.scalars(select(ClaimEvidence).where(ClaimEvidence.claim_id == uuid.UUID(claim["id"])))
    ).all()
    assert len(rows) == 2
    assert all(r.removed_at is None and r.removed_reason is None and r.removed_by is None for r in rows)


async def test_removing_manual_evidence_also_needs_note(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """02 §6 API-134: bỏ **mọi** bằng chứng (kể cả thêm tay) cần lý do; chỉ thêm thì không cần."""
    claim, pack, headers = await _claim_with_pack(api, db, media_settings)
    url = f"/api/v1/claims/{claim['id']}/evidence"
    _, station = await make_station_account(db, "tst_station_br38b", "TST Station BR38B")
    _, (other,) = await make_order(db, 12)
    assert other is not None
    detail = (await api.get(f"/api/v1/claims/{claim['id']}", headers=headers)).json()
    snap_ids = [e["snapshot"]["id"] for e in detail["evidence"] if e["snapshot"]]
    extra = await pack_session_with_clips(db, station, await db.get(Package, pack.package_id), REMOVED_AT)  # type: ignore[arg-type]
    extra.status = "SUPERSEDED"
    await db.flush()
    added = await api.put(
        url,
        headers=headers,
        json={"version": 1, "session_ids": [str(pack.id), str(extra.id)], "snapshot_ids": snap_ids},
    )
    assert added.status_code == 200, added.text

    res = await api.put(
        url, headers=headers, json={"version": 2, "session_ids": [str(pack.id)], "snapshot_ids": snap_ids}
    )

    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {"note": "Nhập lý do bỏ bằng chứng (5–500 ký tự)."}
