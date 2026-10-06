"""Gói bằng chứng (T-112): API-136..138, J-16, dọn J-10, `info.json` L5, `ket-luan.json` (`corrections[]`).

TC-08.16, TC-08.18..21, TC-02.40, TC-02.41, AC-25. Encode video có chữ thay bằng renderer giả (máy dev thiếu
`drawtext`; encode thật kiểm ở QA live trong container `worker-export`).
"""

import hashlib
import io
import json
import uuid
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.claims import pack as packs
from aicam.modules.claims.models import Claim, EvidencePack
from aicam.modules.media import ffmpeg
from aicam.modules.media.exports import Rendered, session_info_fields
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order, pack_session_with_clips, return_session

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


async def fake_render(
    db: AsyncSession, pack: PackSession, sources: list[Clip], video: Path, settings: Settings, progress: Any
) -> Rendered:
    video.write_bytes(b"MP4-" + b"".join(c.camera_role.encode() for c in sources))  # noqa: ASYNC240
    if progress is not None:
        await progress(50)
        await progress(99)
    return Rendered(ffmpeg.sha256_file(video), pack.started_at, pack.ended_at or pack.started_at, [])


def _write(settings: Settings, rel: str, content: bytes) -> str:
    path = settings.video_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


async def _files_for(db: AsyncSession, settings: Settings, session_id: uuid.UUID) -> None:
    """Ghi file thật cho clip / ảnh của phiên, SHA-256 trong DB = file."""
    for clip in (await db.scalars(select(Clip).where(Clip.session_id == session_id))).all():
        if clip.status == "READY":
            clip.path = clip.path or f"clips/{session_id}-{clip.camera_role}.mp4"
            clip.sha256 = _write(settings, clip.path, f"clip-{clip.id}".encode())
    for snap in (await db.scalars(select(Snapshot).where(Snapshot.session_id == session_id))).all():
        snap.sha256 = _write(settings, snap.path or "", f"jpg-{snap.id}".encode())
    await db.flush()


async def _login(api: AsyncClient, db: AsyncSession, name: str, role: str = "CSKH") -> dict[str, str]:
    user = await make_user(db, name, role, display_name=f"{role} {name}")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _issue_claim(
    db: AsyncSession, settings: Settings, station: Station
) -> tuple[Claim, PackSession, PackSession]:
    """Hồ sơ "Hộp rỗng" tự tạo: phiên PACK (clip + ảnh lúc đóng gói) + phiên RETURN (clip + 3 ảnh, đã sửa kết
    luận 1 lần)."""
    from aicam.modules.claims import service as claims

    order, (package,) = await make_order(db, 41)
    pack = await pack_session_with_clips(db, station, package, T0 - timedelta(days=5))
    case = await buyer_return_case(db, order, 41)
    ret = return_session(station, package, case, conclusion="EMPTY_BOX")
    ret.operator_name = "Lan QA"
    ret.inspection_corrections = [
        {
            "by": {"id": str(uuid.uuid4()), "display_name": "Minh QL"},
            "at": clock.iso_z(T0),
            "reason": "Chọn nhầm",
            "before": {"conclusion": "OK", "note": "", "lines": []},
        }
    ]
    ret.camera_clock = [{"camera_role": "CAM1", "clock_offset_ms": 120, "checked_at": None}]
    ret.flags = ["INSPECTION_CORRECTED"]
    db.add(ret)
    await db.flush()
    for role in ("CAM1", "CAM2"):
        db.add(
            Clip(
                session_id=ret.id,
                camera_role=role,
                status="READY",
                start_at=ret.started_at,
                end_at=ret.started_at + timedelta(minutes=2),
                path=f"clips/{ret.id}-{role}.mp4",
                flags=[],
            )
        )
    for n in range(1, 4):
        db.add(
            Snapshot(
                session_id=ret.id,
                kind="MANUAL",
                camera_role="CAM1",
                taken_at=ret.started_at + timedelta(seconds=n),
                path=f"snapshots/{ret.id}_{n:02d}.jpg",
                status="READY",
            )
        )
    await db.flush()
    await _files_for(db, settings, pack.id)
    await _files_for(db, settings, ret.id)
    created = await claims.create_from_return(db, ret, case)
    assert created is not None
    return created.claim, pack, ret


async def test_pack_zip_contents(
    api: AsyncClient, db: AsyncSession, media_settings: Settings, sent_jobs: list[Any]
) -> None:
    """TC-08.16, TC-08.20, AC-25: API-136 202 → J-16 → API-137 READY → API-138 zip đúng cấu trúc; clip gốc
    SHA-256 = DB; `info.json` L5; `ket-luan.json.corrections[0]`; `ho-so.json` liệt kê tệp + SHA-256;
    audit."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    claim, pack, _ = await _issue_claim(db, media_settings, station)
    headers = await _login(api, db, "tst_cskh_pk")

    created = await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=headers)
    assert created.status_code == 202, created.text
    body = created.json()
    assert (body["status"], body["progress"]) == ("QUEUED", 0)
    assert [(t, a, q) for t, a, q, _ in sent_jobs if t == "claims.build_evidence_pack"] == [
        ("claims.build_evidence_pack", [body["id"]], "export")
    ]
    assert (
        await packs.build_evidence_pack(db, uuid.UUID(body["id"]), media_settings, render=fake_render)
        == "READY"
    )

    status = (await api.get(f"/api/v1/evidence-packs/{body['id']}", headers=headers)).json()
    assert (status["status"], status["progress"], status["missing"]) == ("READY", 100, [])
    download = await api.get(status["files"]["zip"])
    assert download.status_code == 200
    assert download.headers["content-type"] == "application/zip"
    assert f'filename="{claim.code}.zip"' in download.headers["content-disposition"]
    assert hashlib.sha256(download.content).hexdigest() == status["sha256"]
    zf = zipfile.ZipFile(io.BytesIO(download.content))
    names = set(zf.namelist())
    pack_dir = next(n.rsplit("/", 1)[0] for n in names if "/01-dong-goi-" in n)
    ret_dir = next(n.rsplit("/", 1)[0] for n in names if "/02-mo-hoan-" in n)
    assert pack_dir == f"{claim.code}/01-dong-goi-20261001-1000"  # 5 ngày trước T0, giờ VN
    assert {f"{claim.code}/ho-so.json", f"{claim.code}/README.txt"} <= names
    for name in (
        "video-ghep-co-chu.mp4",
        "goc-CAM1.mp4",
        "goc-CAM2.mp4",
        "anh-luc-dong-goi.jpg",
        "info.json",
    ):
        assert f"{pack_dir}/{name}" in names
    for name in (
        "video-ghep-co-chu.mp4",
        "goc-CAM1.mp4",
        "anh-01.jpg",
        "anh-02.jpg",
        "anh-03.jpg",
        "ket-luan.json",
    ):
        assert f"{ret_dir}/{name}" in names
    for clip in (await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all():
        assert hashlib.sha256(zf.read(f"{pack_dir}/goc-{clip.camera_role}.mp4")).hexdigest() == clip.sha256
    info = json.loads(zf.read(f"{ret_dir}/info.json"))
    assert (info["session_type"], info["session_status"], info["operator_name"]) == (
        "RETURN",
        "COMPLETED",
        "Lan QA",
    )
    assert info["flags"] == ["INSPECTION_CORRECTED"]
    assert info["cameras"] == [
        {"camera_role": "CAM1", "clock_offset_ms": 120, "clock_checked_at": None},
        {"camera_role": "CAM2", "clock_offset_ms": None, "clock_checked_at": None},
    ]
    conclusion = json.loads(zf.read(f"{ret_dir}/ket-luan.json"))
    assert (conclusion["conclusion"], conclusion["conclusion_label"]) == ("EMPTY_BOX", "Hộp rỗng")
    assert conclusion["corrections"][0]["reason"] == "Chọn nhầm"
    assert conclusion["corrections"][0]["before"]["conclusion"] == "OK"
    summary = json.loads(zf.read(f"{claim.code}/ho-so.json"))
    assert (summary["code"], summary["type"], summary["missing"]) == (claim.code, "EMPTY_BOX", [])
    listed = {f["path"]: f["sha256"] for f in summary["files"]}
    for path, sha in listed.items():
        assert hashlib.sha256(zf.read(f"{claim.code}/{path}")).hexdigest() == sha
    assert summary["return_case"]["return_tracking_number"] == "SPXRTTST000041"
    actions = (
        await db.scalars(
            select(AuditLog.action).where(AuditLog.object_id == str(claim.id)).order_by(AuditLog.id)
        )
    ).all()
    assert {"EXPORT_CLAIM_PACK", "DOWNLOAD_CLAIM_PACK"} <= set(actions)
    bad = await api.get(status["files"]["zip"].replace("sig=", "sig=00"))
    assert (bad.status_code, bad.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")


async def test_pack_with_deleted_clip_and_closed_claim(
    db: AsyncSession, media_settings: Settings, api: AsyncClient
) -> None:
    """TC-08.18: hồ sơ đóng, clip PACK đã `DELETED` → gói vẫn READY, `missing` có `CLIP_DELETED`, `ho-so.json`
    ghi thiếu; không encode phiên không có clip."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    claim, pack, _ = await _issue_claim(db, media_settings, station)
    for clip in (await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all():
        clip.status, clip.deleted_at = "DELETED", T0
    claim.status, claim.closed_at, claim.close_reason = "CLOSED", T0, "Không gửi"
    await db.flush()
    headers = await _login(api, db, "tst_cskh_pk2")
    pack_id = (await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=headers)).json()["id"]

    assert (
        await packs.build_evidence_pack(db, uuid.UUID(pack_id), media_settings, render=fake_render) == "READY"
    )

    row = await db.get(EvidencePack, uuid.UUID(pack_id), populate_existing=True)
    assert row is not None
    assert sorted((m["camera_role"], m["reason"]) for m in row.missing) == [
        ("CAM1", "CLIP_DELETED"),
        ("CAM2", "CLIP_DELETED"),
    ]
    assert row.path is not None
    zf = zipfile.ZipFile(media_settings.video_root / row.path)
    assert not any("01-dong-goi" in n and "video-ghep" in n for n in zf.namelist())
    summary = json.loads(zf.read(f"{claim.code}/ho-so.json"))
    assert len(summary["missing"]) == 2


async def test_pack_conflicts_permissions_and_cleanup(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-08.19 `PACK_IN_PROGRESS` (+ `details.pack_id`), `NO_EVIDENCE`; API-137 người khác → 404, ADMIN →
    200; TC-08.21 quá 24 giờ → J-10 xóa file + dòng → 404; encode lỗi → `FAILED`, thư mục bị dọn."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    claim, _, _ = await _issue_claim(db, media_settings, station)
    owner = await _login(api, db, "tst_cskh_pk3")
    other = await _login(api, db, "tst_cskh_pk4")
    admin = await _login(api, db, "tst_admin_pk", "ADMIN")
    first = (await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=owner)).json()
    busy = await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=owner)
    assert (busy.status_code, busy.json()["error"]["code"]) == (409, "PACK_IN_PROGRESS")
    assert busy.json()["error"]["details"]["pack_id"] == first["id"]
    empty = Claim(
        package_id=claim.package_id, type="OTHER", counterparty="PLATFORM", status="NEW", source="MANUAL"
    )
    db.add(empty)
    await db.flush()
    no_evidence = await api.post(f"/api/v1/claims/{empty.id}/evidence-packs", headers=owner)
    assert (no_evidence.status_code, no_evidence.json()["error"]["code"]) == (409, "NO_EVIDENCE")
    assert (await api.get(f"/api/v1/evidence-packs/{first['id']}", headers=other)).status_code == 404
    assert (await api.get(f"/api/v1/evidence-packs/{first['id']}", headers=admin)).status_code == 200

    async def broken(*_: Any) -> Rendered:
        raise ffmpeg.FFmpegError("encode lỗi")

    assert (
        await packs.build_evidence_pack(db, uuid.UUID(first["id"]), media_settings, render=broken) == "FAILED"
    )
    failed = (await api.get(f"/api/v1/evidence-packs/{first['id']}", headers=owner)).json()
    assert (failed["status"], failed["files"]) == ("FAILED", None)
    assert not (media_settings.video_root / packs.pack_dir(uuid.UUID(first["id"]))).exists()

    second = (await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=owner)).json()
    await packs.build_evidence_pack(db, uuid.UUID(second["id"]), media_settings, render=fake_render)
    folder = media_settings.video_root / packs.pack_dir(uuid.UUID(second["id"]))
    assert folder.exists()
    clock.advance(timedelta(hours=25))
    owner = await _login(api, db, "tst_cskh_pk5")
    assert await packs.cleanup_expired(db, media_settings) == 2  # + gói FAILED (có expires_at — G3 R11)
    assert not folder.exists()
    assert await db.get(EvidencePack, uuid.UUID(second["id"]), populate_existing=True) is None


async def test_info_json_clock_from_close_time(db: AsyncSession) -> None:
    """TC-02.40, TC-02.41 (DEC-261): `info.json` lấy độ lệch giờ đã chụp lúc đóng phiên, không đọc camera lúc
    xuất; phiên trước nâng cấp (không có `camera_clock`) → null."""
    _, station = await make_station_account(db)
    _, (package,) = await make_order(db, 11)
    pack = await pack_session_with_clips(db, station, package, T0)
    pack.flags = ["CAM2_UNVERIFIED"]
    pack.camera_clock = [{"camera_role": "CAM1", "clock_offset_ms": 120, "checked_at": None}]
    fields = session_info_fields(pack)
    assert (fields["session_type"], fields["session_status"], fields["flags"]) == (
        "PACK",
        "COMPLETED",
        ["CAM2_UNVERIFIED"],
    )
    assert fields["cameras"][0] == {"camera_role": "CAM1", "clock_offset_ms": 120, "clock_checked_at": None}
    pack.camera_clock = None
    assert {c["clock_offset_ms"] for c in session_info_fields(pack)["cameras"]} == {None}


async def test_pack_snapshot_checksum_mismatch(
    db: AsyncSession, media_settings: Settings, api: AsyncClient
) -> None:
    """G3 B-2: ảnh trên đĩa khác SHA-256 trong DB → gói vẫn có ảnh, `missing` ghi
    `SNAPSHOT_CHECKSUM_MISMATCH`."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    claim, _, ret = await _issue_claim(db, media_settings, station)
    snap = (
        await db.scalars(select(Snapshot).where(Snapshot.session_id == ret.id).order_by(Snapshot.taken_at))
    ).first()
    assert snap is not None
    assert snap.path
    (media_settings.video_root / snap.path).write_bytes(b"bi-sua")
    headers = await _login(api, db, "tst_cskh_pk_sha")
    pack_id = (await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=headers)).json()["id"]

    assert (
        await packs.build_evidence_pack(db, uuid.UUID(pack_id), media_settings, render=fake_render) == "READY"
    )

    row = await db.get(EvidencePack, uuid.UUID(pack_id), populate_existing=True)
    assert row is not None
    reasons = [m for m in row.missing if m["reason"] == "SNAPSHOT_CHECKSUM_MISMATCH"]
    assert len(reasons) == 1
    assert reasons[0]["snapshot_id"] == str(snap.id)
    assert reasons[0]["sha256_db"] == snap.sha256


async def test_pack_unexpected_error_marks_failed(
    db: AsyncSession, media_settings: Settings, api: AsyncClient
) -> None:
    """G3 R11: lỗi bất kỳ (không phải FFmpeg / OSError) → FAILED + `expires_at` + thư mục được dọn."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    claim, _, _ = await _issue_claim(db, media_settings, station)
    headers = await _login(api, db, "tst_cskh_pk_err")
    pack_id = uuid.UUID(
        (await api.post(f"/api/v1/claims/{claim.id}/evidence-packs", headers=headers)).json()["id"]
    )

    async def broken_render(*_: Any, **__: Any) -> Rendered:
        raise KeyError("lỗi lập trình")

    assert await packs.build_evidence_pack(db, pack_id, media_settings, render=broken_render) == "FAILED"
    row = await db.get(EvidencePack, pack_id, populate_existing=True)
    assert row is not None
    assert row.status == "FAILED"
    assert row.expires_at == T0 + timedelta(hours=media_settings.evidence_pack_ttl_hours)
    assert not (media_settings.video_root / packs.pack_dir(pack_id)).exists()
