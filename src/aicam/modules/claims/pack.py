"""Gói bằng chứng hồ sơ khiếu nại (FR-08.05, AC-25, UC-12): API-136..138, J-16 `claims.build_evidence_pack`,
dọn J-10 (02 §6.2 "API-136 / API-137 / API-138", 02a §7 J-16).

Nội dung zip (`ZIP_STORED` — video đã nén):

```
KN-000124/
├── ho-so.json, README.txt
├── 01-dong-goi-<yyyymmdd-hhmm>/   video-ghep-co-chu.mp4, goc-CAM1.mp4, goc-CAM2.mp4,
│                                  anh-luc-dong-goi.jpg, info.json
├── 02-mo-hoan-<yyyymmdd-hhmm>/    như trên + anh-01.jpg … + ket-luan.json
└── 03-phien-khac-<…>/             chỉ clip gốc + info.json (không encode)
```

Clip gốc chép nguyên (SHA-256 kiểm lại sau khi chép, phải bằng DB — ADR-008). Clip / ảnh đã xóa hoặc chưa
cắt → zip vẫn tạo, ghi vào `missing` (không lỗi). Encode / ghi zip lỗi → `FAILED`.
"""

import asyncio
import json
import shutil
import uuid
import zipfile
from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam import __version__
from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.claims.models import Claim, ClaimEvidence, EvidencePack
from aicam.modules.claims.schemas import EvidencePackCreated, EvidencePackOut, PackFiles, PackMissing
from aicam.modules.claims.service import CONCLUSION_LABELS
from aicam.modules.media import ffmpeg, jobs, signing
from aicam.modules.media.exports import (
    Progress,
    Rendered,
    render_side_by_side_to,
    session_info_fields,
    session_lock,
)
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.media.service import absolute, is_first_byte
from aicam.modules.orders.models import Order, Package
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions import inspection
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station
from aicam.modules.users.queries import get_user_ref

log = structlog.get_logger()

Renderer = Callable[
    [AsyncSession, PackSession, list[Clip], Path, Settings, Progress | None], Awaitable[Rendered]
]
ROLES = ("CAM1", "CAM2")
ACTIVE = ("QUEUED", "RUNNING")
README = """GÓI BẰNG CHỨNG — {code}

Tạo bởi Hệ thống X lúc {at} (giờ Việt Nam) cho {who}.

Thư mục:
- ho-so.json: thông tin hồ sơ khiếu nại, danh sách mọi tệp kèm mã SHA-256, phần còn thiếu.
- 01-dong-goi-…: phiên đóng gói — video ghép Cam 1 + Cam 2 có chữ (mã vận đơn, mã đơn, giờ, station),
  clip gốc từng camera (goc-CAM1.mp4, goc-CAM2.mp4), ảnh lúc đóng gói, info.json.
- 02-mo-hoan-…: phiên mở hàng hoàn — như trên + ảnh chụp khi kiểm (anh-01.jpg …) + ket-luan.json
  (kết luận, từng dòng hàng, người kiểm, lịch sử sửa kết luận).
- 03-phien-khac-…: phiên khác được thêm làm bằng chứng — chỉ clip gốc + info.json.

Kiểm tính toàn vẹn: clip gốc không bị sửa nếu mã SHA-256 của tệp trùng với mã ghi trong ho-so.json và
info.json.
  Windows:  certutil -hashfile goc-CAM1.mp4 SHA256
  macOS / Linux:  shasum -a 256 goc-CAM1.mp4
Video ghép có chữ là bản dựng lại từ clip gốc để xem dễ hơn; bằng chứng gốc là các tệp goc-*.mp4.
"""


def pack_dir(pack_id: uuid.UUID) -> str:
    return f"exports/pack-{pack_id}"


# ---------------------------------------------------------------- API-136


async def create_pack(
    db: AsyncSession, claim_id: uuid.UUID, p: Principal, settings: Settings
) -> EvidencePackCreated:
    """API-136: hồ sơ phải có bằng chứng (`409 NO_EVIDENCE`); một gói đang chạy / hồ sơ
    (`409 PACK_IN_PROGRESS`). Gói `QUEUED`/`RUNNING` quá `EVIDENCE_PACK_TIMEOUT_S` (worker chết) → `FAILED`
    rồi cho tạo lại."""
    claim = await db.get(Claim, claim_id)
    if claim is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
    if await db.scalar(select(ClaimEvidence.id).where(ClaimEvidence.claim_id == claim.id).limit(1)) is None:
        raise AppError("NO_EVIDENCE", "Hồ sơ chưa có bằng chứng.", 409)
    active = await db.scalar(
        select(EvidencePack)
        .where(EvidencePack.claim_id == claim.id, EvidencePack.status.in_(ACTIVE))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if active is not None:
        if active.created_at < clock.now() - timedelta(seconds=settings.evidence_pack_timeout_s + 60):
            active.status, active.error = "FAILED", "Quá thời gian tạo gói (worker dừng giữa chừng)"
            await db.flush()
        else:
            raise _in_progress(active.id)
    pack = EvidencePack(claim_id=claim.id, status="QUEUED", progress=0, missing=[], created_by=p.user_id)
    try:
        async with db.begin_nested():
            db.add(pack)
            await db.flush()
    except IntegrityError as exc:  # hai request cùng lúc — partial unique gói đang chạy
        existing = await db.scalar(
            select(EvidencePack.id).where(EvidencePack.claim_id == claim.id, EvidencePack.status.in_(ACTIVE))
        )
        raise _in_progress(existing) from exc
    audit.record(db, "EXPORT_CLAIM_PACK", user_id=p.user_id, object_type="CLAIM", object_id=claim.id, ip=p.ip,
                 data={"pack_id": str(pack.id), "code": claim.code})  # fmt: skip
    jobs.enqueue_build_evidence_pack(db, pack.id)
    out = EvidencePackCreated(id=pack.id, status="QUEUED", progress=0)
    await commit(db)
    return out


def _in_progress(pack_id: uuid.UUID | None) -> AppError:
    return AppError(
        "PACK_IN_PROGRESS",
        "Hồ sơ đang có gói bằng chứng đang tạo.",
        409,
        {"pack_id": str(pack_id) if pack_id else None},
    )


# ---------------------------------------------------------------- API-137


def pack_out(pack: EvidencePack, uid: uuid.UUID, settings: Settings) -> EvidencePackOut:
    files = None
    if pack.status == "READY" and pack.expires_at and pack.expires_at > clock.now():
        exp = signing.expiry(settings.media_url_ttl_s)
        files = PackFiles(zip=signing.pack_url(settings.media_signing_key, pack.id, uid, exp))
    return EvidencePackOut(
        id=pack.id,
        claim_id=pack.claim_id,
        status=pack.status,
        progress=pack.progress,
        sha256=pack.sha256,
        size_bytes=pack.size_bytes,
        missing=[PackMissing(**m) for m in pack.missing or []],
        files=files,
        expires_at=pack.expires_at,
    )


async def get_pack(db: AsyncSession, pack_id: uuid.UUID, p: Principal, settings: Settings) -> EvidencePackOut:
    """Người tạo hoặc ADMIN; người khác / quá hạn → 404 (02 API-137)."""
    pack = await db.get(EvidencePack, pack_id)
    if (
        pack is None
        or (pack.created_by != p.user_id and p.role != "ADMIN")
        or (pack.expires_at is not None and pack.expires_at <= clock.now())
    ):
        raise AppError("NOT_FOUND", "Không tìm thấy gói bằng chứng. Tạo gói mới.", 404)
    return pack_out(pack, p.user_id, settings)


# ---------------------------------------------------------------- API-138


async def open_pack_file(
    db: AsyncSession,
    pack_id: uuid.UUID,
    *,
    uid: uuid.UUID,
    exp: int,
    sig: str,
    range_header: str | None,
    ip: str | None,
    settings: Settings,
) -> tuple[Path, str]:
    """Kiểm chữ ký `pack:{id}:pack.zip:{uid}:{exp}`; audit `DOWNLOAD_CLAIM_PACK` khi tải từ byte 0."""
    if not signing.is_valid(settings.media_signing_key, signing.pack_message(pack_id, uid, exp), sig, exp):
        raise AppError("SIGNATURE_INVALID", "Liên kết đã hết hạn hoặc không hợp lệ. Tải lại trang.", 403)
    pack = await db.get(EvidencePack, pack_id)
    if (
        pack is None
        or pack.status != "READY"
        or not pack.path
        or not pack.expires_at
        or pack.expires_at <= clock.now()
    ):
        raise AppError("NOT_FOUND", "Gói bằng chứng không còn. Tạo gói mới.", 404)
    path = absolute(settings, pack.path)
    if not path.is_file():
        raise AppError("NOT_FOUND", "Gói bằng chứng không còn. Tạo gói mới.", 404)
    claim = await db.get(Claim, pack.claim_id)
    if is_first_byte(range_header):
        audit.record(db, "DOWNLOAD_CLAIM_PACK", user_id=uid, object_type="CLAIM", object_id=pack.claim_id,
                     ip=ip, data={"pack_id": str(pack.id), "sha256": pack.sha256})  # fmt: skip
        await commit(db)
    return path, f"{claim.code if claim else pack.id}.zip"


# ---------------------------------------------------------------- J-16


async def _publish(db: AsyncSession, pack: EvidencePack, settings: Settings) -> None:
    from aicam.realtime import publish

    data = pack_out(pack, pack.created_by, settings).model_dump(mode="json")
    await publish.to_user(pack.created_by, "evidence_pack.updated", data)


async def build_evidence_pack(
    db: AsyncSession, pack_id: uuid.UUID, settings: Settings, render: Renderer = render_side_by_side_to
) -> str:
    """J-16 (queue `export`, concurrency 1). Giao trùng (acks_late) → khóa advisory → SKIPPED."""
    async with session_lock(db, f"pack:{pack_id}") as got:
        if not got:
            log.warning("evidence_pack_already_running", pack_id=str(pack_id))
            return "SKIPPED"
        return await _build(db, pack_id, settings, render)


class _Builder:
    """Dựng thư mục hồ sơ + theo dõi tệp / phần thiếu / tiến độ."""

    def __init__(
        self,
        db: AsyncSession,
        pack: EvidencePack,
        root: Path,
        work: Path,
        settings: Settings,
        render: Renderer,
    ) -> None:
        self.db, self.pack, self.root, self.work, self.settings, self.render = (
            db,
            pack,
            root,
            work,
            settings,
            render,
        )
        self.missing: list[dict[str, Any]] = []
        self.last_pct = 0
        self.tz = ZoneInfo(settings.tz_display)

    async def progress(self, pct: int) -> None:
        pct = max(0, min(99, pct))
        if pct - self.last_pct >= 5:
            self.last_pct = pct
            self.pack.progress = pct
            await commit(self.db)
            await _publish(self.db, self.pack, self.settings)

    def miss(self, session_id: uuid.UUID, role: str, reason: str, **extra: Any) -> None:
        self.missing.append({"session_id": str(session_id), "camera_role": role, "reason": reason, **extra})

    def folder_name(self, index: int, pack: PackSession, main: bool) -> str:
        kind = "phien-khac" if not main else ("dong-goi" if pack.type == "PACK" else "mo-hoan")
        when = (pack.ended_at or pack.started_at).astimezone(self.tz).strftime("%Y%m%d-%H%M")
        return f"{index:02d}-{kind}-{when}"

    async def copy_clips(self, folder: Path, pack: PackSession) -> list[Clip]:
        """Chép clip gốc READY (kiểm SHA-256 sau khi chép); thiếu → `missing`. Trả clip đã chép."""
        clips = {
            c.camera_role: c
            for c in (await self.db.scalars(select(Clip).where(Clip.session_id == pack.id))).all()
        }
        copied: list[Clip] = []
        for role in ROLES:
            clip = clips.get(role)
            if clip is None:
                self.miss(pack.id, role, "CLIP_MISSING")
                continue
            if clip.status != "READY" or not clip.path:
                reason = {"DELETED": "CLIP_DELETED", "FAILED": "CLIP_FAILED"}.get(
                    clip.status, "CLIP_NOT_READY"
                )
                self.miss(pack.id, role, reason)
                continue
            src = absolute(self.settings, clip.path)
            dst = folder / f"goc-{role}.mp4"
            try:
                await asyncio.to_thread(shutil.copyfile, src, dst)
            except FileNotFoundError:
                self.miss(pack.id, role, "CLIP_FILE_MISSING")
                continue
            sha = await asyncio.to_thread(ffmpeg.sha256_file, dst)
            if sha != clip.sha256:  # clip gốc bất biến (ADR-008): lệch → ghi rõ, không che giấu
                self.miss(pack.id, role, "CLIP_CHECKSUM_MISMATCH", sha256_db=clip.sha256, sha256_file=sha)
            copied.append(clip)
        return copied

    async def copy_snapshot(self, snap: Snapshot, dst: Path) -> None:
        if snap.status != "READY" or not snap.path:
            self.miss(snap.session_id, "CAM1", "SNAPSHOT_DELETED", snapshot_id=str(snap.id))
            return
        try:
            await asyncio.to_thread(shutil.copyfile, absolute(self.settings, snap.path), dst)
        except FileNotFoundError:
            self.miss(snap.session_id, "CAM1", "SNAPSHOT_FILE_MISSING", snapshot_id=str(snap.id))
            return
        if snap.sha256:  # G3 B-2: ảnh gốc bất biến như clip — lệch SHA-256 → ghi rõ, không che giấu
            sha = await asyncio.to_thread(ffmpeg.sha256_file, dst)
            if sha != snap.sha256:
                self.miss(
                    snap.session_id, "CAM1", "SNAPSHOT_CHECKSUM_MISMATCH",
                    snapshot_id=str(snap.id), sha256_db=snap.sha256, sha256_file=sha,
                )  # fmt: skip


async def _session_rows(
    db: AsyncSession, claim: Claim
) -> tuple[list[tuple[PackSession, bool]], list[Snapshot]]:
    evidence = (
        await db.scalars(
            select(ClaimEvidence).where(ClaimEvidence.claim_id == claim.id).order_by(ClaimEvidence.added_at)
        )
    ).all()
    session_ids = [e.session_id for e in evidence if e.session_id]
    auto = {e.session_id for e in evidence if e.session_id and e.auto}
    sessions = []
    if session_ids:
        sessions = list((await db.scalars(select(PackSession).where(PackSession.id.in_(session_ids)))).all())
    # Phiên chính: tự chọn, phiên RETURN, phiên PACK `COMPLETED` (hiệu lực); còn lại = phiên thêm tay.
    rows = [
        (s, s.id in auto or s.type == "RETURN" or (s.type == "PACK" and s.status == "COMPLETED"))
        for s in sessions
    ]
    rows.sort(key=lambda r: (not r[1], r[0].type != "PACK", r[0].started_at, r[0].id))
    snapshot_ids = [e.snapshot_id for e in evidence if e.snapshot_id]
    snapshots = []
    if snapshot_ids:
        snapshots = list(
            (
                await db.scalars(
                    select(Snapshot)
                    .where(Snapshot.id.in_(snapshot_ids))
                    .order_by(Snapshot.taken_at, Snapshot.id)
                )
            ).all()
        )
    return rows, snapshots


async def _session_info(
    db: AsyncSession, pack: PackSession, claim: Claim, copied: list[Clip], rendered: Rendered | None, who: Any
) -> dict[str, Any]:
    package = await db.get(Package, pack.package_id)
    order = await db.get(Order, package.order_id) if package and package.order_id else None
    station = await db.get(Station, pack.station_id)
    return {
        "claim_code": claim.code,
        "session_id": str(pack.id),
        "tracking_number": pack.open_code
        if pack.type == "RETURN"
        else (package.tracking_number if package else None),
        "platform_order_sn": order.platform_order_sn if order else None,
        "station_name": station.name if station else None,
        "session_started_at": clock.iso_z(pack.started_at),
        "session_ended_at": clock.iso_z(pack.ended_at) if pack.ended_at else None,
        **session_info_fields(pack),
        "video": {
            "file": "video-ghep-co-chu.mp4",
            "sha256": rendered.sha256,
            "start_at": clock.iso_z(rendered.start),
            "end_at": clock.iso_z(rendered.end),
            "gaps": rendered.gaps,
        }
        if rendered
        else None,
        "source_clip_sha256": {c.camera_role: c.sha256 for c in copied},
        "exported_by": who,
        "exported_at": clock.iso_z(clock.now()),
        "generator": f"Hệ thống X (aicam {__version__})",
    }


async def _conclusion(db: AsyncSession, pack: PackSession) -> dict[str, Any]:
    """`ket-luan.json` (02 §6.3 #11, DEC-261): kết luận hiện tại + từng dòng + `corrections[]`."""
    lines = await inspection.lines_of(db, pack.id)
    return {
        "session_id": str(pack.id),
        "conclusion": pack.inspection_conclusion,
        "conclusion_label": CONCLUSION_LABELS.get(pack.inspection_conclusion or ""),
        "note": pack.inspection_note,
        "lines_mode": pack.inspection_lines_mode,
        "operator_name": pack.operator_name,
        "saved_at": clock.iso_z(pack.inspection_saved_at) if pack.inspection_saved_at else None,
        "lines": [
            {
                "order_item_id": str(line.order_item_id) if line.order_item_id else None,
                "product_name": line.product_name,
                "variation": line.variation,
                "quantity_sent": line.quantity_sent,
                "quantity_requested": line.quantity_requested,
                "quantity_received": line.quantity_received,
                "condition": line.condition,
                "note": line.note,
            }
            for line in lines
        ],
        "corrections": list(pack.inspection_corrections or []),
    }


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _file_list(work: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": f.relative_to(work).as_posix(),
            "sha256": ffmpeg.sha256_file(f),
            "size_bytes": f.stat().st_size,
        }
        for f in sorted(work.rglob("*"))
        if f.is_file()
    ]


def _zip(root: Path, work: Path, target: Path) -> None:
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_STORED) as zf:
        for f in sorted(work.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(root).as_posix())


async def _build(db: AsyncSession, pack_id: uuid.UUID, settings: Settings, render: Renderer) -> str:
    pack = await db.get(EvidencePack, pack_id, populate_existing=True)
    if pack is None or pack.status not in ACTIVE:
        return "SKIPPED"
    pack.status, pack.progress = "RUNNING", 0
    await commit(db)
    await _publish(db, pack, settings)
    root = absolute(settings, pack_dir(pack.id))
    try:
        claim = await db.get(Claim, pack.claim_id)
        assert claim is not None  # noqa: S101 — FK CASCADE
        shutil.rmtree(root, ignore_errors=True)
        work = root / claim.code
        work.mkdir(parents=True)
        b = _Builder(db, pack, root, work, settings, render)
        creator = await get_user_ref(db, pack.created_by)
        who = {"id": str(pack.created_by), "display_name": creator.display_name if creator else None}
        rows, snapshots = await _session_rows(db, claim)
        by_session: dict[uuid.UUID, list[Snapshot]] = {}
        for snap in snapshots:
            by_session.setdefault(snap.session_id, []).append(snap)
        renders = max(1, sum(1 for _, main in rows if main))
        done = 0
        for index, (session, main) in enumerate(rows, start=1):
            folder = work / b.folder_name(index, session, main)
            folder.mkdir()
            copied = await b.copy_clips(folder, session)
            rendered = None
            if main and copied:
                base, share = 5 + 85 * done // renders, 85 // renders

                async def _progress(pct: int, base: int = base, share: int = share) -> None:
                    await b.progress(base + share * pct // 100)

                rendered = await render(
                    db, session, copied, folder / "video-ghep-co-chu.mp4", settings, _progress
                )
            if main:
                done += 1
            for n, snap in enumerate(by_session.pop(session.id, []), start=1):
                name = "anh-luc-dong-goi.jpg" if snap.kind == "PACK_CLOSE" else f"anh-{n:02d}.jpg"
                await b.copy_snapshot(snap, folder / name)
            _write_json(folder / "info.json", await _session_info(db, session, claim, copied, rendered, who))
            if session.type == "RETURN":
                _write_json(folder / "ket-luan.json", await _conclusion(db, session))
            await b.progress(5 + 85 * done // renders)
        leftovers = [s for group in by_session.values() for s in group]
        if leftovers:  # ảnh là bằng chứng mà phiên của ảnh không trong hồ sơ
            other = work / "anh-khac"
            other.mkdir()
            for n, snap in enumerate(leftovers, start=1):
                await b.copy_snapshot(snap, other / f"anh-{n:02d}.jpg")
        _write_json(work / "ho-so.json", await _summary(db, claim, work, b.missing, who))
        (work / "README.txt").write_text(
            README.format(
                code=claim.code,
                at=clock.now().astimezone(b.tz).strftime("%H:%M %d/%m/%Y"),
                who=who["display_name"] or who["id"],
            ),
            encoding="utf-8",
        )
        await b.progress(95)
        target = root / f"{claim.code}.zip"
        await asyncio.to_thread(_zip, root, work, target)
        shutil.rmtree(work, ignore_errors=True)
        pack.sha256 = await asyncio.to_thread(ffmpeg.sha256_file, target)
        pack.size_bytes = target.stat().st_size
        pack.path = f"{pack_dir(pack.id)}/{claim.code}.zip"
        pack.missing = b.missing
        pack.status, pack.progress, pack.error = "READY", 100, None
        pack.expires_at = clock.now() + timedelta(hours=settings.evidence_pack_ttl_hours)
        log.info("evidence_pack_ready", pack_id=str(pack.id), claim_id=str(claim.id), missing=len(b.missing))
    except Exception as exc:  # G3 R11: mọi lỗi (cả DB / lỗi lập trình) → FAILED + dọn, không kẹt RUNNING
        log.exception("evidence_pack_failed", pack_id=str(pack_id), error=str(exc)[:300])
        shutil.rmtree(root, ignore_errors=True)
        if isinstance(exc, SQLAlchemyError):  # transaction hỏng → lùi rồi đọc lại gói
            await db.rollback()
            failed = await db.get(EvidencePack, pack_id, populate_existing=True)
            if failed is None:
                return "SKIPPED"
            pack = failed
        pack.status, pack.error = "FAILED", (str(exc) or type(exc).__name__)[:500]
        pack.expires_at = clock.now() + timedelta(hours=settings.evidence_pack_ttl_hours)
    await commit(db)
    await _publish(db, pack, settings)
    return pack.status


async def _summary(
    db: AsyncSession, claim: Claim, work: Path, missing: list[dict[str, Any]], who: dict[str, Any]
) -> dict[str, Any]:
    """`ho-so.json`: hồ sơ, đơn, kiện, hồ sơ hàng hoàn, mọi tệp + SHA-256, phần thiếu, người / giờ xuất."""
    package = await db.get(Package, claim.package_id)
    order = await db.get(Order, claim.order_id) if claim.order_id else None
    case = await db.get(ReturnCase, claim.return_case_id) if claim.return_case_id else None
    files = await asyncio.to_thread(_file_list, work)
    return {
        "code": claim.code,
        "type": claim.type,
        "counterparty": claim.counterparty,
        "status": claim.status,
        "source": claim.source,
        "deadline_at": clock.iso_z(claim.deadline_at) if claim.deadline_at else None,
        "platform_claim_ref": claim.platform_claim_ref,
        "order": {"platform_order_sn": order.platform_order_sn} if order else None,
        "package": {"tracking_number": package.tracking_number} if package else None,
        "return_case": {
            "code": case.code,
            "kind": case.kind,
            "return_tracking_number": case.return_tracking_number,
            "platform_return_sn": case.platform_return_sn,
        }
        if case
        else None,
        "files": files,
        "missing": missing,
        "exported_by": who,
        "exported_at": clock.iso_z(clock.now()),
        "generator": f"Hệ thống X (aicam {__version__})",
    }


# ---------------------------------------------------------------- J-10 dọn


async def cleanup_expired(db: AsyncSession, settings: Settings) -> int:
    """Một phần J-10: gói quá `expires_at` (24 giờ) → xóa thư mục + dòng (02a §7)."""
    expired = (
        await db.scalars(
            select(EvidencePack).where(
                EvidencePack.expires_at.is_not(None), EvidencePack.expires_at < clock.now()
            )
        )
    ).all()
    for pack in expired:
        shutil.rmtree(absolute(settings, pack_dir(pack.id)), ignore_errors=True)
    if expired:
        await db.execute(delete(EvidencePack).where(EvidencePack.id.in_([p.id for p in expired])))
    await db.commit()
    return len(expired)
