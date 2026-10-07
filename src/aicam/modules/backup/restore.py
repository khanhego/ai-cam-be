"""Lệnh vận hành API-186 `aicam backup-restore` / `aicam backup-verify` (02 §6.2, 02a §2; FR-02.16, NFR-40;
EX-K8; DEC-495, 499, 518). Runbook: `docs/ops.md` §6.2.

Không bao giờ đánh `DELETED`: clip / ảnh không có tệp sau khôi phục → `MISSING` (J-23 không xóa bản cloud).
Sau khi khôi phục DB: `backup_enabled = false`, `backup_restore_pending = true` tới khi `backup-verify` đạt.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aicam.core import audit, clock
from aicam.core.settings import Settings
from aicam.modules.backup import service, transfer
from aicam.modules.backup.jobs import DB_PREFIX, IMPORTS_PREFIX, pg_env
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import CloudError, ObjectStore
from aicam.modules.settings.models import Setting

log = structlog.get_logger()

EXIT_OK = 0
EXIT_VERIFY_FAILED = 1
EXIT_REFUSED = 2  # khóa DB sai / DB đích không trống / không có bản DB — không ghi gì
EXIT_PARTIAL = 3  # có đối tượng bằng chứng giải mã lỗi / thiếu khóa — đã làm hết phần còn lại


@dataclass
class Report:
    lines: list[str] = field(default_factory=list)
    exit_code: int = EXIT_OK

    def say(self, line: str) -> None:
        self.lines.append(line)


def read_key_files(paths: list[Path]) -> list[bytes]:
    """`--key-file F` (mỗi tệp một khóa base64) — khóa cũ tìm lại được sau này (DEC-495)."""
    return [crypto.parse_key(p.read_text().strip()) for p in paths]


def latest_db_key(store: ObjectStore) -> str | None:
    """`--db latest`: khóa đối tượng có tên (dấu thời gian UTC) lớn nhất dưới `backup/db/`."""
    keys = sorted(o.key for o in store.list(DB_PREFIX) if o.key.endswith(".dump.enc"))
    return keys[-1] if keys else None


async def _db_is_empty(settings: Settings) -> bool:
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            n = await conn.scalar(
                text("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
            )
    finally:
        await engine.dispose()
    return int(n or 0) == 0


async def _pg_restore(settings: Settings, dump: Path, *, force: bool) -> None:
    args = [settings.backup_pg_restore_bin, "--no-owner", "--exit-on-error"]
    if force:
        args += ["--clean", "--if-exists"]
    env = pg_env(settings.database_url)
    args += ["-d", env["PGDATABASE"]]
    with dump.open("rb") as stdin:
        proc = await asyncio.create_subprocess_exec(
            *args, stdin=stdin, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env
        )
        _, err = await proc.communicate()
    if proc.returncode != 0:
        tail = (err or b"").decode(errors="replace").strip().splitlines()[-3:]
        raise RuntimeError("pg_restore lỗi: " + " | ".join(tail))


async def mark_restore_pending(db: AsyncSession) -> None:
    """DEC-499: sao lưu tắt tới khi `backup-verify` đạt (J-20..J-23 không chạy — `state =
    RESTORE_PENDING`)."""
    await db.execute(
        update(Setting).where(Setting.id == 1).values(backup_enabled=False, backup_restore_pending=True)
    )
    await db.commit()


async def restore_db(
    settings: Settings,
    report: Report,
    *,
    db_key: str,
    keys: dict[str, bytes],
    force: bool,
    store: ObjectStore,
) -> bool:
    """(1) Tải + giải mã DB dump (tệp tạm, kiểm SHA-256 với metadata) → `pg_restore` vào DB trống. Khóa không
    khớp / DB không trống → từ chối, **không ghi** gì (mã 2)."""
    object_key = latest_db_key(store) if db_key == "latest" else db_key
    if object_key is None:
        report.say("Không tìm thấy bản sao DB nào dưới backup/db/ trên kho lưu.")
        report.exit_code = EXIT_REFUSED
        return False
    head = await asyncio.to_thread(store.head, object_key)
    if head is None:
        report.say(f"Không thấy đối tượng {object_key}.")
        report.exit_code = EXIT_REFUSED
        return False

    def _peek() -> crypto.Header:
        with store.get_stream(object_key) as body:
            return crypto.read_header(body)

    header = await asyncio.to_thread(_peek)
    if header.fingerprint not in keys:
        report.say(f"Khóa giải mã không khớp (dấu vân tay {header.fingerprint}) — không ghi gì.")
        report.exit_code = EXIT_REFUSED
        return False
    if not force and not await _db_is_empty(settings):
        report.say("DB đích không trống — từ chối khôi phục (dùng --force để ghi đè có chủ đích).")
        report.exit_code = EXIT_REFUSED
        return False
    tmp = Path(tempfile.mkdtemp(prefix="aicam-restore-", dir=_tmp_root(settings)))
    try:
        dump = tmp / "aicam.dump"
        result = await asyncio.to_thread(transfer.download_decrypt_to, store, object_key, dump, keys)
        expected = head.metadata.get("sha256")
        if expected and expected != result.sha256:
            report.say("Bản DB tải về lệch SHA-256 với metadata — dừng, không ghi DB.")
            report.exit_code = EXIT_REFUSED
            return False
        started = clock.now()
        await _pg_restore(settings, dump, force=force)
        report.say(
            f"Đã khôi phục DB từ {object_key} (khóa {header.fingerprint}, "
            f"{result.plain_size:,} byte, pg_restore {(clock.now() - started).total_seconds():.1f} giây)."
        )
        await _restore_imports(settings, report, object_key, keys, store, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return True


def imports_key_for(db_object_key: str) -> str:
    """`backup/db/…/aicam-{stamp}.dump.enc` → `backup/imports/{stamp}.tgz.enc` (cùng lượt J-20)."""
    stamp = db_object_key.rsplit("/", 1)[-1].removeprefix("aicam-").removesuffix(".dump.enc")
    return f"{IMPORTS_PREFIX}{stamp}.tgz.enc"


async def _restore_imports(
    settings: Settings, report: Report, db_key: str, keys: dict[str, bytes], store: ObjectStore, tmp: Path
) -> None:
    """File nhập đơn gốc cùng lượt (FR-02.08 a) → giải nén vào `IMPORT_ROOT` (không ghi đè tệp đã có)."""
    import tarfile

    key = imports_key_for(db_key)
    if await asyncio.to_thread(store.head, key) is None:
        report.say(f"Không có bản file nhập {key} — bỏ qua.")
        return
    tgz = tmp / "imports.tgz"
    try:
        await asyncio.to_thread(transfer.download_decrypt_to, store, key, tgz, keys)
    except (crypto.CryptoError, CloudError) as exc:
        report.say(f"Không khôi phục được file nhập ({type(exc).__name__}) — DB vẫn đã khôi phục.")
        return

    def _extract() -> int:
        root = settings.import_root
        root.mkdir(parents=True, exist_ok=True)
        n = 0
        with tarfile.open(tgz, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.startswith("imports/"):
                    continue
                rel = member.name.removeprefix("imports/")
                target = root / rel
                if target.exists():
                    continue
                member.name = rel
                tar.extract(member, root, filter="data")
                n += 1
        return n

    try:
        n = await asyncio.to_thread(_extract)
    except (OSError, tarfile.TarError) as exc:
        report.say(f"Không giải nén được file nhập vào {settings.import_root} ({exc}) — DB vẫn đã khôi phục.")
        return
    report.say(f"Đã khôi phục {n} file nhập vào {settings.import_root}.")


def _report_failures(settings: Settings, report: Report, stats: EvidenceStats) -> None:
    for kind, oid, key, reason, fp in stats.failures[:50]:
        report.say(f"  lỗi {reason}: {kind} {oid} ({key}{', khóa ' + fp if fp else ''})")


def _tmp_root(settings: Settings) -> str:
    root = settings.backup_tmp_dir
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


async def restore(
    settings: Settings,
    *,
    db_key: str | None = "latest",
    key_files: list[Path] | None = None,
    force: bool = False,
    evidence: bool = False,
    evidence_only: bool = False,
    target_dir: Path | None = None,
    store: ObjectStore | None = None,
) -> Report:
    """API-186 `aicam backup-restore`."""
    from aicam.core.db import dispose_engine, init_engine, sessionmaker

    report = Report()
    store = store or cloud.backup_store(settings)
    try:
        keys = service.keyring(settings, read_key_files(key_files or []))
    except ValueError as exc:
        report.say(f"Khóa không hợp lệ: {exc}")
        report.exit_code = EXIT_REFUSED
        return report
    if not keys:
        report.say("Chưa có khóa giải mã (BACKUP_ENCRYPTION_KEY / BACKUP_OLD_KEYS / --key-file).")
        report.exit_code = EXIT_REFUSED
        return report
    started = clock.now()
    try:
        restore_dump = db_key is not None and not evidence_only
        if restore_dump and not await restore_db(
            settings, report, db_key=db_key or "latest", keys=keys, force=force, store=store
        ):
            return report
        init_engine(settings.database_url)
        try:
            async with sessionmaker()() as db:
                if db_key is not None and not evidence_only:
                    await mark_restore_pending(db)
                    report.say(
                        "Sao lưu tự động đã TẮT (Chờ kiểm khôi phục) tới khi `aicam backup-verify` đạt."
                    )
                if evidence or evidence_only:
                    t = clock.now()
                    stats = await restore_evidence(
                        settings, db, report, keys=keys, store=store, target_dir=target_dir,
                        evidence_only=evidence_only,
                    )  # fmt: skip
                    report.say(
                        f"Bằng chứng: {stats.summary()} — {(clock.now() - t).total_seconds():.1f} giây."
                    )
                    _report_failures(settings, report, stats)
        finally:
            await dispose_engine()
    except CloudError as exc:
        report.say(f"Lỗi kho lưu: {exc.message}")
        report.exit_code = EXIT_REFUSED
        return report
    report.say(f"Xong sau {(clock.now() - started).total_seconds():.1f} giây.")
    log.info("backup_restore", exit_code=report.exit_code)
    return report


# ---------------------------------------------------------------- bằng chứng (EX-K8, DEC-499)


@dataclass
class EvidenceStats:
    downloaded: int = 0
    skipped_present: int = 0
    skipped_deleted: int = 0
    outside_db: int = 0
    marked_missing: int = 0
    recovered: int = 0
    failures: list[tuple[str, str, str, str, str]] = field(default_factory=list)  # kind, id, key, reason, fp

    def summary(self) -> str:
        decrypt = sum(1 for f in self.failures if f[3] == "DECRYPT_FAILED")
        unknown = sum(1 for f in self.failures if f[3] == "UNKNOWN_KEY")
        other = len(self.failures) - decrypt - unknown
        return (
            f"tải {self.downloaded} / thiếu (MISSING) {self.marked_missing} / ngoài DB {self.outside_db} / "
            f"giải mã lỗi {decrypt} / thiếu khóa {unknown}"
            + (f" / lỗi tải {other}" if other else "")
            + f" (đã có trên đĩa {self.skipped_present}, nguồn đã xóa theo lưu trữ {self.skipped_deleted},"
            f" về lại bình thường {self.recovered})"
        )


@dataclass
class _Row:
    kind: str
    id: str
    status: str
    path: str | None
    sha256: str | None
    session_id: str


async def _evidence_rows(db: AsyncSession) -> dict[str, _Row]:
    from sqlalchemy import select

    from aicam.modules.media.models import Clip, Snapshot

    rows: dict[str, _Row] = {}
    for kind, model in (("CLIP", Clip), ("SNAPSHOT", Snapshot)):
        for oid, status, rel, sha, sid in (
            await db.execute(select(model.id, model.status, model.path, model.sha256, model.session_id))
        ).all():
            rows[str(oid)] = _Row(kind, str(oid), status, rel, sha, str(sid))
    return rows


async def _open_claim_sessions(db: AsyncSession) -> set[str]:
    """FR-02.16: bằng chứng của hồ sơ khiếu nại chưa đóng về trước."""
    from sqlalchemy import select

    from aicam.modules.claims.models import Claim, ClaimEvidence

    rows = await db.execute(
        select(ClaimEvidence.session_id, ClaimEvidence.snapshot_id)
        .join(Claim, Claim.id == ClaimEvidence.claim_id)
        .where(Claim.status != "CLOSED", ClaimEvidence.removed_at.is_(None))
    )
    return {str(a or b) for a, b in rows.all()}


def _target(root: Path, rel: str) -> Path | None:
    path = (root / rel).resolve()
    return path if path.is_relative_to(root.resolve()) else None


async def _set_status(db: AsyncSession, row: _Row, status: str, *, only_from: tuple[str, ...]) -> bool:
    """Đổi trạng thái clip / ảnh (chỉ `READY` ↔ `MISSING` — **không bao giờ** `DELETED`)."""
    from aicam.modules.media.models import Clip, Snapshot

    model: Any = Clip if row.kind == "CLIP" else Snapshot
    result = await db.execute(
        update(model).where(model.id == uuid.UUID(row.id), model.status.in_(only_from)).values(status=status)
    )
    await db.commit()
    changed = bool(result.rowcount)  # type: ignore[attr-defined]
    if changed:
        row.status = status
    return changed


async def restore_evidence(
    settings: Settings,
    db: AsyncSession,
    report: Report,
    *,
    keys: dict[str, bytes],
    store: ObjectStore,
    target_dir: Path | None = None,
    evidence_only: bool = False,
) -> EvidenceStats:
    """(2) Duyệt `backup/evidence/` trên cloud (`list` + `HEAD` metadata `kind`, `id`, `relpath`, `sha256`)
    so với
    DB vừa khôi phục: có dòng → tải về `relpath` (hồ sơ khiếu nại chưa đóng trước); không có dòng (tải sau bản
    dump) → vẫn tải, in "ngoài DB"; clip / ảnh `READY` không có tệp, không có đối tượng → `MISSING`.
    Mỗi đối tượng giải mã vào tệp tạm, chỉ đổi tên khi xác thực xong; lỗi → không ghi tệp, chạy tiếp.
    (3) `evidence_only`: chỉ xét clip / ảnh `MISSING` có đối tượng cloud (tìm lại khóa cũ — `--key-file`)."""
    from aicam.modules.backup.jobs import EVIDENCE_PREFIX

    stats = EvidenceStats()
    root = target_dir or settings.video_root
    rows = await _evidence_rows(db)
    first = await _open_claim_sessions(db)
    await db.commit()
    objects = await asyncio.to_thread(lambda: list(store.list(EVIDENCE_PREFIX)))
    metas: list[tuple[int, str, dict[str, str]]] = []
    for info in objects:
        head = await asyncio.to_thread(store.head, info.key)
        meta = head.metadata if head else {}
        row = rows.get(meta.get("id", ""))
        priority = 0 if row and (row.session_id in first or row.id in first) else 1
        metas.append((priority, info.key, meta))
    metas.sort(key=lambda m: (m[0], m[1]))
    covered: set[str] = set()
    for _, key, meta in metas:
        oid, rel = meta.get("id", ""), meta.get("relpath", "")
        row = rows.get(oid)
        if row is not None:
            covered.add(oid)
        if evidence_only and (row is None or row.status != "MISSING"):
            continue
        if row is not None and row.status == "DELETED":
            stats.skipped_deleted += 1  # đã bị retention xóa — không đưa lại (J-23 sẽ dọn bản cloud)
            continue
        rel = (row.path if row and row.path else rel) or ""
        target = _target(root, rel) if rel else None
        if target is None:
            stats.failures.append((meta.get("kind", "?"), oid, key, "BAD_METADATA", meta.get("key-fp", "")))
            continue
        expected = row.sha256 if row else meta.get("sha256")
        if await asyncio.to_thread(target.is_file):
            actual, _ = await asyncio.to_thread(transfer.sha256_file, target)
            if actual == expected:
                stats.skipped_present += 1
                if (
                    row is not None
                    and row.status == "MISSING"
                    and await _set_status(db, row, "READY", only_from=("MISSING",))
                ):
                    stats.recovered += 1
                continue
        try:
            result = await asyncio.to_thread(transfer.download_decrypt_to, store, key, target, keys)
        except crypto.WrongKeyError as exc:
            stats.failures.append((meta.get("kind", "?"), oid, key, "UNKNOWN_KEY", exc.fingerprint))
            await _missing_if_absent(db, row, target, stats)
            continue
        except crypto.CryptoError:
            stats.failures.append((meta.get("kind", "?"), oid, key, "DECRYPT_FAILED", meta.get("key-fp", "")))
            await _missing_if_absent(db, row, target, stats)
            continue
        except CloudError as exc:
            stats.failures.append((meta.get("kind", "?"), oid, key, exc.code, meta.get("key-fp", "")))
            await _missing_if_absent(db, row, target, stats)
            continue
        stats.downloaded += 1
        if row is None:
            stats.outside_db += 1
            report.say(f"  ngoài DB (tải sau bản dump): {key} → {rel}")
            continue
        accepted = meta.get("integrity") == "MISMATCH_ACCEPTED"
        if result.sha256 == row.sha256 or accepted:
            if accepted and result.sha256 != row.sha256:
                await _record_accepted(db, row, result.sha256, key)
            if row.status == "MISSING" and await _set_status(db, row, "READY", only_from=("MISSING",)):
                stats.recovered += 1
    if not evidence_only:
        for row in rows.values():
            if row.status != "READY" or row.id in covered:
                continue
            target = _target(root, row.path) if row.path else None
            absent = target is None or not await asyncio.to_thread(target.is_file)
            if absent and await _set_status(db, row, "MISSING", only_from=("READY",)):
                stats.marked_missing += 1
    return stats


async def _missing_if_absent(db: AsyncSession, row: _Row | None, target: Path, stats: EvidenceStats) -> None:
    """Đối tượng hỏng / thiếu khóa và đĩa không có tệp → `MISSING` (EX-K8) — không bao giờ `DELETED`."""
    if row is None or row.status != "READY":
        return
    if not await asyncio.to_thread(target.is_file) and await _set_status(
        db, row, "MISSING", only_from=("READY",)
    ):
        stats.marked_missing += 1


async def _record_accepted(db: AsyncSession, row: _Row, actual: str, key: str) -> None:
    """Metadata `integrity=MISMATCH_ACCEPTED` (API-188 sau bản dump) → `backup_object.hash_override` để
    `backup-verify` tính "lệch đã chấp nhận" (DEC-518)."""
    from sqlalchemy import select

    from aicam.modules.backup.models import BackupObject

    col = BackupObject.clip_id if row.kind == "CLIP" else BackupObject.snapshot_id
    obj = await db.scalar(select(BackupObject).where(col == uuid.UUID(row.id)))
    if obj is None:
        obj = BackupObject(kind=row.kind, object_key=key, status="UPLOADED")
        if row.kind == "CLIP":
            obj.clip_id = uuid.UUID(row.id)
        else:
            obj.snapshot_id = uuid.UUID(row.id)
        db.add(obj)
    obj.hash_override, obj.sha256_actual = True, actual
    await db.commit()


# ---------------------------------------------------------------- backup-verify


@dataclass
class VerifyResult:
    ok: int = 0
    accepted: int = 0
    mismatch: list[tuple[str, str]] = field(default_factory=list)  # (kind, id)
    missing_known: int = 0
    missing: list[tuple[str, str]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.mismatch and not self.missing


async def _scan(db: AsyncSession, settings: Settings) -> VerifyResult:
    """Băm từng clip / ảnh `READY` trên đĩa so với DB; `MISSING` = thiếu đã ghi nhận."""
    from sqlalchemy import select

    from aicam.modules.backup.models import BackupObject
    from aicam.modules.media import service as media
    from aicam.modules.media.models import Clip, Snapshot

    res = VerifyResult()
    accepted = {
        (r.clip_id or r.snapshot_id): r.sha256_actual
        for r in (await db.scalars(select(BackupObject).where(BackupObject.hash_override.is_(True)))).all()
    }
    for kind, model in (("CLIP", Clip), ("SNAPSHOT", Snapshot)):
        rows = (await db.execute(select(model.id, model.status, model.path, model.sha256))).all()
        for oid, status, rel, sha in rows:
            if status == "MISSING":
                res.missing_known += 1
                continue
            if status != "READY":
                continue
            path = None
            if rel:
                try:
                    path = media.absolute(settings, rel)
                except ValueError:
                    path = None
            if path is None or not await asyncio.to_thread(path.is_file):
                res.missing.append((kind, str(oid)))
                continue
            actual, _ = await asyncio.to_thread(transfer.sha256_file, path)
            if actual == sha:
                res.ok += 1
            elif accepted.get(oid) == actual:
                res.accepted += 1
            else:
                res.mismatch.append((kind, str(oid)))
    await db.commit()
    return res


async def verify(settings: Settings, db: AsyncSession) -> Report:
    """API-186 `aicam backup-verify`: đạt (lệch = 0 và thiếu = 0) → xóa `backup_restore_pending` + audit
    `BACKUP_RESTORE_VERIFIED` → mã 0 (Admin bật lại ở D23); không đạt → mã 1."""
    from aicam.modules.settings import service as settings_service

    report = Report()
    res = await _scan(db, settings)
    report.say(
        f"khớp {res.ok} / lệch đã chấp nhận {res.accepted} / lệch {len(res.mismatch)} / "
        f"thiếu đã ghi nhận {res.missing_known} / thiếu {len(res.missing)}"
    )
    for kind, oid in (res.mismatch + res.missing)[:50]:
        state_ = "lệch" if (kind, oid) in res.mismatch else "thiếu"
        report.say(f"  {state_}: {kind} {oid}")
    if not res.passed:
        report.say("KHÔNG ĐẠT — xem từng tệp lệch / thiếu (docs/ops.md §6.2 'Lối ra').")
        report.exit_code = EXIT_VERIFY_FAILED
        return report
    cfg = await settings_service.get(db)
    if cfg.backup_restore_pending:
        cfg.backup_restore_pending = False
        audit.record(db, "BACKUP_RESTORE_VERIFIED", user_id=None, object_type="SETTING", object_id="1",
                     data={"ok": res.ok, "accepted": res.accepted, "missing_known": res.missing_known,
                           "os_user": _os_user()})  # fmt: skip
        report.say("ĐẠT — đã gỡ 'Chờ kiểm khôi phục'. Admin bật lại sao lưu ở Dashboard → Sao lưu cloud.")
    else:
        report.say("ĐẠT.")
    await db.commit()
    log.info("backup_verify", ok=res.ok, accepted=res.accepted, mismatch=0, missing_known=res.missing_known)
    return report


def _os_user() -> str:
    try:
        return os.getlogin()
    except OSError:
        return os.environ.get("USER", "?")
