"""NFR-41 (02a §11 `test_backup_security_nfr41`, T-280): bản sao trên kho lưu vô dụng nếu không có khóa, khóa
sao lưu không nằm ở dump DB / log / DB / metadata đối tượng / response API, khóa ứng dụng không xóa được phiên
bản hay tắt versioning ở bucket sao lưu (RK-28). Kèm schema guard J-23 trên `alembic_version` thật.

Phần MinIO chỉ chạy khi có `TEST_S3_ENDPOINT` (container MinIO tạm riêng — không phải stack dev); `pg_dump` /
`pg_restore` 16 thật (trong PATH hoặc container tạm `bin/docker-*`); `ffprobe` thật. Nhà cung cấp S3 thật
(Q20 — chính sách quyền, object lock): chưa test — thiếu tài nguyên.
"""

import base64
import hashlib
import io
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.modules.backup import jobs
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import MemoryStore, S3Store

from .backup_fixtures import (
    NOW,
    S3_ENDPOINT,
    World,
    enable_backup,
    login,
    minio_settings,
    pg_tool,
    use_settings,
)

pytestmark = pytest.mark.integration

needs_minio = pytest.mark.skipif(not S3_ENDPOINT, reason="cần MinIO tạm (TEST_S3_ENDPOINT) — chưa test")
FFMPEG, FFPROBE = shutil.which("ffmpeg") or "", shutil.which("ffprobe") or ""
needs_ffprobe = pytest.mark.skipif(not (FFMPEG and FFPROBE), reason="cần ffmpeg / ffprobe — chưa test")


def _needles(key_b64: str) -> list[bytes]:
    """Mọi dạng khóa có thể lọt: base64, base64url, hex thường / hoa, 32 byte thô."""
    raw = crypto.parse_key(key_b64)
    return [
        key_b64.encode(),
        base64.urlsafe_b64encode(raw),
        raw.hex().encode(),
        raw.hex().upper().encode(),
        raw,
    ]


def _leaks(blob: bytes, key_b64: str) -> list[str]:
    names = ["base64", "base64url", "hex", "HEX", "raw"]
    return [n for n, needle in zip(names, _needles(key_b64), strict=True) if needle in blob]


def _real_mp4(path: Path) -> bytes:
    subprocess.run(  # noqa: S603
        [FFMPEG, "-v", "error", "-y", "-f", "lavfi", "-i", "testsrc=duration=1:size=160x120:rate=10",
         "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(path)],
        check=True, timeout=60,
    )  # fmt: skip
    return path.read_bytes()


def _ffprobe_ok(data: bytes, tmp: Path, name: str) -> bool:
    f = tmp / name
    f.write_bytes(data)
    res = subprocess.run(  # noqa: S603
        [FFPROBE, "-v", "error", "-show_streams", str(f)], capture_output=True, timeout=60, check=False
    )
    return res.returncode == 0 and b"codec_type=video" in res.stdout


def _pg_restore(data: bytes, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(  # noqa: S603
        [pg_tool("pg_restore"), *args], input=data, capture_output=True, timeout=180, check=False
    )


def _decrypt(blob: bytes, key_b64: str) -> bytes:
    out = io.BytesIO()
    crypto.decrypt_stream(io.BytesIO(blob), out, crypto.keyring(key_b64))
    return out.getvalue()


def _boto(access: str, secret: str) -> Any:
    import boto3

    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=access, aws_secret_access_key=secret,
        region_name="us-east-1",
    )  # fmt: skip


async def _scan_db(db: AsyncSession, key_b64: str) -> dict[str, int]:
    """Đếm dòng chứa khóa (base64 / hex — bytea hiện `\\x…` hex) ở **mọi** bảng public (transaction test)."""
    raw = crypto.parse_key(key_b64)
    tables = (
        await db.scalars(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
        )
    ).all()
    hits: dict[str, int] = {}
    for t in tables:
        n = await db.scalar(
            text(f'SELECT count(*) FROM "{t}" x WHERE x::text LIKE :a OR x::text ILIKE :b'),  # noqa: S608
            {"a": f"%{key_b64}%", "b": f"%{raw.hex()}%"},
        )
        if n:
            hits[t] = int(n)
    assert {"setting", "backup_run", "backup_object", "audit_log", "notify_provider_token"} <= set(tables)
    return hits


@needs_minio
@needs_ffprobe
async def test_backup_copies_useless_without_key_and_key_never_stored(
    db: AsyncSession,
    world: World,
    backup_api: AsyncClient,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    key = crypto.generate_key()  # khóa ngẫu nhiên thật (không phải khóa test lặp ký tự)
    raw = crypto.parse_key(key)
    assert _leaks(b"x" + raw.hex().encode() + b"y" + raw, key) == ["hex", "raw"]  # bộ dò bắt được thật
    imports = tmp_path / "imports"
    (imports / "2026").mkdir(parents=True)
    (imports / "2026" / "don.csv").write_text("ma_don\n2410TSTNFR41\n")
    settings = minio_settings(
        video_root=world.settings.video_root,
        backup_tmp_dir=tmp_path / "bk",
        import_root=imports,
        backup_encryption_key=key,
        backup_pg_dump_bin=pg_tool("pg_dump"),
    )
    cloud.use_store(cloud.BACKUP, None)  # `world` gắn MemoryStore — phần này dùng MinIO thật
    store = cloud.backup_store(settings)
    assert isinstance(store, S3Store)
    # Clip CAM1 = MP4 thật (để "không phát được khi chưa giải mã" có nghĩa; đối chứng: giải mã → phát được).
    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    mp4 = _real_mp4(settings.video_root / cam1.path)
    cam1.sha256, cam1.size_bytes = hashlib.sha256(mp4).hexdigest(), len(mp4)
    await enable_backup(db, settings)

    # Màn D23: API-180 / API-182 không trả khóa (chỉ dấu vân tay).
    use_settings(backup_api, settings)
    headers, _ = await login(backup_api, db)
    status = await backup_api.get("/api/v1/backup", headers=headers)
    assert status.status_code == 200, status.text
    fp = crypto.fingerprint(crypto.parse_key(key))
    assert status.json()["key"]["fingerprint"] == fp
    confirm = await backup_api.post("/api/v1/backup/confirm-key", headers=headers, json={"fingerprint": fp})
    assert confirm.status_code == 200, confirm.text
    assert not _leaks((status.text + confirm.text).encode(), key)

    # J-20 (pg_dump thật) + J-21 / J-22 lên MinIO.
    out = await jobs.run_db(db, settings, store=store)
    assert out["status"] == "SUCCESS", out
    await jobs.enqueue_evidence(db, settings)
    up = await jobs.upload_evidence(db, settings, store=store)
    assert up.get("UPLOADED", 0) >= 3, up
    rows = {
        o.kind if o.kind in ("DB_DUMP", "IMPORTS") else f"{o.kind}:{o.clip_id or o.snapshot_id}": o
        for o in (await db.scalars(select(BackupObject).where(BackupObject.cloud_present.is_(True)))).all()
    }
    dump_key = rows["DB_DUMP"].object_key
    clip_key = rows[f"CLIP:{cam1.id}"].object_key
    five = [dump_key, rows["IMPORTS"].object_key, clip_key, rows[f"CLIP:{world.clips['CAM2'].id}"].object_key,
            rows[f"SNAPSHOT:{world.snapshot.id}"].object_key]  # fmt: skip

    # 5 đối tượng tải thẳng từ bucket (như kẻ lấy được quyền đọc kho): định dạng AICAMENC1, không chứa khóa,
    # metadata không chứa khóa.
    reader = _boto(os.environ["TEST_S3_ACCESS_KEY"], os.environ["TEST_S3_SECRET_KEY"])
    blobs: dict[str, bytes] = {}
    for k in five:
        obj = reader.get_object(Bucket=store.bucket, Key=k)
        blob = obj["Body"].read()
        blobs[k] = blob
        assert blob.startswith(crypto.MAGIC + b"\x01"), k  # định dạng AICAMENC1
        assert not _leaks(blob, key), k
        meta = repr(obj["Metadata"]).encode() + repr(store.head(k)).encode()
        assert not _leaks(meta, key), (k, obj["Metadata"])
        assert crypto.read_header(io.BytesIO(blob)).fingerprint == fp  # chỉ dấu vân tay trong header

    # Không khóa: video không phát được, dump không đọc được; tệp nhập không phải gzip.
    assert not _ffprobe_ok(blobs[clip_key], tmp_path, "enc.mp4")
    listing = _pg_restore(blobs[dump_key], "--list")
    assert listing.returncode != 0
    assert not blobs[rows["IMPORTS"].object_key].startswith(b"\x1f\x8b")
    # Sai khóa → lỗi, không ra bản rõ.
    with pytest.raises(crypto.CryptoError):
        _decrypt(blobs[clip_key], crypto.generate_key())
    # Đối chứng: có khóa → phát được / đọc được.
    plain_clip = _decrypt(blobs[clip_key], key)
    assert plain_clip == mp4
    assert _ffprobe_ok(plain_clip, tmp_path, "dec.mp4")
    plain_dump = _decrypt(blobs[dump_key], key)
    assert _pg_restore(plain_dump, "--list").returncode == 0
    # Dump DB giải mã → SQL rõ (`-f -`): không có khóa ở bất kỳ dạng nào.
    sql = _pg_restore(plain_dump, "-f", "-")
    assert sql.returncode == 0, sql.stderr[-500:]
    assert b"CREATE TABLE public.backup_object" in sql.stdout
    assert not _leaks(sql.stdout, key)
    assert not _leaks(plain_dump, key)

    # DB (trong transaction test — gồm dòng `setting`, `backup_run`, `backup_object`, audit vừa ghi).
    assert await _scan_db(db, key) == {}
    assert await db.scalar(select(audit.AuditLog).where(audit.AuditLog.action == "BACKUP_KEY_CONFIRM"))

    # Log test (stdout structlog + logging).
    captured = capsys.readouterr()
    assert not _leaks((captured.out + captured.err + caplog.text).encode(), key)

    # Khóa ứng dụng (khóa máy kho) không xóa được phiên bản / tắt versioning / đổi lifecycle (RK-28).
    from botocore.exceptions import ClientError

    app = reader
    version = app.head_object(Bucket=store.bucket, Key=dump_key)["VersionId"]
    for call in (
        lambda: app.delete_object(Bucket=store.bucket, Key=dump_key, VersionId=version),
        lambda: app.put_bucket_versioning(
            Bucket=store.bucket, VersioningConfiguration={"Status": "Suspended"}
        ),
        lambda: app.delete_bucket_lifecycle(Bucket=store.bucket),
    ):
        with pytest.raises(ClientError) as err:
            call()
        assert err.value.response["Error"]["Code"] == "AccessDenied"
    assert app.head_object(Bucket=store.bucket, Key=dump_key, VersionId=version)["ContentLength"] > 0

    # Dọn: xóa mọi phiên bản của lượt test bằng khóa root.
    root = _boto(os.environ["TEST_S3_ROOT_KEY"], os.environ["TEST_S3_ROOT_SECRET"])
    for o in rows.values():
        versions = root.list_object_versions(Bucket=store.bucket, Prefix=o.object_key)
        for v in [*versions.get("Versions", []), *versions.get("DeleteMarkers", [])]:
            root.delete_object(Bucket=store.bucket, Key=v["Key"], VersionId=v["VersionId"])


# ------------------------------------------------------------- schema guard J-23 (DB thật, không monkeypatch)


@pytest.mark.parametrize("revision", ["0005", "0008"])
async def test_j23_schema_guard_real_revision(
    db: AsyncSession, world: World, memory_store: MemoryStore, revision: str
) -> None:
    """J-23 chạy bằng image Phase 3 trên DB lệch head (`0005` = DB Phase 2 chưa nâng cấp / vừa lùi; `0008` =
    DB mới hơn image) → không xóa gì trên kho, log `backup_prune_skipped_schema_mismatch`."""
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    cam1 = world.clips["CAM1"]
    obj = await db.scalar(select(BackupObject).where(BackupObject.clip_id == cam1.id))
    assert obj is not None
    assert obj.cloud_present
    object_key = obj.object_key
    cam1.status, cam1.deleted_at = "DELETED", NOW  # nguồn bị retention xóa → J-23 sẽ xóa bản cloud nếu chạy
    audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=cam1.id,
                 data={"reason": "RETENTION"})  # fmt: skip
    await db.execute(text("UPDATE alembic_version SET version_num = :v"), {"v": revision})
    await db.flush()

    out = await jobs.prune(db, world.settings, store=memory_store)

    assert out == {"skipped_schema_mismatch": 1}
    assert memory_store.head(object_key) is not None
    # prune lùi transaction của nó (không ghi gì): alembic_version trở lại head trong DB test.
    assert await db.scalar(text("SELECT version_num FROM alembic_version")) == "0007"
