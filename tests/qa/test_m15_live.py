"""QA live item 03 — M15 (sao lưu cloud J-20..J-22 lên MinIO + khôi phục) trên stack thật (T-229).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m15` (tự `qa-reset.sh --mute-cam2`; Phase 3 dọn 2
bucket).
Stack: `minio` + `minio-init` (bucket sao lưu versioning, user ứng dụng theo s3-policy), `worker-backup`
(queue
`backup`), khóa sao lưu dev cố định (PRE-15). Các test chạy **theo thứ tự** trong module (trạng thái nối
tiếp).
Phủ: TC-02.50 (API), 02.53, 02.54, 02.65, J-20 (pg_dump mã hóa lên MinIO), J-21 / J-22 (clip đóng gói thật,
`all_pack_clips`), 02.70, 02.71, 02.72, 02.78, 02.80 + MEDIA_MARK_MISSING, khôi phục `aicam backup-restore` →
TC-KR.11 (409 `BACKUP_RESTORE_UNVERIFIED`) → `aicam backup-verify` (lệch → `--accept --reason`). Kho S3 thật:
**chưa test — thiếu tài
nguyên**.
"""

import time
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark

BUCKET = "aicam-dev-backup"


@pytest.fixture(scope="module", autouse=True)
def _reset() -> Iterator[None]:
    p3.reset("--mute-cam2")
    yield
    p3.compose("start", "minio")  # phòng test lỗi giữa chừng khi minio đang dừng


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with p3.api_client() as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    return p3.tokens_for(client)


def _status(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    res = client.get("/backup", headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def _issues(client: httpx.Client, tokens: dict[str, dict[str, str]], **params: Any) -> list[dict[str, Any]]:
    res = client.get("/backup/issues", params={"page_size": 100, **params}, headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    return res.json()["items"]  # type: ignore[no-any-return]


def _objects(prefix: str = "") -> list[str]:
    out = p3.mc("ls", "--recursive", f"local/{BUCKET}/{prefix}")
    assert out.returncode == 0, out.stderr[-1500:]
    return [prefix + line.split()[-1] for line in out.stdout.splitlines() if line.strip()]


def _exists(key: str) -> bool:
    return p3.mc("stat", f"local/{BUCKET}/{key}").returncode == 0


def _upload_round() -> None:
    """J-21 + J-22 chạy đồng bộ trong container worker-backup (cùng code + env, kết quả xác định)."""
    p3.job("tasks.backup_enqueue_evidence()", service="worker-backup")
    p3.job("tasks.backup_upload_evidence()", service="worker-backup")


def _clip_rows(code: str) -> list[tuple[str, str, str]]:
    """(clip_id, camera_role, path) của phiên PACK kiện `code`."""
    sql = (
        "SELECT c.id, c.camera_role, c.path FROM clip c JOIN session s ON s.id = c.session_id "
        "JOIN package p ON p.id = s.package_id WHERE p.tracking_number = '%s' ORDER BY c.camera_role"
    )
    rows = p3.psql(sql % code)
    return [tuple(r.split("|")) for r in rows.splitlines()]  # type: ignore[misc]


def _obj_of(clip_id: str) -> dict[str, str]:
    sql = "SELECT id, status, coalesce(last_error, ''), object_key FROM backup_object WHERE clip_id = '%s'"
    row = p3.psql(sql % clip_id)
    oid, status, error, key = row.split("|")
    return {"id": oid, "status": status, "error": error, "key": key}


def _video(*cmd: str) -> str:
    out = p3.compose("exec", "-T", "-u", "0", "worker", "sh", "-c", " ".join(cmd))  # clip chỉ đọc (0444)
    assert out.returncode == 0, out.stderr[-1500:]
    return out.stdout.strip()


# ---------------------------------------------------------------- chưa cấu hình / kết nối


def test_tc_02_50_not_configured_temp_api() -> None:
    """TC-02.50 (API): `S3_ENDPOINT` rỗng (API tạm cùng DB) → API-180 `configured = false`,
    `state = NOT_CONFIGURED`, `storage = null`; API-181 bật / 183 / 184 → 503 `BACKUP_NOT_CONFIGURED`."""
    with p3.temp_api({"S3_ENDPOINT": ""}) as api:
        admin = p3.login(api, "tst_admin")
        st = api.get("/backup", headers=admin).json()
        assert (st["configured"], st["state"], st["storage"]) == (False, "NOT_CONFIGURED", None)
        calls = [("PUT", "/backup/settings", {"enabled": True}), ("POST", "/backup/test", None),
                 ("POST", "/backup/run-db", None)]  # fmt: skip
        for method, path, body in calls:
            res = api.request(method, path, json=body, headers=admin)
            assert p3.err(res) == (503, "BACKUP_NOT_CONFIGURED"), (path, res.text)


def test_key_confirm_then_enable(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """PRE-15: bật trước khi xác nhận khóa → 409 `BACKUP_KEY_UNCONFIRMED`; dấu vân tay sai → 409; đúng → bật
    (`ON`); audit `BACKUP_KEY_CONFIRM`, `BACKUP_SETTINGS_UPDATE`; SUPERVISOR không đổi được (403)."""
    admin = tokens["ADMIN"]
    st = _status(client, tokens)
    assert (st["configured"], st["state"], st["enabled"]) == (True, "KEY_UNCONFIRMED", False)
    assert st["storage"] == {"endpoint_host": "minio:9000", "bucket": BUCKET}
    res = client.put("/backup/settings", json={"enabled": True}, headers=admin)
    assert p3.err(res) == (409, "BACKUP_KEY_UNCONFIRMED")
    res = client.post("/backup/confirm-key", json={"fingerprint": "0000-0000-0000-0000"}, headers=admin)
    assert res.status_code == 409, res.text
    fingerprint = st["key"]["fingerprint"]
    assert client.post("/backup/confirm-key", json={"fingerprint": fingerprint},
                       headers=tokens["SUPERVISOR"]).status_code == 403  # fmt: skip
    res = client.post("/backup/confirm-key", json={"fingerprint": fingerprint}, headers=admin)
    assert res.status_code == 200, res.text
    res = client.put("/backup/settings", json={"enabled": True}, headers=admin)
    assert res.status_code == 200, res.text
    assert (res.json()["state"], res.json()["enabled"]) == ("ON", True)
    assert len(p3.audit_rows(client, admin, "BACKUP_KEY_CONFIRM")) == 1
    assert len(p3.audit_rows(client, admin, "BACKUP_SETTINGS_UPDATE")) == 1


def test_tc_02_53_test_connection_ok(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-02.53: API-183 → 200 `{ok, elapsed_ms ≤ 10.000}`; audit `BACKUP_TEST {ok: true}`; không còn đối
    tượng
    probe (bản hiện hành) dưới `backup/_probe/`."""
    res = client.post("/backup/test", headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    assert res.json()["ok"] is True
    assert res.json()["elapsed_ms"] <= 10_000
    rows = p3.audit_rows(client, tokens["ADMIN"], "BACKUP_TEST")
    assert rows[0]["data"]["ok"] is True
    assert _objects("backup/_probe/") == []


def test_tc_02_54_connection_errors(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-02.54: sai khóa truy cập → 502 `CLOUD_AUTH_FAILED`; bucket không có → 502 `CLOUD_ERROR`; dừng minio
    →
    504 `CLOUD_UNREACHABLE` ≤ 11 giây (API tạm cho 2 ca đầu — không đổi `api` dùng chung)."""
    with p3.temp_api({"S3_SECRET_ACCESS_KEY": "sai-khoa-qa"}) as api:
        res = api.post("/backup/test", headers=p3.login(api, "tst_admin"))
        assert p3.err(res) == (502, "CLOUD_AUTH_FAILED"), res.text
        assert res.json()["error"]["message"] == "Kho lưu từ chối: sai khóa truy cập."
    with p3.temp_api({"S3_BUCKET": "aicam-qa-khong-co"}) as api:
        res = api.post("/backup/test", headers=p3.login(api, "tst_admin"))
        assert p3.err(res)[0] == 502, res.text
        assert p3.err(res)[1] in {"CLOUD_ERROR", "CLOUD_AUTH_FAILED"}, res.text
    assert p3.compose("stop", "minio").returncode == 0
    try:
        started = time.monotonic()
        res = client.post("/backup/test", headers=tokens["ADMIN"])
        elapsed = time.monotonic() - started
        assert p3.err(res) == (504, "CLOUD_UNREACHABLE"), res.text
        assert res.json()["error"]["message"] == "Không kết nối được kho lưu. Kiểm tra Internet."
        assert elapsed <= 11.5, elapsed
    finally:
        p3.compose("start", "minio")

    def healthy() -> bool:
        return client.post("/backup/test", headers=tokens["ADMIN"]).status_code == 200

    p3.wait_for(healthy, 60, 2, "minio lên lại")


# ---------------------------------------------------------------- J-20 DB


def test_tc_02_65_run_db_now_on_worker(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-02.65 + J-20: API-184 → 202 `{run_id}` + audit `BACKUP_RUN_NOW`; gọi lại ngay → 409
    `BACKUP_RUNNING`;
    worker-backup chạy pg_dump → đối tượng `backup/db/…dump.enc` + `backup/imports/…tgz.enc` trên MinIO, lịch
    sử
    `SUCCESS` có dấu vân tay khóa."""
    admin = tokens["ADMIN"]
    res = client.post("/backup/run-db", headers=admin)
    assert res.status_code == 202, res.text
    run_id = res.json()["run_id"]
    again = client.post("/backup/run-db", headers=admin)
    assert p3.err(again) == (409, "BACKUP_RUNNING"), again.text
    assert again.json()["error"]["message"] == "Đang sao lưu, thử lại sau."

    def done() -> dict[str, Any] | None:
        st = _status(client, tokens)
        runs = [h for h in st["history"] if h["id"] == run_id and h["status"] != "RUNNING"]
        return runs[0] if runs else None

    run = p3.wait_for(done, 180, 2, "J-20 xong")
    assert run["status"] == "SUCCESS", run
    assert run["size_bytes"] > 10_000
    assert run["key_fingerprint"] == _status(client, tokens)["key"]["fingerprint"]
    assert len(p3.audit_rows(client, admin, "BACKUP_RUN_NOW")) == 1
    keys = _objects("backup/")
    assert any(k.startswith("backup/db/") and k.endswith(".dump.enc") for k in keys), keys
    assert any(k.startswith("backup/imports/") and k.endswith(".tgz.enc") for k in keys), keys
    assert "backup.run_db" in p3.logs("worker-backup")


# ---------------------------------------------------------------- J-21 / J-22 bằng chứng


@pytest.fixture(scope="module")
def packed(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, list[tuple[str, str, str]]]:
    """3 phiên đóng gói thật (clip Cam 1 + Cam 2 READY): 04 (tải bình thường), 05 (lệch mã băm), 06 (thiếu
    tệp)."""
    out = {}
    for code in ("SPXTST0000004", "SPXTST0000005", "SPXTST0000006"):
        p3.pack_with_clips(client, tokens, code, hold_s=5)
        out[code] = _clip_rows(code)
        assert [r[1] for r in out[code]] == ["CAM1", "CAM2"], out[code]
    return out


def test_j21_j22_all_pack_clips_uploaded(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, list[tuple[str, str, str]]]
) -> None:
    """FR-02.18 + J-21 / J-22: bật "sao lưu mọi clip đóng gói" → J-21 xếp hàng, J-22 mã hóa + tải lên MinIO
    (`UPLOADED`, `cloud_present`); tệp 05 bị sửa trước → `HASH_MISMATCH`; tệp 06 bị xóa trước →
    `FAILED SOURCE_MISSING` (EX-K6, EX-K9)."""
    res = client.put("/backup/settings", json={"all_pack_clips": True}, headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    for _, _, path in packed["SPXTST0000005"]:
        _video(f"printf qa-sua-tep >> /data/video/{path}")
    for _, _, path in packed["SPXTST0000006"]:
        _video(f"cp /data/video/{path} /tmp/qa-$(basename {path}) && rm /data/video/{path}")
    _upload_round()
    for clip_id, _, _ in packed["SPXTST0000004"]:
        obj = _obj_of(clip_id)
        assert obj["status"] == "UPLOADED", obj
        assert _exists(obj["key"]), obj
    assert {_obj_of(c)["status"] for c, _, _ in packed["SPXTST0000005"]} == {"HASH_MISMATCH"}
    missing = [_obj_of(c) for c, _, _ in packed["SPXTST0000006"]]
    assert {(o["status"], o["error"]) for o in missing} == {("FAILED", "SOURCE_MISSING")}
    st = _status(client, tokens)["evidence"]
    assert st["uploaded"] >= 2
    assert (st["hash_mismatch"], st["source_missing"]) == (2, 2)
    issues = _issues(client, tokens)
    assert {i["status"] for i in issues} >= {"HASH_MISMATCH", "FAILED"}


def test_tc_02_72_resolve_errors(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, list[tuple[str, str, str]]]
) -> None:
    """TC-02.72: `RETRY` cho `HASH_MISMATCH` → 409 `BACKUP_ISSUE_ACTION_INVALID`; `note` "abc" → 422; id
    không có
    → 404; TC-02.80 (1): `UPLOAD_ANYWAY` cho `SOURCE_MISSING` → 409."""
    admin = tokens["ADMIN"]
    mismatch = _obj_of(packed["SPXTST0000005"][0][0])["id"]
    missing = _obj_of(packed["SPXTST0000006"][0][0])["id"]
    url = f"/backup/issues/{mismatch}/resolve"
    res = client.post(url, json={"action": "RETRY", "note": "Thử lại xem"}, headers=admin)
    assert p3.err(res) == (409, "BACKUP_ISSUE_ACTION_INVALID"), res.text
    res = client.post(url, json={"action": "IGNORE", "note": "abc"}, headers=admin)
    assert res.status_code == 422, res.text
    assert res.json()["error"]["details"]["fields"]["note"] == "Nhập lý do (5–500 ký tự)."
    res = client.post(f"/backup/issues/{p3.psql('SELECT gen_random_uuid()')}/resolve",
                      json={"action": "IGNORE", "note": "Không có tệp này"}, headers=admin)  # fmt: skip
    assert res.status_code == 404, res.text
    body = {"action": "UPLOAD_ANYWAY", "note": "Tải đại đi"}
    res = client.post(f"/backup/issues/{missing}/resolve", json=body, headers=admin)
    assert p3.err(res) == (409, "BACKUP_ISSUE_ACTION_INVALID"), res.text
    assert client.post(url, json={"action": "IGNORE", "note": "Không quyền"},
                       headers=tokens["SUPERVISOR"]).status_code == 403  # fmt: skip


def test_tc_02_70_upload_anyway(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, list[tuple[str, str, str]]]
) -> None:
    """TC-02.70: `UPLOAD_ANYWAY` → 200 `PENDING` (`hash_override`), audit `BACKUP_ISSUE_RESOLVE`; J-22 →
    `UPLOADED`; metadata đối tượng `sha256` = băm thực tế, `sha256-expected`,
    `integrity=MISMATCH_ACCEPTED`."""
    admin = tokens["ADMIN"]
    clip_id = packed["SPXTST0000005"][0][0]
    obj = _obj_of(clip_id)
    body = {"action": "UPLOAD_ANYWAY", "note": "Tệp do IT sửa, giữ bản hiện có"}
    res = client.post(f"/backup/issues/{obj['id']}/resolve", json=body, headers=admin)
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "PENDING"
    assert p3.psql(f"SELECT hash_override FROM backup_object WHERE id = '{obj['id']}'") == "t"  # noqa: S608
    p3.job("tasks.backup_upload_evidence()", service="worker-backup")
    after = _obj_of(clip_id)
    assert after["status"] == "UPLOADED", after
    stat = p3.mc("stat", f"local/{BUCKET}/{after['key']}")
    assert stat.returncode == 0, stat.stderr
    meta = stat.stdout.lower()
    assert "mismatch_accepted" in meta, stat.stdout
    assert "sha256-expected" in meta, stat.stdout
    expected = p3.psql(f"SELECT sha256 FROM clip WHERE id = '{clip_id}'")  # noqa: S608
    assert expected in stat.stdout
    rows = p3.audit_rows(client, admin, "BACKUP_ISSUE_RESOLVE")
    assert rows[0]["data"].get("action") == "UPLOAD_ANYWAY"


def test_tc_02_71_ignore_mismatch(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, list[tuple[str, str, str]]]
) -> None:
    """TC-02.71: `IGNORE` → `IGNORED` (cuối), không tính `pending`; API-185 mặc định không còn,
    `include_resolved=true` có `resolution`; xử lý lại → 409 `BACKUP_ISSUE_RESOLVED` (TC-02.72 ý 2)."""
    admin = tokens["ADMIN"]
    obj = _obj_of(packed["SPXTST0000005"][1][0])
    url = f"/backup/issues/{obj['id']}/resolve"
    res = client.post(url, json={"action": "IGNORE", "note": "Tệp lỗi, bỏ qua"}, headers=admin)
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "IGNORED"
    assert obj["id"] not in {i["object_id"] for i in _issues(client, tokens)}
    resolved = {i["object_id"]: i for i in _issues(client, tokens, include_resolved="true")}
    assert resolved[obj["id"]]["resolution"]["action"] == "IGNORE"
    res = client.post(url, json={"action": "IGNORE", "note": "Bỏ qua lần nữa"}, headers=admin)
    assert p3.err(res) == (409, "BACKUP_ISSUE_RESOLVED"), res.text
    assert _status(client, tokens)["evidence"]["hash_mismatch"] == 0


def test_tc_02_78_80_source_missing(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, list[tuple[str, str, str]]]
) -> None:
    """TC-02.78: IT chép lại tệp → `RETRY` → `attempts = 0`, đến hạn ngay → J-22 `UPLOADED`. TC-02.80: tệp có
    lại
    → `IGNORE` 409 "Tệp đã có lại tại kho — bấm Thử lại ngay."; tệp vẫn mất → `IGNORE` → `IGNORED`, clip
    `MISSING` ngay, audit `MEDIA_MARK_MISSING {cause: BACKUP_IGNORE}`."""
    admin = tokens["ADMIN"]
    (cam1_id, _, cam1_path), (cam2_id, _, _) = packed["SPXTST0000006"]
    _video(f"cp /tmp/qa-$(basename {cam1_path}) /data/video/{cam1_path}")
    obj = _obj_of(cam1_id)
    url = f"/backup/issues/{obj['id']}/resolve"
    res = client.post(url, json={"action": "IGNORE", "note": "Bỏ qua tệp này"}, headers=admin)
    assert p3.err(res) == (409, "BACKUP_ISSUE_ACTION_INVALID"), res.text
    assert res.json()["error"]["message"] == "Tệp đã có lại tại kho — bấm Thử lại ngay."
    res = client.post(url, json={"action": "RETRY", "note": "IT đã chép lại tệp"}, headers=admin)
    assert res.status_code == 200, res.text
    row = p3.psql(f"SELECT attempts, next_attempt_at <= now() FROM backup_object WHERE id = '{obj['id']}'")  # noqa: S608
    assert row == "0|t", row
    p3.job("tasks.backup_upload_evidence()", service="worker-backup")
    assert _obj_of(cam1_id)["status"] == "UPLOADED"
    gone = _obj_of(cam2_id)
    res = client.post(f"/backup/issues/{gone['id']}/resolve",
                      json={"action": "IGNORE", "note": "Tệp mất hẳn do hỏng ổ"}, headers=admin)  # fmt: skip
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "IGNORED"
    assert p3.psql(f"SELECT status FROM clip WHERE id = '{cam2_id}'") == "MISSING"  # noqa: S608
    marks = p3.audit_rows(client, admin, "MEDIA_MARK_MISSING")
    assert any(r["data"].get("cause") == "BACKUP_IGNORE" for r in marks), marks


# ---------------------------------------------------------------- khôi phục (dòng lệnh)


def test_restore_then_kr_11_then_verify(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """AC-50 trên stack: J-20 mới → xóa sạch schema DB → `aicam backup-restore` (latest, giải mã bằng khóa
    hiện
    tại) → số dòng như trước; `RESTORE_PENDING` → TC-KR.11: API-181 bật / API-184 → 409
    `BACKUP_RESTORE_UNVERIFIED`; `aicam backup-verify` → mã 0, audit `BACKUP_RESTORE_VERIFIED`, hết chờ
    kiểm."""
    admin = tokens["ADMIN"]
    res = client.post("/backup/run-db", headers=admin)
    assert res.status_code == 202, res.text
    run_id = res.json()["run_id"]

    def ok() -> bool:
        return any(h["id"] == run_id and h["status"] == "SUCCESS" for h in _status(client, tokens)["history"])

    p3.wait_for(ok, 180, 2, "J-20 trước khôi phục")
    count_sql = (
        "SELECT (SELECT count(*) FROM package) || ',' || (SELECT count(*) FROM \"order\") || ',' || "
        "(SELECT count(*) FROM shop) || ',' || (SELECT count(*) FROM return_case) || ',' || "
        "(SELECT count(*) FROM clip) || ',' || (SELECT count(*) FROM \"user\")"
    )
    before = p3.psql(count_sql)
    # Dừng worker / beat (như runbook ops §6.2) để không ghi vào DB đang khôi phục.
    workers = [
        "beat",
        "worker",
        "worker-sync",
        "worker-sync-long",
        "worker-notify",
        "worker-export",
        "vision",
    ]
    p3.compose("stop", *workers)
    try:
        p3.psql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        out = p3.compose("exec", "-T", "api", "aicam", "backup-restore", timeout=900)
        assert out.returncode == 0, out.stdout[-3000:] + out.stderr[-3000:]
        assert p3.psql(count_sql) == before
        assert p3.psql("SELECT version_num FROM alembic_version") == "0007"
    finally:
        p3.compose("start", *workers)
    tokens = p3.tokens_for(client)  # phiên đăng nhập mới (bảng phiên vừa khôi phục)
    admin = tokens["ADMIN"]
    st = _status(client, tokens)
    assert st["state"] == "RESTORE_PENDING", st["state"]
    res = client.put("/backup/settings", json={"enabled": True}, headers=admin)
    assert p3.err(res) == (409, "BACKUP_RESTORE_UNVERIFIED"), res.text
    assert p3.err(client.post("/backup/run-db", headers=admin)) == (409, "BACKUP_RESTORE_UNVERIFIED")
    # Clip Cam 2 kiện 05 bị sửa tệp và đã "Bỏ qua" ở TC-02.71 → verify báo lệch (mã 1, không gỡ cờ) → IT chấp
    # nhận bản hiện có bằng `--accept <id> --reason` (ops §6.2 "Lối ra") → đạt (mã 0).
    out = p3.compose("exec", "-T", "api", "aicam", "backup-verify", timeout=900)
    assert out.returncode == 1, out.stdout[-3000:] + out.stderr[-3000:]
    assert "lệch 1" in out.stdout
    assert _status(client, tokens)["state"] == "RESTORE_PENDING"
    tampered = p3.psql(
        "SELECT c.id FROM clip c JOIN session s ON s.id = c.session_id JOIN package p ON p.id = s.package_id "
        "WHERE p.tracking_number = 'SPXTST0000005' AND c.camera_role = 'CAM2'"
    )
    accept = ["backup-verify", "--accept", tampered, "--reason", "Tệp do IT sửa, giữ bản hiện có"]
    out = p3.compose("exec", "-T", "api", "aicam", *accept, timeout=900)
    assert out.returncode == 0, out.stdout[-3000:] + out.stderr[-3000:]
    assert _status(client, tokens)["state"] != "RESTORE_PENDING"
    assert len(p3.audit_rows(client, admin, "BACKUP_RESTORE_VERIFIED")) == 1
    accepted = p3.audit_rows(client, admin, "BACKUP_VERIFY_ACCEPT")
    assert len(accepted) == 1
    assert accepted[0]["data"].get("os_user") is not None
