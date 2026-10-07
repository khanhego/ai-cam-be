#!/bin/sh
# Đưa stack dev về trạng thái sạch cho QA (04-test-cases §1): migrate lại từ đầu + seed TST.
# KHÔNG BAO GIỜ chạy trên production.
set -eu
cd "$(dirname "$0")/.."
# Tham số hóa qua biến môi trường (T-229, DEC-820) — mặc định = stack dev như trước:
#   AICAM_COMPOSE_PROJECT  (aicam-dev)              AICAM_COMPOSE_FILES  (docker/compose.dev.yml; nhiều file cách ":")
#   AICAM_API_URL          (http://localhost:8180)  AICAM_MEDIAMTX_API   (http://localhost:59997)
#   AICAM_RESET_PHASE3=1   thêm dọn Phase 3 (04 §1 "Dọn"): 2 bucket MinIO (mọi phiên bản), `notify:mock:*`.
# Stack QA riêng: `. docker/qa.env` trước khi chạy (project aicam-qa, api :8280).
PROJECT="${AICAM_COMPOSE_PROJECT:-aicam-dev}"
API_URL="${AICAM_API_URL:-http://localhost:8180}"
MEDIAMTX_API="${AICAM_MEDIAMTX_API:-http://localhost:59997}"
COMPOSE="docker compose -p $PROJECT"
for f in $(printf '%s' "${AICAM_COMPOSE_FILES:-docker/compose.dev.yml}" | tr ':' ' '); do
  COMPOSE="$COMPOSE -f $f"
done

# Camera cũ bị xóa khỏi DB nhưng video thô của chúng vẫn còn: dọn trước khi seed (seed tạo camera id mới mỗi lần),
# để stack dev không đầy ổ. Image MediaMTX không có shell nên dùng container tạm gắn cùng volume.
docker run --rm -v "${PROJECT}_video":/v alpine sh -c 'rm -rf /v/raw/* /v/clips/* /v/exports/* /v/snapshots/*' >/dev/null 2>&1 || true
# Xóa sạch schema rồi migrate từ đầu. Không dùng `alembic downgrade base`: downgrade 0003 giữ dữ liệu Phase 2 trong
# `phase2_archive` (T-120) để nâng cấp lại khôi phục — reset QA thì cố ý bỏ hết dữ liệu.
$COMPOSE exec -T postgres psql -U aicam -d aicam -qc 'DROP SCHEMA IF EXISTS phase2_archive CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;' >/dev/null
$COMPOSE exec -T api sh -c 'alembic upgrade head && aicam seed-demo' | tail -3
# Bỏ đếm đăng nhập sai theo IP và khay Cam 2 còn sót trong Redis.
$COMPOSE exec -T redis sh -c 'redis-cli --scan --pattern "login_fail_ip:*" | xargs -r redis-cli del >/dev/null; redis-cli --scan --pattern "tray:*" | xargs -r redis-cli del >/dev/null'
if [ "${AICAM_RESET_PHASE3:-}" = "1" ]; then
  # Tin mock + khóa / cờ chạy của job Phase 3 (sao lưu, link, thông báo) còn sót từ lượt trước.
  $COMPOSE exec -T redis sh -c 'for p in "notify:*" "backup:*" "share:*" "sync:*" "sync_returns:*"; do redis-cli --scan --pattern "$p" | xargs -r redis-cli del >/dev/null; done'
  # Bucket MinIO: xóa mọi phiên bản bằng tài khoản root (khóa ứng dụng không có quyền xóa phiên bản — DEC-501).
  $COMPOSE run --rm --no-deps -T --entrypoint sh minio-init -c \
    'mc alias set local "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null &&
     mc rm --recursive --force --versions "local/$BACKUP_BUCKET" >/dev/null 2>&1;
     mc rm --recursive --force "local/$SHARE_BUCKET" >/dev/null 2>&1; true'
fi
# Chờ camera seed (cam-<uuid> của TST Station 01) có luồng thật trên MediaMTX: ngay sau reset path mới cần vài giây,
# test chụp ảnh / live / cắt clip chạy sớm hơn sẽ chập chờn (QA G4). Tối đa 45 giây, không chặn nếu MediaMTX không có.
paths=$(API_URL="$API_URL" python3 - <<'PY' 2>/dev/null || true
import json, os, urllib.request
api = os.environ["API_URL"] + "/api/v1"
req = urllib.request.Request(f"{api}/auth/login", json.dumps({"username": "tst_admin", "password": "matkhau123",
      "client": "DASHBOARD"}).encode(), {"Content-Type": "application/json"})
token = json.load(urllib.request.urlopen(req))["access_token"]
req = urllib.request.Request(f"{api}/stations", headers={"Authorization": f"Bearer {token}"})
data = json.load(urllib.request.urlopen(req))
items = data["items"] if isinstance(data, dict) else data
print(" ".join(f"cam-{c['id']}" for s in items if s["name"] == "TST Station 01" for c in s.get("cameras", [])))
PY
)
i=0
ready=0
while [ -n "$paths" ] && [ $i -lt 45 ]; do
  ready=$(curl -sf "$MEDIAMTX_API/v3/paths/list" 2>/dev/null | PATHS="$paths" python3 -c '
import json, os, sys
want = set(os.environ["PATHS"].split())
print(sum(1 for p in json.load(sys.stdin)["items"] if p["name"] in want and p.get("ready")))' 2>/dev/null || echo 0)
  [ "${ready:-0}" -ge 2 ] && break
  i=$((i + 1)); sleep 1
done
echo "Camera seed sẵn sàng: ${ready:-0}/2 sau ${i:-0} giây"
[ "${ready:-0}" -ge 2 ] || echo "Cảnh báo: camera seed chưa sẵn sàng sau 45 giây" >&2
# --mute-cam2: E2E không cần Cam 2 đọc phiếu thật. Camera giả 2 phát vòng 60 giây nhiều phiếu → khay có phiếu khác
# đúng lúc quét đóng thì BR-06 chặn, test phụ thuộc thời điểm (QA G4). Đặt ROI vào góc khay luôn trống (NOT_SEEN).
if [ "${1:-}" = "--mute-cam2" ]; then
  $COMPOSE exec -T postgres psql -U aicam -d aicam -qc "UPDATE camera SET roi = '{\"x\":0,\"y\":0,\"w\":0.1,\"h\":0.1}' WHERE role = 'CAM2'" >/dev/null
  $COMPOSE exec -T redis redis-cli PUBLISH vision.config '{}' >/dev/null
  sleep 3  # vision nạp ROI + khử nhiễu khay trống
fi
echo "QA reset xong"
