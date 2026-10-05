#!/bin/sh
# Đưa stack dev về trạng thái sạch cho QA (04-test-cases §1): migrate lại từ đầu + seed TST.
# KHÔNG BAO GIỜ chạy trên production.
set -eu
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker/compose.dev.yml"

# Camera cũ bị xóa khỏi DB nhưng video thô của chúng vẫn còn: dọn trước khi seed (seed tạo camera id mới mỗi lần),
# để stack dev không đầy ổ. Image MediaMTX không có shell nên dùng container tạm gắn cùng volume.
docker run --rm -v aicam-dev_video:/v alpine sh -c 'rm -rf /v/raw/* /v/clips/* /v/exports/*' >/dev/null 2>&1 || true
# Xóa sạch schema rồi migrate từ đầu. Không dùng `alembic downgrade base`: downgrade 0003 từ chối khi DB đã có
# dữ liệu Phase 2 (guard DEC-301, archive `phase2_archive` ở T-120) — reset QA thì cố ý bỏ hết dữ liệu.
$COMPOSE exec -T postgres psql -U aicam -d aicam -qc 'DROP SCHEMA IF EXISTS phase2_archive CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;' >/dev/null
$COMPOSE exec -T api sh -c 'alembic upgrade head && aicam seed-demo' | tail -3
# Bỏ đếm đăng nhập sai theo IP và khay Cam 2 còn sót trong Redis.
$COMPOSE exec -T redis sh -c 'redis-cli --scan --pattern "login_fail_ip:*" | xargs -r redis-cli del >/dev/null; redis-cli --scan --pattern "tray:*" | xargs -r redis-cli del >/dev/null'
# Chờ camera seed (cam-<uuid> của TST Station 01) có luồng thật trên MediaMTX: ngay sau reset path mới cần vài giây,
# test chụp ảnh / live / cắt clip chạy sớm hơn sẽ chập chờn (QA G4). Tối đa 45 giây, không chặn nếu MediaMTX không có.
paths=$(python3 - <<'PY' 2>/dev/null || true
import json, urllib.request
api = "http://localhost:8180/api/v1"
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
  ready=$(curl -sf http://localhost:59997/v3/paths/list 2>/dev/null | PATHS="$paths" python3 -c '
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
