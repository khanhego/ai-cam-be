#!/bin/sh
# Đưa stack dev về trạng thái sạch cho QA (04-test-cases §1): migrate lại từ đầu + seed TST.
# KHÔNG BAO GIỜ chạy trên production.
set -eu
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker/compose.dev.yml"

$COMPOSE exec -T api sh -c 'alembic downgrade base && alembic upgrade head && aicam seed-demo' | tail -3
# Bỏ đếm đăng nhập sai theo IP và khay Cam 2 còn sót trong Redis.
$COMPOSE exec -T redis sh -c 'redis-cli --scan --pattern "login_fail_ip:*" | xargs -r redis-cli del >/dev/null; redis-cli --scan --pattern "tray:*" | xargs -r redis-cli del >/dev/null'
echo "QA reset xong"
