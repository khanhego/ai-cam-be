#!/usr/bin/env bash
# minio-init (02a §7.2, ADR-010, DEC-501): tạo 2 bucket + user ứng dụng. Chạy lại được (idempotent).
#  - bucket sao lưu  $BACKUP_BUCKET: versioning + lifecycle xóa phiên bản cũ sau 7 ngày + dọn delete marker hết hạn
#  - bucket link     $SHARE_BUCKET : không versioning; lifecycle xóa `share/` > 8 ngày (lưới an toàn)
#  - user $APP_KEY gắn chính sách theo docs/s3-policy.example.json (không DeleteObjectVersion / PutBucketVersioning
#    / PutLifecycleConfiguration ở bucket sao lưu) — máy kho bị chiếm quyền không xóa vĩnh viễn được bản sao (RK-28)
set -euo pipefail
: "${MINIO_URL:=http://minio:9000}"
: "${BACKUP_BUCKET:?}" "${SHARE_BUCKET:?}" "${APP_KEY:?}" "${APP_SECRET:?}"
: "${MINIO_ROOT_USER:?}" "${MINIO_ROOT_PASSWORD:?}"

for i in $(seq 1 30); do
  mc alias set local "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && break
  sleep 1
done
mc alias set local "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

mc mb --ignore-existing "local/$BACKUP_BUCKET"
mc mb --ignore-existing "local/$SHARE_BUCKET"
mc version enable "local/$BACKUP_BUCKET"
# Image không có grep: so chuỗi bằng bash (chạy lại không thêm quy tắc trùng).
rules=$(mc ilm rule ls "local/$BACKUP_BUCKET" 2>/dev/null || true)
if [[ "$rules" != *NoncurrentVersionExpiration* ]]; then
  mc ilm rule add --noncurrent-expire-days 7 --expire-delete-marker "local/$BACKUP_BUCKET"
fi
rules=$(mc ilm rule ls "local/$SHARE_BUCKET" 2>/dev/null || true)
if [[ "$rules" != *share/* ]]; then
  mc ilm rule add --prefix "share/" --expire-days 8 "local/$SHARE_BUCKET"
fi

policy=$(cat /policy/s3-policy.example.json)
policy=${policy//aicam-backup/$BACKUP_BUCKET}
policy=${policy//aicam-share/$SHARE_BUCKET}
printf '%s' "$policy" > /tmp/aicam-app-policy.json
mc admin policy create local aicam-app /tmp/aicam-app-policy.json
mc admin user add local "$APP_KEY" "$APP_SECRET"
mc admin policy attach local aicam-app --user "$APP_KEY" 2>/dev/null || true
echo "minio-init: $BACKUP_BUCKET (versioning) + $SHARE_BUCKET, user $APP_KEY — xong"
