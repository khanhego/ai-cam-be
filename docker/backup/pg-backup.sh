#!/bin/sh
# Sao lưu hằng ngày (02 §10 rollback "DB khôi phục từ pg_dump hằng ngày"): pg_dump -Fc + file nhập CSV gốc.
# Chạy trong service `backup` của compose.yml. `pg-backup.sh once` = chạy 1 lần rồi thoát (sao lưu trước nâng cấp),
# mã thoát ≠ 0 nếu lỗi.
# Biến: PGHOST, PGUSER, PGPASSWORD, PGDATABASE, BACKUP_HOUR (giờ VN, mặc định 01), BACKUP_KEEP_DAYS (mặc định 14:
# `find -mtime +14` xóa bản cũ hơn ~15 ngày).
#
# G3-N3: `if run; then` tắt `set -e` bên trong `run` → mỗi bước phải tự kiểm (`|| fail`). Bản mới phải đọc được
# (`pg_restore -l`) mới đổi tên `.part` → `.dump`; chỉ dọn bản cũ khi lần này thành công (lỗi liên tiếp không
# làm mất bản tốt). `umask 077`: bản sao lưu chứa token sàn đã mã hóa, mật khẩu băm — chỉ chủ đọc được.
set -eu
umask 077
HOUR="${BACKUP_HOUR:-01}"
KEEP="${BACKUP_KEEP_DAYS:-14}"
OUT="${BACKUP_OUT:-/backups}"

log() { echo "$(date -Iseconds) $*"; }

run() {
  stamp=$(date +%Y%m%d-%H%M%S)
  dump="$OUT/aicam-$stamp.dump"
  tgz="$OUT/imports-$stamp.tgz"
  fail() { rm -f "$dump.part" "$tgz.part"; log "backup_failed step=$1" >&2; return 1; }

  pg_dump -Fc -f "$dump.part" || { fail pg_dump; return 1; }
  [ -s "$dump.part" ] || { fail empty_dump; return 1; }
  pg_restore -l "$dump.part" >/dev/null || { fail verify; return 1; }
  if [ -d /imports ]; then
    tar czf "$tgz.part" -C /imports . || { fail imports_tar; return 1; }
    mv "$tgz.part" "$tgz" || { fail imports_mv; return 1; }
  fi
  mv "$dump.part" "$dump" || { fail dump_mv; return 1; }
  # Chỉ tới đây (bản mới đã kiểm) mới dọn bản cũ.
  find "$OUT" -maxdepth 1 \( -name 'aicam-*.dump' -o -name 'imports-*.tgz' \) -mtime +"$KEEP" -delete \
    || log "backup_prune_failed" >&2
  log "backup_ok $(basename "$dump")"
}

if [ "${1:-}" = "once" ]; then
  run || exit 1
  exit 0
fi

last=""
while true; do
  today=$(date +%Y%m%d)
  if [ "$(date +%H)" = "$HOUR" ] && [ "$last" != "$today" ]; then
    if run; then last="$today"; fi  # lỗi: thử lại sau 5 phút trong cùng giờ BACKUP_HOUR
  fi
  sleep 300
done
