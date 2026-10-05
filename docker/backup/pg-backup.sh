#!/bin/sh
# Sao lưu hằng ngày (02 §10 rollback "DB khôi phục từ pg_dump hằng ngày"): pg_dump -Fc + file nhập CSV gốc.
# Chạy trong service `backup` của compose.yml. `pg-backup.sh once` = chạy 1 lần rồi thoát (sao lưu trước nâng cấp).
# Biến: PGHOST, PGUSER, PGPASSWORD, PGDATABASE, BACKUP_HOUR (giờ VN, mặc định 01), BACKUP_KEEP_DAYS (mặc định 14).
set -eu
HOUR="${BACKUP_HOUR:-01}"
KEEP="${BACKUP_KEEP_DAYS:-14}"
OUT=/backups

run() {
  stamp=$(date +%Y%m%d-%H%M%S)
  pg_dump -Fc -f "$OUT/aicam-$stamp.dump.part"
  mv "$OUT/aicam-$stamp.dump.part" "$OUT/aicam-$stamp.dump"
  if [ -d /imports ]; then
    tar czf "$OUT/imports-$stamp.tgz.part" -C /imports . && mv "$OUT/imports-$stamp.tgz.part" "$OUT/imports-$stamp.tgz"
  fi
  find "$OUT" -maxdepth 1 \( -name 'aicam-*.dump' -o -name 'imports-*.tgz' \) -mtime +"$KEEP" -delete
  echo "$(date -Iseconds) backup_ok aicam-$stamp.dump"
}

if [ "${1:-}" = "once" ]; then
  run
  exit 0
fi

last=""
while true; do
  today=$(date +%Y%m%d)
  if [ "$(date +%H)" = "$HOUR" ] && [ "$last" != "$today" ]; then
    if run; then last="$today"; else echo "$(date -Iseconds) backup_failed" >&2; fi
  fi
  sleep 300
done
