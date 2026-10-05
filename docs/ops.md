# Vận hành Hệ thống X tại kho

Stack chạy bằng Docker Compose trên một server trong LAN kho (architecture §14). File chuẩn: `docker/compose.yml`.
Lệnh trong tài liệu chạy từ thư mục `ai-cam-be/`; đặt bí danh cho gọn:

```sh
alias dc='docker compose --env-file docker/.env -f docker/compose.yml'
```

| Service | Việc | Mở ra LAN |
|---|---|---|
| `caddy` | HTTPS, FE tĩnh, chuyển `/api`, `/ws`, `/live` | 80, 443 |
| `api` | FastAPI (station, dashboard, WS) | không (qua Caddy) |
| `worker`, `worker-export`, `beat` | Celery: cắt clip, xuất, đồng bộ Shopee, retention | không |
| `vision` | Theo dõi camera, đọc mã Cam 2 | không |
| `mediamtx` | Kéo RTSP camera, ghi video 60 giây / file, live view | chỉ ICE 8189 UDP + TCP |
| `postgres`, `redis` | Dữ liệu, hàng đợi job | không |
| `migrate` | `alembic upgrade head` rồi thoát, chạy trước `api` | — |
| `backup` | `pg_dump` + file nhập CSV hằng ngày | — |

## 1. Chuẩn bị server

- Linux x86_64, Docker Engine ≥ 24 + plugin `docker compose`. Ổ dữ liệu lớn cho video (ước tính: 4 camera × 30 ngày video thô + clip 90 ngày — xem RB-7 / NFR dung lượng trong SRS).
- Giờ: chrony làm NTP cho server, camera, máy station (architecture §12). Lệch > 1 giây → cảnh báo `CLOCK_DRIFT`.
- Mạng: camera ở VLAN riêng, server thấy được RTSP camera. Tường lửa server chỉ mở cho LAN: 443/tcp, 80/tcp (chuyển hướng sang HTTPS), 8189/udp + 8189/tcp (live view).
- UPS ≥ 15 phút + NUT tắt máy an toàn.

## 2. Cài đặt lần đầu

1. Lấy mã nguồn cùng một phiên bản cho cả hai repo (`ai-cam-be`, `ai-cam-fe`) cạnh nhau trong một thư mục.
2. Build FE (cần Node 20 + pnpm), **không** chạy ở chế độ mock:
   ```sh
   cd ../ai-cam-fe && pnpm install --frozen-lockfile && pnpm build && cd ../ai-cam-be
   ```
   `dist/` có file `*.map` (source map "hidden") — Caddy trả 404 cho mọi `*.map`.
3. Cấu hình:
   ```sh
   cp docker/.env.production.example docker/.env && chmod 600 docker/.env
   ```
   Điền `SITE_ADDRESS`, `LAN_IP`, 4 secret (lệnh sinh ghi trong file). Không commit `docker/.env`.
4. Video trên ổ lớn: hoặc đặt Docker `data-root` lên ổ dữ liệu (`/etc/docker/daemon.json`), hoặc tạo `docker/compose.override.yml` gắn volume `video` vào thư mục NAS / RAID:
   ```yaml
   volumes:
     video:
       driver: local
       driver_opts: { type: none, o: bind, device: /mnt/video }
   ```
   (khi dùng override, thêm `-f docker/compose.override.yml` vào bí danh `dc`).
5. Chạy:
   ```sh
   dc up -d --build
   dc ps            # migrate: Exited (0); các service khác: running / healthy
   curl -k https://<SITE_ADDRESS>/healthz
   ```
6. Tạo Admin đầu tiên (mật khẩu ≥ 8 ký tự, hỏi qua stdin):
   ```sh
   dc exec api aicam create-admin --username admin --display-name "Chủ shop"
   ```
   `aicam seed-demo` bị chặn trên production (tài khoản TST mật khẩu chung).

## 3. Máy station và dashboard

- Caddy cấp chứng chỉ bằng CA nội bộ. Lấy chứng chỉ gốc rồi cài vào mục "Trusted Root" của từng máy (Windows: `certmgr.msc`; Chrome / Edge dùng kho của hệ điều hành):
  ```sh
  dc cp caddy:/data/caddy/pki/authorities/local/root.crt ./aicam-root.crt
  ```
  CA nội bộ giữ trong volume `caddy_data` — **không xóa** volume này, nếu không phải cài lại chứng chỉ ở mọi máy.
- Station: mở `https://<SITE_ADDRESS>/station`, đăng nhập bằng tài khoản STATION. Dashboard: `https://<SITE_ADDRESS>/admin`.

## 4. Thêm station và camera

Trên dashboard bằng tài khoản Admin:

1. **Cài đặt → Người dùng** (`/admin/settings/users`): tạo tài khoản vai trò `STATION` cho từng bàn (vd `station01`), và tài khoản SUPERVISOR / CSKH.
2. **Cài đặt → Station** (`/admin/settings/stations`) → Thêm station, gắn tài khoản STATION.
3. Trong station: đặt **Cam 1** (toàn cảnh bàn) và **Cam 2** (khay phiếu) bằng địa chỉ RTSP + tài khoản camera → **Thử kết nối** phải ra ảnh. Mật khẩu camera được mã hóa (`FERNET_KEY`).
4. Cam 2: vẽ **vùng đọc mã** (ROI) bao khay phiếu. Kiểm ở `/admin/live`: camera "Đang ghi".
5. Sau khi lưu, api tự thêm path `cam-<id>` vào MediaMTX và bắt đầu ghi. Kiểm: `dc logs --since 5m mediamtx | grep cam-`.

## 5. Kết nối Shopee hoặc nhập đơn

- Có partner key (Q11): điền `SHOPEE_*` trong `docker/.env`, `SHOPEE_ENABLED=true`, `SHOPEE_REDIRECT_URL=https://<SITE_ADDRESS>/api/v1/shops/shopee/callback` (đăng ký URL này trên Shopee Open Platform), `dc up -d`, rồi **Cài đặt → Shopee** → Kết nối.
- Chưa có: **Nhập đơn** (`/admin/imports`) bằng file CSV / xlsx theo file mẫu.

## 6. Sao lưu và khôi phục

Service `backup` chạy `pg_dump -Fc` + nén thư mục file nhập CSV mỗi ngày lúc `BACKUP_HOUR` (giờ VN), giữ `BACKUP_KEEP_DAYS` ngày, ghi vào `BACKUP_DIR` (mặc định `docker/backups/` — nên trỏ sang NAS / ổ khác).

```sh
dc exec backup /bin/sh /pg-backup.sh once     # sao lưu ngay (trước khi nâng cấp)
ls -lh docker/backups/                       # aicam-YYYYmmdd-HHMMSS.dump, imports-….tgz
dc logs backup | tail                        # dòng backup_ok / backup_failed
```

Video (`raw/`, `clips/`) **không** nằm trong bản sao lưu DB — là bằng chứng, nên đặt trên RAID / NAS có snapshot riêng. Clip có SHA-256 trong DB để đối chiếu.

Khôi phục DB (dừng ghi trước):

```sh
dc stop api vision worker worker-export beat
dc exec -T postgres sh -c 'dropdb -U aicam aicam && createdb -U aicam aicam'
dc exec -T postgres pg_restore -U aicam -d aicam --no-owner < docker/backups/aicam-<thời điểm>.dump
dc up -d
```

Khôi phục file nhập: `docker run --rm -v aicam_imports:/data/imports -v $PWD/docker/backups:/b alpine tar xzf /b/imports-<thời điểm>.tgz -C /data/imports`.

Thử khôi phục định kỳ (mỗi quý) vào một máy khác — sao lưu chưa từng khôi phục coi như chưa có.

## 7. Nâng cấp

```sh
dc exec backup /bin/sh /pg-backup.sh once          # 1. sao lưu
git -C ../ai-cam-be pull && git -C ../ai-cam-fe pull   # 2. lấy phiên bản mới (cặp BE / FE tương thích)
(cd ../ai-cam-fe && pnpm install --frozen-lockfile && pnpm build)
dc up -d --build                                    # 3. build image, migrate chạy trước api
dc ps && dc logs migrate                            # 4. migrate Exited (0), api healthy
```

Khi có image trên registry: đặt `AICAM_IMAGE=ghcr.io/<org>/ai-cam-be:<tag>` rồi `dc pull && dc up -d`.

Rollback (02 §10): về tag image / commit trước + `dc up -d`; migration mới có `downgrade`: `dc run --rm migrate alembic downgrade -1` (chạy **trước** khi về image cũ). Nặng hơn: khôi phục DB từ bản sao lưu ở bước 1.

Làm nên lúc ngoài giờ đóng gói: api khởi động lại vài giây, station tự nối lại (phiên đang mở nằm trong DB).

## 8. Xem log, giám sát

```sh
dc ps                                   # trạng thái, healthy
dc logs -f --since 10m api              # log JSON: request_id, station_id, session_id, tracking_number
dc logs --since 1h worker | grep -E 'clip_built|clip_failed'
dc logs --since 1h vision | grep camera
dc logs caddy | tail                    # access log (chữ ký URL, token WS đã che)
```

Log Docker giới hạn 20 MB × 5 file / service. Khung **Sức khỏe hệ thống** ở Cài đặt → Lưu trữ (`/admin/settings/storage`, API-81): DB, Redis, MediaMTX, ổ đĩa, từng camera, lần đồng bộ sàn. Tổng quan (`/admin`) có mục "Cần xử lý": camera mất tín hiệu, lệch giờ, clip lỗi, ổ ≥ 80 %, lỗi đồng bộ.

## 9. Dọn đĩa

- Tự động: J-02 (02:00 hằng ngày) xóa video thô quá `retention_raw_days` (30) và clip quá `retention_clip_days` (90), trừ clip đang **Giữ**. Cấu hình ở **Cài đặt → Lưu trữ** (`/admin/settings/storage`). MediaMTX không tự xóa (`recordDeleteAfter: 0s`).
- Bản xuất tự xóa sau 24 giờ; file nhập CSV gốc sau 90 ngày.
- Xem dung lượng: `docker system df -v | grep -E 'aicam_(video|pgdata)'`; trong volume: `docker run --rm -v aicam_video:/v alpine du -sh /v/raw /v/clips /v/exports`.
- Dọn image cũ sau nâng cấp: `docker image prune -f` (không dùng `docker system prune --volumes`, **không** `dc down -v`).

## 10. Sự cố thường gặp

| Hiện tượng | Kiểm tra | Xử lý |
|---|---|---|
| Camera "Mất tín hiệu" (dashboard / station) | Ping camera từ server; `dc logs --since 10m mediamtx \| grep cam-<id>`; ảnh **Thử kết nối** | Nguồn / dây mạng / PoE; mật khẩu camera đổi → nhập lại ở Cài đặt → Station. MediaMTX tự nối lại khi camera lên. Phiên trong lúc mất hình gắn cờ `VIDEO_INCOMPLETE` |
| Cam 2 không đọc mã (tray `UNAVAILABLE` / `NOT_SEEN`) | `dc logs vision`; ROI; ánh sáng | Vẽ lại ROI; phiên vẫn chạy với cờ `CAM2_UNVERIFIED`. `vision` chết tự khởi động lại |
| Ổ đầy / cảnh báo ổ ≥ 80 % | API-81; `df -h`; mục 9 | Giảm `retention_raw_days`; bỏ Giữ clip không còn cần; thêm ổ. Ổ đầy → MediaMTX ngừng ghi |
| Shopee "Hết hạn" / lỗi đồng bộ | Cài đặt → Shopee; `dc logs worker \| grep platform_` | Token hết hạn (J-12 không refresh được) → bấm **Kết nối lại**. Lỗi mạng tạm: tự thử lại; đơn vẫn nhập được bằng CSV. Quét vẫn chạy khi mất Internet (kiện "chưa xác minh", J-05 xác minh lại) |
| Clip "Không cắt được" | `dc logs worker \| grep clip_failed` | Thường do thiếu video (camera mất hình); Admin / Supervisor bấm **Thử lại** ở chi tiết kiện |
| Station không vào được / chứng chỉ lỗi | Máy station đã cài `aicam-root.crt` (mục 3)? Giờ máy station đúng? | Cài lại chứng chỉ; đồng bộ giờ |
| Live view không lên hình (ICE `failed`) | `LAN_IP` đúng IP server? 8189/udp mở? | Sửa `LAN_IP` → `dc up -d mediamtx`; mở tường lửa |
| Đăng nhập báo "Thử lại sau ít phút" cho mọi người | 30 lần sai / 5 phút theo IP | Chờ 5 phút. Nếu mọi máy bị chung một IP → kiểm `FORWARDED_ALLOW_IPS` = `CADDY_IP` (mục 11) |
| `api` không khởi động: "cần đặt secret thật" | `dc logs api` | Điền secret thật trong `docker/.env` |

## 11. Checklist bảo mật trước khi đưa vào dùng (DEC-53)

- [ ] `docker/.env` có 4 secret sinh ngẫu nhiên (không phải giá trị mẫu / `dev-only-`), `chmod 600`, không nằm trong git.
- [ ] `APP_ENV=production` (mặc định của compose): api từ chối secret dev, `seed-demo` bị chặn, không có tài khoản `tst_*`.
- [ ] `FORWARDED_ALLOW_IPS` của api = IP Caddy (`CADDY_IP`, cùng dải `AICAM_SUBNET`): `dc exec api env | grep FORWARDED`.
- [ ] `https://<SITE_ADDRESS>/assets/<file>.js.map` trả 404 (source map không lộ ra ngoài).
- [ ] Chỉ 80, 443, 8189 mở trên server (`ss -lntup`); api 8000, Postgres, Redis, API MediaMTX 9997 không truy cập được từ LAN.
- [ ] `/live/...` không có token → 401; tài khoản CSKH / STATION → 403.
- [ ] Admin đầu tiên đổi mật khẩu mạnh; tắt tài khoản không dùng (Người dùng).
- [ ] Sao lưu chạy (`backup_ok` trong log) và đã thử khôi phục một lần; `BACKUP_DIR` ở ổ khác.
- [ ] Camera ở VLAN riêng; mật khẩu camera không phải mặc định nhà sản xuất.
- [ ] Truy cập từ xa (nếu cần) chỉ qua Tailscale / tunnel, không mở cổng router.
