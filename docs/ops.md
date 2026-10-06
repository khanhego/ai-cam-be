# Vận hành Hệ thống X tại kho

Stack chạy bằng Docker Compose trên một server trong LAN kho (architecture §14). File chuẩn: `docker/compose.yml`.
Lệnh trong tài liệu chạy từ thư mục `ai-cam-be/`; đặt bí danh cho gọn:

```sh
alias dc='docker compose --env-file docker/.env -f docker/compose.yml'
```

| Service | Việc | Mở ra LAN |
|---|---|---|
| `caddy` | HTTPS, FE tĩnh, chuyển `/api`, `/ws`, `/live` | 80, 443 |
| `api` | FastAPI (station, dashboard, WS) — **một tiến trình** (mục 8) | không (qua Caddy) |
| `worker` | Celery queue `default`, `video`: cắt clip (J-01), retention, dọn dẹp | không |
| `worker-sync` | Celery queue `sync`: đồng bộ Shopee (J-04/05/06/12), 2 tiến trình — tách để Shopee chậm không làm trễ cắt clip (NFR-03) | không |
| `worker-export`, `beat` | Celery: encode bản xuất (1 job / lần); lịch job | không |
| `vision` | Theo dõi camera, đọc mã Cam 2 | không |
| `mediamtx` | Kéo RTSP camera, ghi video 60 giây / file, live view | chỉ ICE 8189 UDP + TCP |
| `postgres`, `redis` | Dữ liệu, hàng đợi job | không |
| `migrate` | `alembic upgrade head` rồi thoát, chạy trước `api` | — |
| `backup` | `pg_dump` + file nhập CSV hằng ngày | — |

## 1. Chuẩn bị server

- Linux x86_64, Docker Engine ≥ 24 + plugin `docker compose`. Ổ dữ liệu lớn cho video (ước tính: 4 camera × 30 ngày video thô + clip 90 ngày — xem RB-7 / NFR dung lượng trong SRS).
- Giờ: chrony làm NTP cho server, camera, máy station (architecture §12). Lệch > 1 giây → cảnh báo `CLOCK_DRIFT`.
- Mạng: camera ở VLAN riêng, server thấy được RTSP camera. Tường lửa server chỉ mở cho LAN: 443/tcp, 80/tcp (chuyển hướng sang HTTPS), 8189/udp + 8189/tcp (live view).
- **Docker bỏ qua UFW / firewalld** cho cổng đã publish (Docker tự chèn rule iptables). Hai cách giới hạn cổng 80/443/8189 chỉ cho LAN kho (G3-N10):
  - Bind cổng vào IP LAN thay vì mọi giao diện — trong `docker/.env`: `HTTP_PORT=<LAN_IP>:80`, `HTTPS_PORT=<LAN_IP>:443` (và sửa `ports` của mediamtx trong `compose.override.yml` thành `<LAN_IP>:8189:8189/udp`, `<LAN_IP>:8189:8189`).
  - Hoặc rule chuỗi `DOCKER-USER` (giữ qua reboot bằng `iptables-persistent`), ví dụ LAN `192.168.10.0/24`, card mạng `eth0`:
    ```sh
    iptables -I DOCKER-USER -i eth0 ! -s 192.168.10.0/24 -p tcp -m multiport --dports 80,443,8189 -j DROP
    iptables -I DOCKER-USER -i eth0 ! -s 192.168.10.0/24 -p udp --dport 8189 -j DROP
    ```
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

   **Quyền thư mục video / NAS (G3-P2-7):** mọi tiến trình ghi video chạy uid **10001** (mediamtx, worker). `volume-init` chỉ chown thư mục gốc + cấp 1–2 chưa đúng chủ (không `chown -R` toàn bộ video mỗi lần `up` — tránh hàng phút không ghi hình) và **không chặn** mediamtx khi chown lỗi. Với NFS `root_squash` (root trong container không chown được), chuẩn bị trước trên NAS / server:
   ```sh
   mkdir -p /mnt/video/raw /mnt/video/clips /mnt/video/exports && chown -R 10001:10001 /mnt/video
   ```
   (hoặc export NFS `all_squash,anonuid=10001,anongid=10001`). Log `CẢNH BÁO volume-init` trong `dc logs volume-init` = quyền chưa đúng → mediamtx / worker sẽ báo lỗi ghi.
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

Service `backup` chạy `pg_dump -Fc` + nén thư mục file nhập CSV mỗi ngày lúc `BACKUP_HOUR` (giờ VN), ghi vào `BACKUP_DIR` (mặc định `docker/backups/` — nên trỏ sang NAS / ổ khác). Bản mới phải đọc được (`pg_restore -l`) mới được giữ; bản cũ chỉ bị dọn khi lần sao lưu này thành công (`find -mtime +BACKUP_KEEP_DAYS` — với 14 là bản cũ hơn khoảng **15 ngày**). File tạo với quyền `600` (G3-N3).

**Sao lưu `docker/.env` riêng, ra ngoài server (off-site, két / trình quản lý mật khẩu).** `FERNET_KEY` mã hóa token Shopee và mật khẩu camera trong DB: khôi phục DB với `.env` khác → token / mật khẩu camera không giải mã được (shop chuyển "Hết hạn" — `CREDENTIALS_UNREADABLE`, phải Kết nối lại; camera phải nhập lại mật khẩu). `JWT_SECRET` / `MEDIA_SIGNING_KEY` khác chỉ làm mọi người đăng nhập lại.

```sh
dc exec backup /bin/sh /pg-backup.sh once     # sao lưu ngay (trước khi nâng cấp)
ls -lh docker/backups/                       # aicam-YYYYmmdd-HHMMSS.dump, imports-….tgz
dc logs backup | tail                        # dòng backup_ok / backup_failed
```

Video (`raw/`, `clips/`) **không** nằm trong bản sao lưu DB — là bằng chứng, nên đặt trên RAID / NAS có snapshot riêng. Clip có SHA-256 trong DB để đối chiếu.

Khôi phục DB (dừng ghi trước):

Dùng đúng `docker/.env` của lúc sao lưu (cùng `FERNET_KEY`). Dừng cả `backup` để nó không `pg_dump` giữa chừng khi DB đang trống:

```sh
dc stop api vision worker worker-sync worker-export beat backup
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

Rollback (02 §10): về tag image / commit trước + `dc up -d`; migration mới có `downgrade`: `dc run --rm migrate alembic downgrade <revision>` bằng **image mới** (chạy **trước** khi về image cũ). Nặng hơn: khôi phục DB từ bản sao lưu ở bước 1. Lùi từ Phase 2 về Phase 1: theo mục 7.1, không dùng `downgrade -1`.

Làm nên lúc ngoài giờ đóng gói: api khởi động lại vài giây, station tự nối lại (phiên đang mở nằm trong DB).

### 7.1 Phase 2 (hàng hoàn, đối soát, khiếu nại): nâng cấp và lùi về Phase 1

Phase 2 thêm 2 migration: **0003** (bảng / cột mới, chỉ thêm) và **0004** (clip đang "Giữ" → hồ sơ khiếu nại
"Chuyển từ cờ giữ" — `LEGACY_HOLD`). Từ Phase 2, clip được giữ theo **hồ sơ** (ADR-009) thay cho cờ giữ từng clip.

**Nâng cấp** (như mục 7, thêm):

1. Sao lưu DB (`pg-backup.sh once`) **và** chụp snapshot volume video (NAS / RAID) — bằng chứng không nằm trong `pg_dump`.
2. Sàn giữ clip: `RETENTION_CLIP_MIN_DAYS` (mặc định 60, đặt trong `docker/.env`). 0003 nâng `retention_clip_days` dưới sàn lên sàn + audit `RETENTION_RAISED_TO_MINIMUM`.
3. `dc up -d --build` → kiểm `dc logs migrate`: dòng `0004: N clip giữ → M hồ sơ LEGACY_HOLD` và `held_before=… protected_after=…` (tập sau ≥ tập trước — giữ theo phiên, cả Cam 1 + Cam 2). `migrate` lỗi → không có gì thay đổi (một transaction), `api` không lên: giữ nguyên image cũ, gửi log cho BE.
4. Admin xem **Hồ sơ khiếu nại** lọc nguồn "Chuyển từ cờ giữ" (hạn = lúc nâng cấp + 30 ngày) — đóng hồ sơ không còn cần.

**Lùi về Phase 1** — chỉ khi không sửa tiến được (ưu tiên forward-fix). Thứ tự bắt buộc: **downgrade bằng image mới
trước, đổi image sau**. Downgrade không xóa dữ liệu Phase 2: chép sang schema `phase2_archive` (hồ sơ hàng hoàn,
phiên nhận hàng hoàn + clip / sự kiện / bản xuất / yêu cầu duyệt của nó, kiện tạm `TAM-`, ảnh, hồ sơ khiếu nại +
bằng chứng + ghi chú + gói bằng chứng, cảnh báo đối soát, lịch sử trạng thái hoàn, cột Phase 2 của station / cài đặt
/ phiên / kiện, số thứ tự mã HH- / KN- / TAM-). Kiện đang ở trạng thái hoàn hiện lại trạng thái cuối trước đó (không
có → "Đã giao"). Clip đang được bảo vệ theo hồ sơ được đặt cờ **Giữ** để J-02 của image cũ không xóa. File video,
ảnh, gói zip **giữ nguyên trên đĩa**.

```sh
dc exec backup /bin/sh /pg-backup.sh once                  # 1. sao lưu (bắt buộc) + snapshot volume video
# 2. Hoàn tất / hủy mọi phiên nhận hàng hoàn đang mở ở station (downgrade từ chối nếu còn — không đổi gì).
#    Phiên hoàn có clip "Không cắt được": bấm Thử lại (API-46) trước — image cũ không giữ video thô cho chúng.
dc stop api vision worker worker-sync worker-export beat   # 3. dừng dịch vụ (không để job ghi giữa chừng)
dc run --rm migrate alembic downgrade 0002                 # 4. bằng IMAGE MỚI (0004 rồi 0003, một transaction)
dc run --rm migrate alembic current                        #    phải in 0002
# 5. Log bước 4: "0004 downgrade: đặt held cho N clip…", "0003 downgrade: chép sang phase2_archive {…số dòng…}"
AICAM_IMAGE=<tag Phase 1> dc up -d                         # 6. rồi mới đổi image BE (và build FE Phase 1)
```

- **Không** xóa schema `phase2_archive`, **không** dọn `clips/`, `snapshots/`, `exports/pack-*` bằng tay, **không** bỏ
  "Giữ" hàng loạt khi đang chạy Phase 1 (bỏ giữ → J-02 cũ xóa được clip bằng chứng).
- Image cũ **không chạy được** trên DB đã nâng cấp: `migrate` của nó báo `Can't locate revision identified by '0004'`
  (thoát ≠ 0) nên `api` / worker không khởi động — đúng ý (chặn J-02 cũ). Gặp lỗi này: làm lại bước 3–4 bằng image
  mới. Không bỏ qua bằng `docker start` / `dc start api`.
- Lỗi ở bước 4 → cả lệnh lùi lại, DB giữ nguyên Phase 2 (chạy lại sau khi xử lý nguyên nhân trong log).

**Nâng cấp lại lên Phase 2** sau khi đã lùi: như phần Nâng cấp. 0003 khôi phục mọi thứ từ `phase2_archive` (kiện mà
image cũ đã đổi trạng thái thì giữ trạng thái mới — log `kiện hoàn đã đổi trạng thái`, đối soát J-14 sẽ báo lệch nếu
có); 0004 trả cờ giữ do downgrade đặt (clip Admin đã giữ trước khi lùi vẫn giữ), khôi phục hồ sơ "Chuyển từ cờ giữ"
cũ, chỉ tạo hồ sơ mới cho clip được giữ thêm trong lúc chạy Phase 1, rồi drop `phase2_archive`. Mã HH- / KN- / TAM-
mới không trùng mã cũ. Kiểm log migrate: `0003: khôi phục từ phase2_archive {…}` (số dòng = lúc chép) và
`0004: khôi phục …`.

## 8. Xem log, giám sát

```sh
dc ps                                   # trạng thái, healthy
dc logs -f --since 10m api              # log JSON: request_id, station_id, session_id, tracking_number
dc logs --since 1h worker | grep -E 'clip_built|clip_failed'
dc logs --since 1h worker-sync | grep -E 'platform_|shopee_call'
dc logs --since 1h vision | grep camera
dc logs caddy | tail                    # access log (chữ ký URL, token WS đã che)
```

Log không chứa bí mật trong URL (G3-F3, G3-N1): uvicorn tắt access log (`--no-access-log`, Caddy đã ghi access log có che); mọi log stdlib (uvicorn, httpx, celery) qua bộ che query `token`, `sig`, `exp`, `uid`, `code`, `state`, `access_token`, `refresh_token`, `sign`; `httpx` / `httpcore` chỉ ghi từ WARNING. Giá trị bị thay bằng `[token đã che]`, `[sig đã che]`… Caddy che `sig`, `token` ở cả access log lẫn log lỗi / cảnh báo (logger `default` — vd `aborting with incomplete response` khi trình duyệt huỷ tải video ghi nguyên `request.uri`; G5). Kiểm nhanh: `dc logs api worker-sync caddy | grep -E 'token=|sig=|access_token=' | grep -v REDACTED` phải rỗng.

**`api` chạy một tiến trình (G3-F12).** Bus Redis `tray.changed` / `camera.health` (vision → api) được mọi tiến trình api nghe và xử lý: chạy nhiều tiến trình (`uvicorn --workers N`, `dc up --scale api=N`) làm cờ phiên / WS bị xử lý lặp. Một tiến trình đủ cho NFR-01 (đo T-19); muốn scale phải tách listener ra tiến trình riêng trước.

Log Docker giới hạn 20 MB × 5 file / service. Khung **Sức khỏe hệ thống** ở Cài đặt → Lưu trữ (`/admin/settings/storage`, API-81): DB, Redis, MediaMTX, ổ đĩa, từng camera, lần đồng bộ sàn. Tổng quan (`/admin`) có mục "Cần xử lý": camera mất tín hiệu, lệch giờ, clip lỗi, ổ ≥ 80 %, lỗi đồng bộ.

## 9. Dọn đĩa

- Tự động: J-02 (02:00 hằng ngày) xóa video thô quá `retention_raw_days` (30) và clip quá `retention_clip_days` (90), trừ clip đang **Giữ**. Cấu hình ở **Cài đặt → Lưu trữ** (`/admin/settings/storage`). MediaMTX không tự xóa (`recordDeleteAfter: 0s`).
- Bản xuất tự xóa sau 24 giờ; file nhập CSV gốc sau 90 ngày.
- Xem dung lượng: `docker system df -v | grep -E 'aicam_(video|pgdata)'`; trong volume: `docker run --rm -v aicam_video:/v alpine du -sh /v/raw /v/clips /v/exports`.
- Dọn image cũ sau nâng cấp: `docker image prune -f` (không dùng `docker system prune --volumes`, **không** `dc down -v`).
- J-02 **giữ** video thô của phiên có clip "Không cắt được" / đang cắt (FAILED / PENDING) để **Thử lại** còn dùng được sau 30 ngày (G3-F7); bấm Thử lại thành công thì lượt J-02 sau mới xóa. Clip quá hạn: DB chuyển "Đã xóa" trước, xóa file sau — xóa file lỗi thì lượt sau dọn tiếp (`retention_clip_unlink_failed` trong log).

## 10. Sự cố thường gặp

| Hiện tượng | Kiểm tra | Xử lý |
|---|---|---|
| Camera "Mất tín hiệu" (dashboard / station) | Ping camera từ server; `dc logs --since 10m mediamtx \| grep cam-<id>`; ảnh **Thử kết nối** | Nguồn / dây mạng / PoE; mật khẩu camera đổi → nhập lại ở Cài đặt → Station. MediaMTX tự nối lại khi camera lên. Phiên trong lúc mất hình gắn cờ `VIDEO_INCOMPLETE` |
| Cam 2 không đọc mã (tray `UNAVAILABLE` / `NOT_SEEN`) | `dc logs vision`; ROI; ánh sáng | Vẽ lại ROI; phiên vẫn chạy với cờ `CAM2_UNVERIFIED`. `vision` chết tự khởi động lại |
| Ổ đầy / cảnh báo ổ ≥ 80 % | API-81; `df -h`; mục 9 | Giảm `retention_raw_days`; bỏ Giữ clip không còn cần; thêm ổ. Ổ đầy → MediaMTX ngừng ghi |
| Shopee "Hết hạn" / lỗi đồng bộ | Cài đặt → Shopee; `dc logs worker-sync \| grep platform_` | Token hết hạn (J-12 không refresh được) → bấm **Kết nối lại**. `CREDENTIALS_UNREADABLE` = `FERNET_KEY` khác lúc kết nối (khôi phục sai `.env`) → dùng lại `.env` cũ hoặc Kết nối lại. `error_sign` / `error_permission` = sai partner key / quyền app → sửa `SHOPEE_*` (shop không bị đánh "Hết hạn"). Lỗi mạng tạm: tự thử lại; đơn vẫn nhập được bằng CSV. Quét vẫn chạy khi mất Internet (kiện "chưa xác minh", J-05 xác minh lại) |
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
- [ ] Chỉ 80, 443, 8189 mở trên server (`ss -lntup`), và chỉ cho dải LAN kho (bind `LAN_IP` hoặc rule `DOCKER-USER` — mục 1); api 8000, Postgres, Redis, API MediaMTX 9997 không truy cập được từ LAN.
- [ ] MediaMTX chỉ cho đọc / API không mật khẩu từ mạng compose của stack (`MTX_AUTHINTERNALUSERS_0_IPS` = 127.0.0.1, ::1, `AICAM_SUBNET` — G3-N9), không cả `172.16.0.0/12`.
- [ ] Header CSP có trong response trang (`curl -kI https://<SITE_ADDRESS>/ | grep -i content-security`). FE đổi script chọn theme inline trong `index.html` → tính lại hash `sha256-…` trong `docker/Caddyfile` (trình duyệt báo lỗi CSP ở Console).
- [ ] Body quá lớn bị chặn: upload nhập đơn > 6 MB, API khác > 1 MB → 413 (Caddy `request_body` + api — G3-N2).
- [ ] `/live/...` không có token → 401; tài khoản CSKH / STATION → 403.
- [ ] Admin đầu tiên đổi mật khẩu mạnh; tắt tài khoản không dùng (Người dùng).
- [ ] Sao lưu chạy (`backup_ok` trong log) và đã thử khôi phục một lần; `BACKUP_DIR` ở ổ khác; `docker/.env` có bản sao off-site.
- [ ] Camera ở VLAN riêng; mật khẩu camera không phải mặc định nhà sản xuất.
- [ ] Truy cập từ xa (nếu cần) chỉ qua Tailscale / tunnel, không mở cổng router.
