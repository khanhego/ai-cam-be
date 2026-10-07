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

### 6.1 Sao lưu local (`pg-backup.sh`)

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

### 6.2 Sao lưu cloud (Phase 3 — FR-02.08, 02.13..18, ADR-010)

Ngoài bản sao local ở trên, Phase 3 sao lưu **DB + file nhập mỗi 6 giờ** (01, 07, 13, 19 giờ VN) và **bằng chứng
cần giữ** (clip / ảnh của hồ sơ — BR-33) trong ≤ 1 giờ lên kho S3-compatible, **mã hóa tại kho** (AES-256-GCM,
định dạng `AICAMENC1`). Nhà cung cấp chỉ thấy bản mã. Service: `worker-backup` (queue `backup`), lịch ở `beat`.
Nhà cung cấp thật chưa chốt (Q20) — mọi bước dưới đã chạy trên MinIO; với nhà cung cấp thật: **chưa test**.

**Cài lần đầu (IT)**

1. Tạo 2 bucket riêng tư: sao lưu (bật **versioning** + lifecycle "xóa phiên bản cũ sau 7 ngày" + object lock
   governance 7 ngày nếu có) và link chia sẻ (không versioning, lifecycle xóa `share/` > 8 ngày). Tạo khóa ứng
   dụng theo `docs/s3-policy.example.json` (thay tên bucket) — khóa này **không** được xóa phiên bản / đổi
   versioning, nên máy kho bị chiếm quyền cũng không xóa vĩnh viễn được bản sao (RK-28).
2. `dc run --rm api aicam backup-keygen` → chép `BACKUP_ENCRYPTION_KEY=…` vào `docker/.env` cùng `S3_ENDPOINT`,
   `S3_REGION`, `S3_BUCKET`, `S3_SHARE_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` (`S3_PUBLIC_ENDPOINT` nếu
   host ký URL khác). `dc up -d api worker-backup beat`.
3. **Cất bản sao khóa ngoài máy** (két / trình quản lý mật khẩu, 2 nơi). Mất khóa = bản cloud vô dụng (EX-K5).
4. Dashboard → Sao lưu cloud: **Kiểm tra kết nối** (ghi / đọc / xóa 1 KB) → đối chiếu dấu vân tay
   `XXXX-XXXX-XXXX-XXXX` với dòng in ở bước 2 → **Đã cất bản sao khóa giải mã** → sao lưu bật.
5. Bấm **Sao lưu DB ngay**, chờ lịch sử có dòng "Thành công".

**Bí mật phải cất ngoài máy** (kiểm ở mỗi diễn tập — 02 API-186): `BACKUP_ENCRYPTION_KEY` + **mọi khóa cũ còn bản
trên cloud**; `FERNET_KEY` (token sàn, mật khẩu camera, URL link, token Zalo trong DB — mất → kết nối lại mọi shop,
nhập lại mật khẩu camera, link cũ không sao chép được); `S3_ENDPOINT`, `S3_BUCKET`, `S3_SHARE_BUCKET`,
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` + tài khoản quản trị nhà cung cấp (khôi phục phiên bản cũ); `JWT_SECRET`;
`MEDIA_SIGNING_KEY`; `POSTGRES_PASSWORD`; `SHOPEE_PARTNER_ID` / `SHOPEE_PARTNER_KEY`; `TIKTOK_APP_KEY` /
`TIKTOK_APP_SECRET` / `TIKTOK_SERVICE_ID`; `TELEGRAM_BOT_TOKEN`; `ZALO_APP_ID` / `ZALO_APP_SECRET`;
`SITE_ADDRESS`, `LAN_IP`. Cách nhanh: cất nguyên `docker/.env` + danh sách khóa cũ.

**Đổi khóa sao lưu** (lộ khóa / nhân sự nghỉ — EX-K7): `aicam backup-keygen` → đặt khóa mới vào
`BACKUP_ENCRYPTION_KEY`, **chuyển khóa cũ sang `BACKUP_OLD_KEYS`** (cách dấu phẩy) → `dc up -d api worker-backup`.
Sao lưu dừng ("Khóa đã đổi") tới khi Admin xác nhận dấu vân tay mới ở D23. Sau đó D23 hiện "{N} tệp bằng chứng và
{M} bản DB mã hóa bằng khóa cũ" + nút **Tải lại bằng chứng bằng khóa mới** (chỉ tệp còn ở kho; bản DB cũ hết hạn
theo chính sách 30 ngày / tháng). **Giữ khóa cũ** (cất ngoài máy + `BACKUP_OLD_KEYS`) tới khi D23 không còn dòng
khóa cũ — tệp đã bị xóa tại kho không tải lại được, chỉ khôi phục được bằng khóa cũ.

**Theo dõi** — D23 (trạng thái, lịch sử 14 ngày, tệp chờ / lỗi), D2 "Cần xử lý" + N08 khi: DB không thành công >
26 giờ, 2 lượt DB liền lỗi, tệp chờ > 24 giờ, lệch mã băm, không thấy tệp tại kho. Log: `backup_db`,
`backup_object`, `backup_hash_mismatch`, `backup_source_missing`, `backup_lease_expired`, `backup_prune`.

**Khôi phục sang máy mới (RTO DB ≤ 60 phút — NFR-40)**

```sh
# 0. Máy mới đã cài hệ thống (cùng phiên bản image), docker/.env khôi phục từ bản cất (cùng FERNET_KEY, S3_*,
#    BACKUP_ENCRYPTION_KEY; khóa cũ trong BACKUP_OLD_KEYS hoặc tệp riêng). DB trống: chưa chạy migrate.
dc up -d postgres redis
dc run --rm api aicam backup-restore --db latest --evidence  # tải + giải mã + pg_restore vào DB trống,
#    giải nén file nhập cùng lượt vào IMPORT_ROOT (không ghi đè), rồi tải bằng chứng từ backup/evidence/ về
#    VIDEO_ROOT (hồ sơ khiếu nại chưa đóng trước). Clip / ảnh không có bản cloud và không có tệp → "Thiếu tệp"
#    (MISSING — KHÔNG phải "Đã xóa", không kéo theo xóa bản cloud nào). Đối tượng không có trong DB (tải lên sau
#    bản dump) vẫn được tải về, in "ngoài DB". In: tải N / thiếu N / ngoài DB N / giải mã lỗi N / thiếu khóa N.
#    Chỉ DB trước, bằng chứng sau: bỏ --evidence rồi chạy lại với --evidence-only.
#    khóa không khớp → "Khóa giải mã không khớp (dấu vân tay …)", mã 2, KHÔNG ghi gì → tìm đúng khóa
#    DB không trống → từ chối (mã 2); cố ý ghi đè: --force
#    thêm khóa cũ: --key-file /đường/dẫn/khoa-cu.txt (lặp được)
dc run --rm api alembic upgrade head                          # bản dump cũ hơn image → nâng schema
dc up -d
dc run --rm api aicam backup-verify                           # đạt (mã 0) → gỡ "Chờ kiểm khôi phục"
```

Sau `backup-restore` sao lưu tự động **tắt** (D23 "Chờ kiểm khôi phục", J-20..J-23 không chạy — máy mới không tự xóa
bản cloud nào) tới khi `backup-verify` đạt; Admin bật lại ở D23.

`backup-verify` in `khớp N / lệch đã chấp nhận N / lệch N / thiếu đã ghi nhận N / thiếu N`. Đạt = lệch 0 và thiếu
0. Không đạt → mã 1, xem danh sách id (≤ 50 trên màn hình, đủ trong `VIDEO_ROOT/restore-reports/verify-….csv`).

**Lối ra khi khôi phục / kiểm không trọn (DEC-518 — luôn có đường ra có dấu vết)**

| Tình huống | Làm gì |
|---|---|
| `backup-restore` mã 3: có đối tượng **giải mã lỗi** (`DECRYPT_FAILED` — hỏng / bị sửa) hoặc **thiếu khóa** (`UNKNOWN_KEY`) | Lệnh đã làm hết phần còn lại; tệp lỗi không được ghi (không tệp dở), clip / ảnh đó thành "Thiếu tệp". Danh sách: `VIDEO_ROOT/restore-reports/restore-failures-….csv` (`kind, id, object_key, reason, key_fp`). Thiếu khóa → tìm khóa cũ theo `key_fp`, chạy `aicam backup-restore --evidence-only --key-file khoa-cu.txt` → tệp về lại bình thường. Đối tượng hỏng: thử khôi phục phiên bản cũ ở bucket (tài khoản quản trị nhà cung cấp, ≤ 7 ngày) rồi `--evidence-only` lại; không được thì giữ "Thiếu tệp". |
| `backup-verify` báo **lệch** (tệp trên đĩa khác mã băm lúc tạo) | Xem từng id (D4 / D17). Nếu chấp nhận bản hiện có: `aicam backup-verify --accept <id> [<id>…] --reason "lý do 5–500 ký tự"` → ghi "lệch đã chấp nhận" + audit `BACKUP_VERIFY_ACCEPT`; bản gốc trên cloud **không** bị ghi đè. |
| `backup-verify` báo **thiếu** (`READY` mà không có tệp) | Chép lại tệp nếu còn ở đâu đó rồi chạy lại verify; không còn → `--accept <id> --reason "…"` → "Thiếu tệp" (không phải "Đã xóa"). |
| Tệp đã "Vẫn sao lưu bản hiện có" ở D23 trước sự cố | Tự tính "lệch đã chấp nhận" (metadata `integrity=MISMATCH_ACCEPTED`) — không làm trượt. |
| Muốn kiểm sâu bản cloud (diễn tập) | `aicam backup-verify --from-cloud [--key-file …]` — giải mã từng bản cloud, so SHA-256 (chỉ chẩn đoán, không gỡ cờ). |

Không có cờ bỏ kiểm toàn bộ: mọi mục lệch / thiếu phải được xem và chấp nhận từng id, có lý do.

**Diễn tập** mỗi quý (AC-50): làm đủ các bước trên ở máy / VM khác, ghi thời gian từng bước, số khớp / thiếu.
Biên bản diễn tập dev: `docs/ai/items/03-expansion-tiktok/evidence/m15-restore-drill.txt` (repo tài liệu).

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

Phase 2 thêm 3 migration: **0003** (bảng / cột mới, chỉ thêm), **0004** (clip đang "Giữ" → hồ sơ khiếu nại
"Chuyển từ cờ giữ" — `LEGACY_HOLD`) và **0005** (index tìm kiện hoàn theo tiền tố). Thời gian: đo trên máy dev với
1 triệu kiện, 0003 chạy ~34 giây và **khóa bảng kiện suốt thời gian đó** (một transaction; đọc / ghi kiện chờ) —
nâng cấp ngoài giờ đóng gói; 0005 ~1,3 giây (index tạo không `CONCURRENTLY` — mọi service đã dừng). Từ Phase 2, clip được giữ theo **hồ sơ** (ADR-009) thay cho cờ giữ từng clip.

**Nâng cấp** — khác mục 7: **dừng mọi service ứng dụng trước khi migrate** (như phần lùi). Lý do: 0004 bỏ cờ
"Giữ" của clip đã chuyển thành hồ sơ `LEGACY_HOLD`; J-02 của image Phase 1 (02:00 giờ VN) nếu còn chạy chỉ biết cờ
này → xóa đúng các clip bằng chứng đó. 0004 tự từ chối khi còn kết nối khác vào DB và có clip đang giữ (log
`0004: còn N kết nối khác…`); từ Phase 2, api / worker / beat / vision so phiên bản schema với image lúc khởi động
và **thoát** (log `schema_version_mismatch`; worker / beat / vision mã 78, api mã 3 vì uvicorn bọc lỗi lifespan
thành `Application startup failed` — đo ở G5) nếu lệch, J-02 kiểm lại ngay trước khi xóa.

```sh
dc exec backup /bin/sh /pg-backup.sh once                  # 1. sao lưu DB + snapshot volume video (NAS / RAID)
git -C ../ai-cam-be pull && git -C ../ai-cam-fe pull         # 2. mã Phase 2 (BE + FE cùng lúc)
(cd ../ai-cam-fe && pnpm install --frozen-lockfile && pnpm build)
dc stop api vision worker worker-sync worker-export beat   # 3. BẮT BUỘC: không còn tiến trình Phase 1 nào
dc build migrate && dc run --rm migrate alembic current    # 4. image mới; phải in 0002 (Phase 1)
dc run --rm migrate alembic upgrade head                   # 5. 0003 → 0004 → 0005 (một transaction)
dc run --rm migrate alembic current                        #    phải in 0005 (head)
dc exec postgres psql -U aicam -d aicam -c 'VACUUM ANALYZE package'   # 6. dọn bloat backfill 0003, cập nhật thống kê
dc up -d                                                   # 7. mọi service image mới
dc ps                                                      # 8. api healthy; worker, worker-sync, worker-export, beat, vision Up
```

Mạng compose của bản Phase 1 chưa có `ip_range` (thêm ở G5 item 02 — giữ `CADDY_IP` không bị container khác chiếm):
bước 7 lần đầu báo lỗi tạo lại mạng ("has active endpoints") → thay bước 7 bằng `dc down` (**không** `-v`, giữ
volume) rồi `dc up -d`.

1. Sàn giữ clip: `RETENTION_CLIP_MIN_DAYS` (mặc định 60, đặt trong `docker/.env`). 0003 nâng `retention_clip_days`
   dưới sàn lên sàn + audit `RETENTION_RAISED_TO_MINIMUM`.
2. Log bước 5: `0004: N clip giữ → M hồ sơ LEGACY_HOLD` và `held_before=… protected_after=…` (tập sau ≥ tập trước —
   giữ theo phiên, cả Cam 1 + Cam 2). Lỗi → không có gì thay đổi (một transaction): `lock_timeout` 5 giây (còn
   tiến trình giữ khóa bảng kiện) → làm lại bước 3; lỗi khác → giữ image cũ (`AICAM_IMAGE=<tag Phase 1> dc up -d`),
   gửi log cho BE.
3. **Đối soát và hàng hoàn chạy ngay khi beat lên**: J-14 (đối soát, 30 phút) và J-13 (yêu cầu trả hàng Shopee, 15
   phút). Muốn bật dần: đặt `RECON_ENABLED=false` (tắt J-14) trong `docker/.env` trước bước 7. J-13 với Shopee thật
   **tắt mặc định** (`SHOPEE_RETURNS_ENABLED=false`) tới khi API `returns` được xác nhận với tài khoản partner (T-3);
   bật bằng `SHOPEE_RETURNS_ENABLED=true` + `dc up -d`.
4. Lượt J-13 đầu của mỗi shop (chưa có mốc hàng hoàn) lùi `SHOPEE_RETURNS_INITIAL_DAYS` ngày (mặc định 15 — shop
   kết nối từ Phase 1 có yêu cầu trả đang chạy từ trước nâng cấp). Cần lùi xa hơn: đặt biến này (tối đa 60) trước
   lượt đầu; đã chạy rồi thì xóa mốc của shop: `dc exec postgres psql -U aicam -d aicam -c "UPDATE shop SET
   last_return_cursor = NULL"` — lượt kế lùi lại từ đầu (idempotent theo mã yêu cầu trả). Cảnh báo / mốc quá hạn
   chỉ tính cho kiện vào hàng hoàn **sau** lúc nâng cấp (`recon_start_at`).
5. Admin xem **Hồ sơ khiếu nại** lọc nguồn "Chuyển từ cờ giữ" (hạn = lúc nâng cấp + 30 ngày) — đóng hồ sơ không còn cần.

**Lùi về Phase 1** — chỉ khi không sửa tiến được (ưu tiên forward-fix). Thứ tự bắt buộc: **downgrade bằng image mới
trước, đổi image sau**. Downgrade không xóa dữ liệu Phase 2: chép sang schema `phase2_archive` (hồ sơ hàng hoàn,
phiên nhận hàng hoàn + clip / sự kiện / bản xuất / yêu cầu duyệt của nó, kiện tạm `TAM-`, ảnh, hồ sơ khiếu nại +
bằng chứng + ghi chú + gói bằng chứng, cảnh báo đối soát, lịch sử trạng thái hoàn, cột Phase 2 của station / cài đặt
/ phiên / kiện, số thứ tự mã HH- / KN- / TAM-). Kiện đang ở trạng thái hoàn hiện lại trạng thái cuối trước đó (không
có → "Đã giao"). Clip đang được bảo vệ theo hồ sơ được đặt cờ **Giữ** để J-02 của image cũ không xóa. File video,
ảnh, gói zip **giữ nguyên trên đĩa**. Cờ Giữ do downgrade đặt đứng tên "Hệ thống (bảo vệ bằng chứng Phase 2)" (người
dùng không đăng nhập được). Kiện tạm `TAM-` còn phiên đóng gói (hiếm) giữ lại với trạng thái "Đã hủy".

```sh
dc exec backup /bin/sh /pg-backup.sh once                  # 1. sao lưu (bắt buộc) + snapshot volume video
# 2. Hoàn tất / hủy mọi phiên nhận hàng hoàn đang mở ở station (downgrade từ chối nếu còn — không đổi gì).
#    Phiên hoàn có clip "Không cắt được" / đang cắt: bấm Thử lại (API-46), chờ READY — downgrade TỪ CHỐI nếu còn
#    (image cũ không giữ video thô cho chúng); chấp nhận mất: AICAM_DOWNGRADE_ALLOW_UNCUT_RETURN_CLIPS=1 (dc run -e).
dc stop api vision worker-sync worker-export beat          # 3a. dừng nhận việc mới; worker còn chạy để cắt clip
dc exec redis redis-cli llen video                         # 3b. chờ tới khi in 0 (J-01 phiên vừa đóng đã chạy xong —
#    downgrade TỪ CHỐI nếu còn phiên hoàn đã kết thúc chưa có clip, BUG-G5-P2-2)
dc stop worker                                             # 3c. dừng worker
dc run --rm migrate alembic downgrade 0002                 # 4. bằng IMAGE MỚI (0005, 0004 rồi 0003, một transaction)
dc run --rm migrate alembic current                        #    phải in 0002
# 5. Log bước 4: "0004 downgrade: đặt held cho N clip…", "0003 downgrade: chép sang phase2_archive {…số dòng…}"
AICAM_IMAGE=<tag Phase 1> dc up -d                         # 6. rồi mới đổi image BE (và build FE Phase 1)
```

- **Không** xóa schema `phase2_archive`, **không** dọn `clips/`, `snapshots/`, `exports/pack-*` bằng tay, **không** bỏ
  "Giữ" hàng loạt khi đang chạy Phase 1 (bỏ giữ → J-02 cũ xóa được clip bằng chứng).
- Image cũ **không chạy được** trên DB đã nâng cấp: `migrate` của nó báo `Can't locate revision identified by '0005'` (revision mới nhất)
  (thoát ≠ 0) nên `api` / worker không khởi động — đúng ý (chặn J-02 cũ). Gặp lỗi này: làm lại bước 3–4 bằng image
  mới. Không bỏ qua bằng `docker start` / `dc start api`.
- Lỗi ở bước 4 → cả lệnh lùi lại, DB giữ nguyên Phase 2 (chạy lại sau khi xử lý nguyên nhân trong log).

**Nâng cấp lại lên Phase 2** sau khi đã lùi: như phần Nâng cấp. 0003 khôi phục mọi thứ từ `phase2_archive` (kiện mà
image cũ đã đổi trạng thái thì giữ trạng thái mới — log `kiện hoàn đã đổi trạng thái`, đối soát J-14 sẽ báo lệch nếu
có); 0004 trả cờ giữ do downgrade đặt (clip Admin đã giữ trước khi lùi vẫn giữ), khôi phục hồ sơ "Chuyển từ cờ giữ"
cũ, chỉ tạo hồ sơ mới cho clip được giữ thêm trong lúc chạy Phase 1, rồi drop `phase2_archive`. Mã HH- / KN- / TAM-
mới không trùng mã cũ. Kiểm log migrate: `0003: khôi phục từ phase2_archive {…}` (số dòng = lúc chép) và
`0004: khôi phục …`. Hồ sơ có clip bằng chứng bị xóa trong lúc chạy Phase 1 → log `0004: N hồ sơ có clip bằng chứng
bị xóa…` + audit `EVIDENCE_CLIP_DELETED_DURING_ROLLBACK` (báo CSKH). Mã `TAM-` bị kiện khác dùng trong lúc chạy
Phase 1 → 0003 dừng với danh sách mã (đổi mã kiện kia rồi chạy lại).

### 7.2 Phase 3 (TikTok Shop, báo cáo, sao lưu cloud, link chia sẻ, thông báo): nâng cấp và lùi về Phase 2

> Đang soạn theo từng task của item 03 — runbook đủ bước (dừng `worker-sync-long worker-backup worker-notify`,
> `aicam fix-cancel-requests`, lùi có cờ) hoàn thiện ở T-230.

Phase 3 thêm 2 migration: **0006** (9 bảng mới, cột mới, CHECK mở rộng `TIKTOK` / `MISSING`, backfill nhóm trạng thái
đơn / yêu cầu trả theo bảng Shopee, `return_case.shop_id`, `shop.grant_ref`, `claim.submitted_at` / `result_at` từ
audit, index báo cáo) và **0007** (mã đơn / mã yêu cầu trả unique theo shop). Cả hai là một transaction, như 7.1:
**dừng mọi service ứng dụng trước khi migrate**.

Thời gian đo trên máy dev (Docker Desktop, không phải server kho — T-201,
`RUN_PERF=1 uv run pytest -m perf tests/integration/test_perf_migration_0006.py -s`): 1 triệu đơn + 1 triệu kiện /
phiên đóng gói / dòng lịch sử, 20.000 hồ sơ hàng hoàn, 5.000 hồ sơ khiếu nại → `alembic upgrade` 0005 → 0006
**~20 giây**, 0005 → 0007 (đủ M11: 4b, 4c, unique theo shop) **~22 giây** (khóa các bảng bị sửa suốt thời gian đó).
Log thêm: `backfill_review_needed [...]` (phiên Supervisor hủy trước Phase 3 đã vào hồ sơ khiếu nại mở với nhãn
"Cần soát" — gửi CSKH soát) và `0006: N kiện có thể bị hủy oan …` (chạy `aicam fix-cancel-requests`, T-285).

**Bước 1b — trả lại kiện hủy oan (sau migrate, trước khi bật lại service):** Phase 2 coi "người mua đang yêu cầu
hủy" (Shopee `IN_CANCEL`) là đơn đã hủy → kiện `NEW` thành `CANCELLED`, `PACKED` thành `CANCELLED_AFTER_PACK` dù sàn
có thể từ chối yêu cầu. Phase 3 chỉ hủy kiện khi sàn hủy thật. Chạy thử trước (chỉ in danh sách, không ghi):

```bash
docker compose run --rm api aicam fix-cancel-requests
```

Mỗi dòng: `SẼ TRẢ LẠI <mã kiện> · đơn <mã> (<shop>, nhóm <nhóm>) · CANCELLED → NEW` hoặc `BỎ QUA … — <lý do>`
("Hủy do người chỉnh tay — kiểm tay", "Cảnh báo BR-11 đã được xử lý tay — kiểm tay": xem từng kiện trên D4 /
D15). Đúng thì ghi: `docker compose run --rm api aicam fix-cancel-requests --apply` (mỗi kiện một transaction,
audit `PACKAGE_CANCEL_REVERT`, chạy lại không đổi gì; mã thoát 1 khi có kiện lỗi — xem dòng `LỖI`). Quên chạy:
đồng bộ (J-04 / J-06) tự trả lại kiện của đơn khi sàn từ chối yêu cầu hủy; kiện của đơn vẫn đang yêu cầu hủy được
trả lại nhưng quét vẫn bị chặn tới khi sàn quyết định (BR-01).

**Lùi về Phase 2** (`alembic downgrade 0005` bằng image Phase 3, trước khi về image cũ) chép dữ liệu Phase 3 sang
schema `phase3_archive` rồi mới gỡ; nâng cấp lại khôi phục y hệt. Từ chối (không đổi gì) khi: còn mã đơn / mã yêu
cầu trả trùng giữa shop (0007 — sửa tiến); còn link chia sẻ đang tạo / đang hoạt động (thu hồi trước, hoặc
`AICAM_DOWNGRADE_ALLOW_ACTIVE_SHARES=1`); còn kiện của đơn TikTok / shop Shopee sẽ bị ngắt (Phase 2 một shop) —
`AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS=1` để tách kiện khỏi đơn trong thời gian chạy Phase 2. Bằng chứng đã bỏ còn
hạn giữ → hồ sơ hệ thống "Bằng chứng đã bỏ — giữ tới …" (đã đóng) để Phase 2 giữ đúng hạn. Log migrate in `0006: backfill {…}` (số dòng từng phần) và
cảnh báo `0006: đơn có trạng thái sàn chưa ánh xạ → nhóm UNKNOWN` kèm 20 chữ trạng thái nhiều nhất nếu có. Sau
nâng cấp: `VACUUM ANALYZE "order"` (cập nhật cột nhóm trạng thái).

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

**Báo cáo (D20, API-150..153 — Phase 3).** Tính trực tiếp trên DB, cache Redis 60 giây theo bộ lọc; mỗi lần tính ghi
log `report_built {report, days, seconds}` (WARNING khi kỳ ≤ 92 ngày mà > 3 giây — NFR-37), quá 15 giây bị hủy
(`report_timeout`, người dùng thấy "Không tải được báo cáo."). Xuất CSV ghi audit `REPORT_EXPORT`. Đo trên **máy dev**
(Apple M4 Pro, Postgres Docker Desktop — **chưa đo máy kho**; T-217, `RUN_PERF=1 uv run python
tests/load/perf_reports.py`, DB tạm, 183.000 đơn / kiện 366 ngày, 20 lần mỗi tab): kỳ 92 ngày p95 hàng hoàn 0,50
giây · khiếu nại 0,13 · năng suất 0,25; kỳ 366 ngày p95 1,41 · 0,43 · 0,91 giây (giới hạn 3 / 10 giây). Chạy lại
lệnh trên máy kho trước go-live (dùng `TEST_DATABASE_URL` trỏ Postgres của máy kho; DB tạm bị xóa sau khi đo).

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
| `caddy` đứng ở `Created`, `dc up` báo "Address already in use" | `docker network inspect <dự án>_aicam` — container khác đang giữ `CADDY_IP` | Mạng tạo từ bản compose chưa có `ip_range` (trước G5 item 02): `dc down` (**không** `-v`) rồi `dc up -d` để tạo lại mạng với `AICAM_IP_RANGE`. Gấp: `dc restart <container đang giữ IP>` rồi `dc up -d caddy` |

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
