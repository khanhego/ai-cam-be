# ai-cam-be

Backend của Hệ thống X: ghi hình 2 camera và đối soát quy trình đóng gói. Một codebase Python chạy 4 tiến trình: `api`, `worker`, `beat`, `vision`.

Tài liệu thiết kế nằm ở repo gốc `AI-cam-shop-managment/docs/ai/`:
- Kiến trúc: `system/architecture.md`.
- Spec backend MVP: `items/01-packing-mvp/02a-be-spec.md`.
- API contract: `items/01-packing-mvp/02-tech-spec.md` §6.

## Yêu cầu

- Python 3.12 và [uv](https://docs.astral.sh/uv/).
- Docker (dùng cho Postgres, Redis, MediaMTX).
- FFmpeg (khi chạy test media ngoài Docker).

## Lệnh

| Việc | Lệnh |
|---|---|
| Cài phụ thuộc | `uv sync` |
| Lint + format | `uv run ruff check . && uv run ruff format --check .` |
| Type check | `uv run mypy` |
| Ranh giới module | `uv run lint-imports` |
| Test (unit + integration) | `uv run pytest` (cần Postgres của stack dev; DB `aicam_test` tự tạo) |
| Chỉ unit test | `uv run pytest -m "not integration"` |
| Migration | `uv run alembic upgrade head` · tạo mới: `uv run alembic revision --autogenerate -m "<tên>"` · kiểm khớp model: `uv run alembic check` |
| Tạo Admin đầu tiên | `uv run aicam create-admin --username admin` (mật khẩu hỏi qua stdin hoặc `AICAM_ADMIN_PASSWORD`) |
| Chạy API local | `uv run uvicorn aicam.entrypoints.api:app --reload --port 8180` |
| Stack dev (Docker) | `docker compose -f docker/compose.dev.yml up --build` |
| Build image | `docker build -f docker/Dockerfile -t aicam .` |

## Port của stack dev

Các port host được lệch khỏi giá trị chuẩn để không đụng dự án khác chạy trên cùng máy.

| Service | Host | Container |
|---|---|---|
| api | 8180 | 8000 |
| postgres | 55432 | 5432 |
| redis | 56379 | 6379 |
| mediamtx RTSP | 58554 | 8554 |
| mediamtx WebRTC / WHEP | 58889 | 8889 |
| mediamtx API | 59997 | 9997 |

Kiểm tra API đang chạy: `curl http://localhost:8180/healthz`.

## Camera giả (dev)

`fake-cam1` và `fake-cam2` phát lặp video mẫu 60 giây vào MediaMTX, path `cam-fake1` và `cam-fake2`. Giờ thực được chèn lên hình để giả lập OSD camera.

Kịch bản khay của Cam 2 (lặp mỗi 60 giây):

| Giây | Trên khay |
|---|---|
| 0–20 | `SPXTST0000001` |
| 20–25 | trống |
| 25–45 | `SPXTST0000002` |
| 45–50 | `SPXTST0000002` + `SPXTST0000003` |
| 50–60 | trống |

- Xem trực tiếp: `ffplay rtsp://localhost:58554/cam-fake2`.
- File ghi hình nằm trong volume `video`, đường dẫn `/data/video/raw/<path>/YYYY/MM/DD/HH-MM-SS-ffffff.mp4`. Giờ trong tên file là UTC, mỗi segment 60 giây.

## Cấu trúc

```
src/aicam/
├── core/          # settings, logging, db, redis, security, errors (dùng chung)
├── modules/       # users, stations, orders, sessions, approvals, media, vision,
│                  # platforms, imports, reports, settings
├── realtime/      # WebSocket hub
├── workers/       # Celery app, lịch beat
├── entrypoints/   # api, worker, beat, vision, cli
└── main.py        # FastAPI app factory
```

`lint-imports` kiểm hai quy tắc ranh giới:
- `core` không import module nghiệp vụ.
- Module nghiệp vụ không import `entrypoints`.
