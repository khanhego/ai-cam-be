# Một image cho mọi tiến trình: api, worker, beat, vision (02a §2, ADR-002).
FROM python:3.12-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

# FFmpeg cho cắt clip / xuất; fontconfig + Be Vietnam Pro cho overlay drawtext (02a J-03).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fontconfig curl ca-certificates \
    && mkdir -p /usr/share/fonts/truetype/bevietnampro \
    && for w in Regular SemiBold; do \
         curl -fsSL -o /usr/share/fonts/truetype/bevietnampro/BeVietnamPro-$w.ttf \
           "https://github.com/google/fonts/raw/main/ofl/bevietnampro/BeVietnamPro-$w.ttf"; \
       done \
    && fc-cache -f \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /app

# Cài phụ thuộc trước để tận dụng cache layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY README.md ./
# Migration chạy trong container trước khi api khởi động (architecture §14.2).
COPY alembic.ini ./
COPY alembic ./alembic
RUN uv sync --frozen --no-dev

RUN useradd --create-home --uid 10001 aicam
USER aicam

# IP thật của client cho rate-limit đăng nhập (BR-13) khi chạy sau reverse proxy: đặt FORWARDED_ALLOW_IPS = IP proxy
# lúc deploy (checklist T-19). Mặc định chỉ tin 127.0.0.1 — header X-Forwarded-For từ nơi khác bị bỏ qua (review M1 #3).
ENV FORWARDED_ALLOW_IPS=127.0.0.1
EXPOSE 8000
# Không bật access log của uvicorn (G3-F3): path kèm query lộ `sig` URL ký và `token` WS. Caddy đã ghi access log
# có che query; logger stdlib còn lại đi qua bộ che `RedactQueryFilter` (core/logging.py).
CMD ["uvicorn", "aicam.entrypoints.api:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--no-access-log"]
