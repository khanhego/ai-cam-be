"""T-230 (02a §9, ops §7.2): stack production (`docker/compose.yml`) truyền đủ biến Phase 3 vào mọi service
ứng dụng, `.env.production.example` có đủ biến để IT điền, và worker Phase 3 có trong compose production.

Trước T-230 compose production thiếu mọi biến `TIKTOK_*`, `S3_*`, `BACKUP_*` → sao lưu cloud / link / TikTok
luôn "chưa cấu hình" ở kho dù `.env` đã điền (DEC-783).
"""

import re
from pathlib import Path

import pytest

DOCKER = Path(__file__).resolve().parents[2] / "docker"
COMPOSE = (DOCKER / "compose.yml").read_text(encoding="utf-8")
ENV_EXAMPLE = (DOCKER / ".env.production.example").read_text(encoding="utf-8")

PHASE3_ENV = (
    "TIKTOK_ENABLED", "TIKTOK_RETURNS_ENABLED", "TIKTOK_ADAPTER", "TIKTOK_APP_KEY", "TIKTOK_APP_SECRET",
    "TIKTOK_SERVICE_ID", "TIKTOK_REDIRECT_URL", "S3_ENDPOINT", "S3_PUBLIC_ENDPOINT", "S3_REGION",
    "S3_BUCKET", "S3_SHARE_BUCKET", "S3_ADDRESSING_STYLE", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY",
    "NOTIFY_ENABLED", "NOTIFY_TRANSPORT", "TELEGRAM_BOT_TOKEN",
    "ZALO_APP_ID", "ZALO_APP_SECRET", "ZALO_OA_REFRESH_TOKEN", "SITE_ADDRESS", "SYNC_TASK_BUDGET_S",
    "SYNC_LONG_TASK_BUDGET_S",
)  # fmt: skip


def _app_env_block() -> str:
    start = COMPOSE.index("environment: &app_env")
    return COMPOSE[start : COMPOSE.index("\nx-backup-keys:")]


@pytest.mark.parametrize("name", PHASE3_ENV)
def test_production_compose_passes_phase3_env(name: str) -> None:
    assert re.search(rf"^\s+{name}: \$\{{{name}:-", _app_env_block(), re.M), f"compose.yml thiếu {name}"


@pytest.mark.parametrize("name", PHASE3_ENV)
def test_env_example_documents_phase3_env(name: str) -> None:
    assert re.search(rf"^#?\s*{name}=", ENV_EXAMPLE, re.M), f".env.production.example thiếu {name}"


def test_production_secrets_have_no_insecure_defaults() -> None:
    """Secret Phase 3 mặc định rỗng (không có khóa mẫu trong compose production — khác compose.dev.yml)."""
    for name in ("S3_SECRET_ACCESS_KEY", "TELEGRAM_BOT_TOKEN", "TIKTOK_APP_SECRET"):
        assert f"{name}: ${{{name}:-}}" in _app_env_block()
    for name in BACKUP_KEYS:
        assert f"{name}: ${{{name}:-}}" in _backup_keys_block()
    assert "NOTIFY_TRANSPORT: ${NOTIFY_TRANSPORT:-real}" in _app_env_block()
    # Cờ lùi chỉ đặt khi chạy downgrade, không để trong .env.
    assert not re.search(r"^AICAM_DOWNGRADE_", ENV_EXAMPLE, re.M)


@pytest.mark.parametrize(
    "service", ["worker-sync", "worker-sync-long", "worker-notify", "worker-backup", "beat"]
)
def test_phase3_services_in_production_compose(service: str) -> None:
    assert re.search(rf"^  {service}:\n    <<: \*app", COMPOSE, re.M)


BACKUP_KEYS = ("BACKUP_ENCRYPTION_KEY", "BACKUP_OLD_KEYS")


def _backup_keys_block() -> str:
    start = COMPOSE.index("x-backup-keys: &backup_keys")
    return COMPOSE[start : COMPOSE.index("\nservices:")]


def _service_block(service: str) -> str:
    start = COMPOSE.index(f"\n  {service}:\n")
    nxt = re.search(r"\n  [a-z][a-z-]*:\n", COMPOSE[start + 1 :])
    return COMPOSE[start : start + 1 + nxt.start()] if nxt else COMPOSE[start:]


def test_backup_keys_only_where_needed_g3_bk7() -> None:
    """G3-BK-7: khóa sao lưu không nằm trong env chung — chỉ api / worker-backup / worker-notify nhận."""
    for name in BACKUP_KEYS:
        assert name not in _app_env_block()
        assert name in _backup_keys_block()
    for service in ("api", "worker-backup", "worker-notify"):
        assert "<<: [*app_env, *backup_keys]" in _service_block(service), service
    for service in (
        "worker",
        "worker-sync",
        "worker-sync-long",
        "worker-export",
        "vision",
        "beat",
        "migrate",
    ):
        assert "backup_keys" not in _service_block(service), service
