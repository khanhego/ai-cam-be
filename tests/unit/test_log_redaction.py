"""G3-F3 / G3-N1: log stdlib (uvicorn access, httpx) không lộ `sig`, token WS, token / sign Shopee."""

import logging
from datetime import UTC, datetime

import httpx
import pytest
import respx

from aicam.core import clock
from aicam.core.logging import RedactQueryFilter, install_stdlib_redaction, redact, redact_query
from aicam.modules.platforms.shopee.client import ShopeeClient

SECRETS = ("sigvalue123", "jwt.token.value", "acc-tok-123", "1700000000", "uid-1")


def test_redact_query_masks_known_params_only() -> None:
    url = "/api/v1/media/clips/abc?uid=uid-1&exp=1700000000&sig=sigvalue123&page=2"
    assert redact_query(url) == "/api/v1/media/clips/abc?[uid đã che]&[exp đã che]&[sig đã che]&page=2"
    assert redact_query("/ws/station?token=jwt.token.value") == "/ws/station?[token đã che]"
    assert redact_query("/cb?code=c1&state=s1&shop_id=9") == "/cb?[code đã che]&[state đã che]&shop_id=9"


def test_uvicorn_access_record_keeps_args_tuple() -> None:
    """AccessFormatter của uvicorn unpack `args` (client, method, path, http, status) — phải còn tuple."""
    record = logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d',
        ("10.0.0.5:5000", "GET", "/ws/station?token=jwt.token.value", "1.1", 101), None,
    )  # fmt: skip
    assert RedactQueryFilter().filter(record)
    assert isinstance(record.args, tuple)
    assert record.args[4] == 101
    assert "jwt.token.value" not in record.getMessage()
    assert "[token đã che]" in record.getMessage()
    assert "token=" not in record.getMessage()


def test_uvicorn_access_formatter_output_redacted() -> None:
    from uvicorn.logging import AccessFormatter

    record = logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d',
        ("10.0.0.5:5000", "GET", "/api/v1/media/clips/x?uid=uid-1&exp=1700000000&sig=sigvalue123", "1.1",
         200),
        None,
    )  # fmt: skip
    RedactQueryFilter().filter(record)
    line = AccessFormatter('%(client_addr)s - "%(request_line)s" %(status_code)s', use_colors=False).format(
        record
    )
    assert not any(s in line for s in SECRETS)


def test_structlog_redacts_query_in_values() -> None:
    event = redact(None, "info", {"event": "x", "url": "https://h/p?access_token=acc-tok-123&sign=abc"})
    assert event["url"] == "https://h/p?[access_token đã che]&[sign đã che]"


async def test_shopee_client_http_logs_hide_token_and_sign(caplog: pytest.LogCaptureFixture) -> None:
    """httpx log "HTTP Request: GET <url>" ở INFO chứa nguyên `access_token` / `sign` → WARNING + che."""
    clock.freeze(datetime(2026, 10, 5, 1, 0, tzinfo=UTC))
    # alembic `env.py` (`fileConfig`) tắt logger đã có khi test migration chạy trước trong cùng tiến trình
    # (DEC-545) — bật lại để test không phụ thuộc thứ tự.
    logging.getLogger("httpx").disabled = False
    install_stdlib_redaction()
    caplog.set_level(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    client = ShopeeClient(2001234, "k", "https://partner.test", max_attempts=1)
    with respx.mock:
        respx.get("https://partner.test/api/v2/shop/get_shop_info").mock(
            return_value=httpx.Response(200, json={"error": "", "shop_name": "S"})
        )
        await client.call("GET", "/api/v2/shop/get_shop_info", access_token="acc-tok-123", shop_id="99")
    assert "acc-tok-123" not in caplog.text
    assert "sign=" not in caplog.text
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
    # Kể cả khi ai đó bật lại INFO cho httpx: query vẫn bị che.
    logging.getLogger("httpx").setLevel(logging.INFO)
    caplog.clear()
    with respx.mock:
        respx.get("https://partner.test/api/v2/shop/get_shop_info").mock(
            return_value=httpx.Response(200, json={"error": "", "shop_name": "S"})
        )
        await client.call("GET", "/api/v2/shop/get_shop_info", access_token="acc-tok-123", shop_id="99")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    assert "HTTP Request" in caplog.text
    assert "acc-tok-123" not in caplog.text
    assert "[access_token đã che]" in caplog.text
    assert "access_token=" not in caplog.text
    clock.reset()


def test_celery_logger_signal_installs_redaction() -> None:
    from aicam.workers import celery_app

    logger = logging.getLogger("tst_celery_task_logger")
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logging.getLogger("httpx").setLevel(logging.INFO)
    import structlog

    saved = structlog.get_config()
    try:
        celery_app._redact_logs(logger=logger)
        assert logging.getLogger("httpx").level == logging.WARNING
        assert any(isinstance(f, RedactQueryFilter) for f in handler.filters)
        # T-228 (DEC-781): worker / beat cũng có bộ che structlog (trước đó dùng cấu hình mặc định).
        assert redact in structlog.get_config()["processors"]
    finally:
        structlog.configure(**saved)
        logger.removeHandler(handler)


def test_dockerfile_disables_uvicorn_access_log() -> None:
    from pathlib import Path

    dockerfile = Path(__file__).resolve().parents[2] / "docker" / "Dockerfile"
    cmd = next(line for line in dockerfile.read_text().splitlines() if line.startswith("CMD"))
    assert "--no-access-log" in cmd
    assert "--workers" not in cmd  # G3-F12: api một tiến trình (bus Redis nghe ở mọi tiến trình api)


def test_caddy_default_logger_redacts_query() -> None:
    """G5: log lỗi / cảnh báo của Caddy (reverse_proxy "aborting with incomplete response") ghi `request.uri`
    đầy đủ — logger `default` phải che `sig` / `token` như access log, không chỉ khối `log` của site."""
    import re
    from pathlib import Path

    caddyfile = (Path(__file__).resolve().parents[2] / "docker" / "Caddyfile").read_text()
    global_block = caddyfile[caddyfile.index("{") : caddyfile.index("\n}\n")]
    default_log = re.search(r"log default \{(.*?)\n\t\}", global_block, re.S)
    assert default_log, "thiếu `log default` trong global options"
    body = default_log.group(1)
    assert "request>uri query" in body
    assert "replace sig REDACTED" in body
    assert "replace token REDACTED" in body


# ---------------------------------------------------------------- T-228 (Phase 3: TikTok / Zalo / Telegram /
# khóa sao lưu / URL ký S3 / token link — 02a §2 core/logging.py, NFR-41, NFR-42, DEC-781)

SHARE_TOKEN = "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789_-ABCD"  # 43 ký tự như token_urlsafe(32)


def test_redact_phase3_patterns() -> None:
    tiktok = "https://auth.tiktok-shops.com/api/v2/token/get?app_key=k&app_secret=tt-secret&auth_code=ac1&x=1"
    out = redact_query(tiktok)
    assert "tt-secret" not in out
    assert "ac1" not in out
    assert "app_key=k" in out
    tg = "POST https://api.telegram.org/bot123456:AAH-tg_secret/sendMessage"
    assert redact_query(tg) == "POST https://api.telegram.org/bot[token đã che]/sendMessage"
    s3 = "https://s3.x/aicam-share/share/k/index.html?X-Amz-Credential=AKIA%2F1&X-Amz-Signature=deadbeef"
    assert "deadbeef" not in redact_query(s3)
    assert "AKIA" not in redact_query(s3)
    key = f"share/{SHARE_TOKEN}/index.html"
    assert redact_query(f"NoSuchKey: {key}") == "NoSuchKey: share/[token đã che]/index.html"
    zalo = '{"access_token": "zalo-acc-1", "refresh_token":"zalo-ref-2", "expires_in": "90000"}'
    out = redact_query(zalo)
    assert "zalo-acc-1" not in out
    assert "zalo-ref-2" not in out
    assert '"expires_in": "90000"' in out


def test_registered_secret_values_redacted_everywhere() -> None:
    from aicam.core.logging import SECRET_MASK, register_secrets

    backup_key = "S0VZLUJBQ0tVUC0zMi1CWVRFUy0xMjM0NTY3ODkwYWI="
    register_secrets([backup_key, "short"])
    # Dưới tên trường không nhạy cảm, trong chuỗi tự do, trong log stdlib.
    event = redact(None, "info", {"event": "x", "detail": f"pg_restore failed key={backup_key}", "n": 1})
    assert backup_key not in str(event)
    assert SECRET_MASK in event["detail"]
    record = logging.LogRecord("celery", logging.ERROR, __file__, 1, "boom %s", (backup_key,), None)
    RedactQueryFilter().filter(record)
    assert backup_key not in record.getMessage()
    assert redact_query("short") == "short"  # < 8 ký tự không đăng ký (tránh che nhầm chữ thường)


def test_structlog_exception_traceback_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    import structlog

    from aicam.core.logging import configure_structlog

    saved = structlog.get_config()
    secret = "tg-bot-secret-value-xyz"
    try:
        configure_structlog("INFO", json=True, secrets=[secret])
        log = structlog.get_logger("t228")
        try:
            raise RuntimeError(f"call failed https://api.telegram.org/bot1:{secret}/x token={secret}")
        except RuntimeError:
            log.exception("notify_send_failed", share=f"share/{SHARE_TOKEN}/v.mp4")
        out = capsys.readouterr().out
        assert "notify_send_failed" in out
        assert "RuntimeError" in out
        assert secret not in out
        assert SHARE_TOKEN not in out
    finally:
        structlog.configure(**saved)


def test_settings_hide_secrets_in_repr_and_errors() -> None:
    import base64

    from aicam.core.settings import SECRET_FIELDS, Settings

    key = base64.b64encode(b"K" * 32).decode()
    values = {
        "backup_encryption_key": key,
        "telegram_bot_token": "123:tg-token-in-env",
        "zalo_app_secret": "zalo-app-secret-1",
        "zalo_oa_refresh_token": "zalo-refresh-1",
        "tiktok_app_secret": "tiktok-secret-1",
        "s3_secret_access_key": "s3-secret-key-1",
    }
    cfg = Settings(_env_file=None, app_env="test", **values)  # type: ignore[call-arg]
    text = repr(cfg) + str(cfg)
    assert not any(v in text for v in values.values())
    assert set(values) <= set(SECRET_FIELDS)
    assert key in cfg.secret_values()
    assert "123:tg-token-in-env" in cfg.secret_values()
    # Lỗi validator khi khởi động (log container) không in dict đầu vào.
    with pytest.raises(ValueError, match="TIKTOK_ADAPTER") as err:
        Settings(_env_file=None, app_env="test", tiktok_adapter="bad", **values)  # type: ignore[call-arg]
    assert not any(v in str(err.value) for v in values.values())
    assert "TIKTOK_ADAPTER" in str(err.value)
