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
    celery_app._redact_logs(logger=logger)
    assert logging.getLogger("httpx").level == logging.WARNING
    assert any(isinstance(f, RedactQueryFilter) for f in handler.filters)
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
