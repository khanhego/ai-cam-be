"""T-226: nhà cung cấp Telegram / Zalo OA trên server giả (respx) + token Zalo xoay vòng lưu DB Fernet
(DEC-445).

**Giả định theo tài liệu công khai — chưa test với bot / OA thật (Q21).**"""

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.notify.models import NotifyProviderToken
from aicam.modules.notify.providers import SendError, get_provider
from aicam.modules.notify.providers.telegram import TelegramProvider
from aicam.modules.notify.providers.zalo import DbTokenStore, ZaloProvider, ZaloToken

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)
ZALO_API = "https://zalo-api.test"
ZALO_OAUTH = "https://zalo-oauth.test"
SEND = f"{ZALO_API}/v3.0/oa/message/cs"
REFRESH = f"{ZALO_OAUTH}/v4/oa/access_token"


@pytest.fixture(autouse=True)
def _clock() -> None:
    clock.freeze(NOW)


def _zalo(db: AsyncSession, settings: Settings) -> ZaloProvider:
    return ZaloProvider(
        app_id="app-1",
        app_secret="secret-1",
        initial_refresh_token="ref-env",
        api_base=ZALO_API,
        oauth_base=ZALO_OAUTH,
        store=DbTokenStore(db.bind, Cipher(settings.fernet_key)),  # type: ignore[arg-type]
    )


@respx.mock
async def test_telegram_rate_limit_and_token_errors() -> None:
    tg = TelegramProvider("1:abc", "https://tg.test")
    route = respx.post("https://tg.test/bot1:abc/sendMessage")
    route.mock(
        return_value=httpx.Response(
            429, json={"ok": False, "error_code": 429, "description": "Too Many Requests",
                       "parameters": {"retry_after": 17}},
        )
    )  # fmt: skip
    with pytest.raises(SendError) as exc:
        await tg.send("-100", "x")
    assert exc.value.retry_after_s == 17
    assert exc.value.provider_code == "429"
    assert not exc.value.timeout
    route.mock(
        return_value=httpx.Response(401, json={"ok": False, "error_code": 401, "description": "Unauthorized"})
    )
    with pytest.raises(SendError) as exc:
        await tg.send("-100", "x")
    assert exc.value.message == "Bot Telegram trên máy chủ không hợp lệ. Liên hệ IT."
    route.mock(return_value=httpx.Response(502, text="bad gateway"))
    with pytest.raises(SendError) as exc:
        await tg.send("-100", "x")
    assert exc.value.provider_code == "502"
    assert "1:abc" not in exc.value.message


@respx.mock
async def test_zalo_first_send_refreshes_and_stores_encrypted(
    db: AsyncSession, redis_client: object, notify_settings: Settings
) -> None:
    refresh = respx.post(REFRESH).mock(
        return_value=httpx.Response(
            200, json={"access_token": "acc-1", "refresh_token": "ref-2", "expires_in": "90000"}
        )
    )
    send = respx.post(SEND).mock(return_value=httpx.Response(200, json={"error": 0, "message": "Success"}))
    zalo = _zalo(db, notify_settings)
    await zalo.send("8412345", "Xin chào")
    form = refresh.calls.last.request.content.decode()
    assert "refresh_token=ref-env" in form
    assert "grant_type=refresh_token" in form
    assert "app_id=app-1" in form
    assert refresh.calls.last.request.headers["secret_key"] == "secret-1"
    assert send.calls.last.request.headers["access_token"] == "acc-1"
    row = await db.get(NotifyProviderToken, "ZALO_OA", populate_existing=True)
    assert row is not None
    assert row.expires_at == NOW + timedelta(seconds=90000)
    assert row.access_token_enc is not None
    assert b"acc-1" not in row.access_token_enc
    cipher = Cipher(notify_settings.fernet_key)
    assert row.refresh_token_enc is not None
    assert cipher.decrypt(row.refresh_token_enc) == "ref-2"

    # còn hạn → không làm mới lại
    await zalo.send("8412345", "Lần 2")
    assert refresh.call_count == 1
    assert send.call_count == 2
    # gần hết hạn → làm mới bằng refresh token **trong DB** (xoay vòng), không dùng biến môi trường
    clock.advance(timedelta(seconds=90000 - 60))
    refresh.mock(
        return_value=httpx.Response(
            200, json={"access_token": "acc-3", "refresh_token": "ref-3", "expires_in": 3600}
        )
    )
    await zalo.send("8412345", "Lần 3")
    assert "refresh_token=ref-2" in refresh.calls.last.request.content.decode()
    assert send.calls.last.request.headers["access_token"] == "acc-3"


@respx.mock
async def test_zalo_errors(db: AsyncSession, redis_client: object, notify_settings: Settings) -> None:
    store = DbTokenStore(db.bind, Cipher(notify_settings.fernet_key))  # type: ignore[arg-type]
    await store.save(ZaloToken("acc-old", "ref-1", NOW + timedelta(hours=10)))
    refresh = respx.post(REFRESH).mock(
        return_value=httpx.Response(
            200, json={"access_token": "acc-new", "refresh_token": "ref-2", "expires_in": 3600}
        )
    )
    send = respx.post(SEND)
    # -216 token bị thu hồi → làm mới ép + gửi lại một lần
    send.side_effect = [
        httpx.Response(200, json={"error": -216, "message": "Access token is invalid"}),
        httpx.Response(200, json={"error": 0, "message": "Success"}),
    ]
    zalo = _zalo(db, notify_settings)
    await zalo.send("841", "x")
    assert refresh.call_count == 1
    assert [c.request.headers["access_token"] for c in send.calls] == ["acc-old", "acc-new"]

    send.side_effect = None
    send.mock(return_value=httpx.Response(200, json={"error": -213, "message": "User has not followed OA"}))
    with pytest.raises(SendError) as exc:
        await zalo.send("841", "x")
    assert exc.value.message == "Người nhận chưa quan tâm OA của shop."
    assert exc.value.provider_code == "-213"

    send.mock(side_effect=httpx.ConnectError("blocked"))
    with pytest.raises(SendError) as exc:
        await zalo.send("841", "x")
    assert exc.value.timeout
    assert exc.value.message == "Không kết nối được Zalo từ máy chủ (mạng chặn?)."

    # làm mới bị từ chối → lỗi rõ, không ghi đè token
    await store.save(ZaloToken("acc-x", "ref-x", NOW - timedelta(minutes=1)))
    refresh.mock(
        return_value=httpx.Response(200, json={"error": -14014, "error_name": "Invalid refresh token"})
    )
    with pytest.raises(SendError) as exc:
        await zalo.send("841", "x")
    assert exc.value.message == "Không làm mới được token Zalo OA trên máy chủ. Liên hệ IT."
    assert (await store.load()).refresh_token == "ref-x"


@respx.mock
async def test_zalo_concurrent_refresh_uses_refresh_token_once(
    engine: AsyncEngine, redis_client: object, notify_settings: Settings
) -> None:
    """Refresh token Zalo dùng một lần: hai lần gửi song song khi token hết hạn → chỉ **một** lần làm mới
    (khóa `zalo:token` + đọc lại DB đã commit — DEC-445, như DEC-433)."""

    async def slow_refresh(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.3)
        return httpx.Response(
            200, json={"access_token": "acc-1", "refresh_token": "ref-2", "expires_in": 3600}
        )

    refresh = respx.post(REFRESH).mock(side_effect=slow_refresh)
    respx.post(SEND).mock(return_value=httpx.Response(200, json={"error": 0}))

    def provider() -> ZaloProvider:  # mỗi tiến trình một kho token, chung DB thật (commit thật)
        return ZaloProvider(app_id="app-1", app_secret="secret-1", initial_refresh_token="ref-env",
                            api_base=ZALO_API, oauth_base=ZALO_OAUTH,
                            store=DbTokenStore(engine, Cipher(notify_settings.fernet_key)))  # fmt: skip

    try:
        tokens = await asyncio.gather(provider().access_token(), provider().access_token())
        assert tokens == ["acc-1", "acc-1"]
        assert refresh.call_count == 1
    finally:
        async with engine.begin() as conn:
            await conn.execute(delete(NotifyProviderToken))


async def test_get_provider_by_transport(db: AsyncSession, notify_settings: Settings) -> None:
    assert type(get_provider("TELEGRAM", notify_settings, db)).__name__ == "MockProvider"
    notify_settings.notify_transport = "real"
    assert isinstance(get_provider("TELEGRAM", notify_settings, db), TelegramProvider)
    assert isinstance(get_provider("ZALO_OA", notify_settings, db), ZaloProvider)
