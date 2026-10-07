"""T-226: API-170..176 thông báo (02 §6.2; FR-06.04, 06.07, 06.08, 06.10; EX-N1, EX-N2; AC-55 phần gửi thử).

Telegram / Zalo thật: server giả (respx) — **chưa test với bot / OA thật (Q21)**."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.notify.models import NotifyChannel, NotifyMessage

from .notify_fixtures import TG_BASE, TG_TOKEN, login, make_channel, make_notify_settings, mock_sent

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)
URL = "/api/v1/notify/channels"


@pytest.fixture(autouse=True)
def _clock() -> None:
    clock.freeze(NOW)


async def _audit(db: AsyncSession, action: str) -> list[AuditLog]:
    return list((await db.scalars(select(AuditLog).where(AuditLog.action == action))).all())


async def test_crud_and_list(notify_api: AsyncClient, db: AsyncSession) -> None:
    h, _ = await login(notify_api, db)
    res = await notify_api.get(URL, headers=h)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["providers"] == {"TELEGRAM": {"configured": True}, "ZALO_OA": {"configured": True}}  # mock
    assert body["quiet_hours"] == {"enabled": True, "start": "22:00", "end": "07:00"}
    assert [e["code"] for e in body["events"]] == [f"N{i:02d}" for i in range(1, 11)]
    n07 = next(e for e in body["events"] if e["code"] == "N07")
    assert n07["severity"] == "MEDIUM"
    assert n07["suggested_channel"] == "Quản trị"
    assert body["items"] == []

    res = await notify_api.post(
        URL,
        headers=h,
        json={"name": "  Kho  ", "type": "TELEGRAM", "target": "-1001234567890",
              "events": ["N09", "N01", "N01"], "enabled": True},
    )  # fmt: skip
    assert res.status_code == 201, res.text
    item = res.json()
    assert item["name"] == "Kho"
    assert item["events"] == ["N01", "N09"]
    assert item["last_status"] == "NEVER"
    assert item["last_error"] is None
    cid = item["id"]

    res = await notify_api.patch(f"{URL}/{cid}", headers=h, json={"events": ["N02"], "enabled": False})
    assert res.status_code == 200, res.text
    assert res.json()["events"] == ["N02"]
    assert res.json()["enabled"] is False

    # đổi loại → kiểm lại target theo loại mới (Chat ID âm không phải Zalo user ID)
    res = await notify_api.patch(f"{URL}/{cid}", headers=h, json={"type": "ZALO_OA"})
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"]["target"] == "Zalo user ID là dãy 1–64 chữ số."
    res = await notify_api.patch(f"{URL}/{cid}", headers=h, json={"type": "ZALO_OA", "target": "84987654321"})
    assert res.status_code == 200
    assert res.json()["type"] == "ZALO_OA"

    res = await notify_api.get(URL, headers=h)
    assert [i["id"] for i in res.json()["items"]] == [cid]
    assert len(await _audit(db, "NOTIFY_CHANNEL_CREATE")) == 1
    assert len(await _audit(db, "NOTIFY_CHANNEL_UPDATE")) == 2


async def test_validation_and_name_conflict(notify_api: AsyncClient, db: AsyncSession) -> None:
    h, _ = await login(notify_api, db)
    res = await notify_api.post(URL, headers=h, json={"name": "K", "type": "TELEGRAM", "target": "abc",
                                                     "events": []})  # fmt: skip
    assert res.status_code == 422
    fields = res.json()["error"]["details"]["fields"]
    assert fields == {
        "name": "Tên kênh 2–40 ký tự.",
        "target": "Chat ID là một số (nhóm thường bắt đầu bằng -100).",
        "events": "Chọn ít nhất 1 sự kiện.",
    }
    res = await notify_api.post(URL, headers=h, json={"name": "Kho", "type": "TELEGRAM", "target": "1",
                                                     "events": ["N99"]})  # fmt: skip
    assert res.status_code == 422
    assert "events" in res.json()["error"]["details"]["fields"]

    await make_channel(db, "Kho")
    res = await notify_api.post(URL, headers=h, json={"name": "KHO", "type": "TELEGRAM", "target": "1",
                                                     "events": ["N01"]})  # fmt: skip
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "CHANNEL_NAME_EXISTS"
    assert res.json()["error"]["details"]["fields"]["name"] == "Đã có kênh tên này."
    other = await make_channel(db, "CSKH")
    res = await notify_api.patch(f"{URL}/{other.id}", headers=h, json={"name": "kho"})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "CHANNEL_NAME_EXISTS"

    res = await notify_api.patch(
        f"{URL}/00000000-0000-7000-8000-000000000000", headers=h, json={"enabled": True}
    )
    assert res.status_code == 404


async def test_admin_only(notify_api: AsyncClient, db: AsyncSession) -> None:
    for role in ("SUPERVISOR", "CSKH"):
        h, _ = await login(notify_api, db, role)
        for method, path in (
            ("GET", URL),
            ("POST", URL),
            ("GET", "/api/v1/notify/messages"),
            ("PUT", "/api/v1/notify/quiet-hours"),
        ):
            res = await notify_api.request(method, path, headers=h, json={})
            assert res.status_code == 403, (role, method, path)


async def test_provider_not_configured(db: AsyncSession, redis_client: object, tmp_path: Path) -> None:
    """EX-N1: transport thật, chưa có bot / OA → `configured=false`, thêm kênh loại đó → 409."""
    from aicam.core.db import get_session
    from aicam.core.settings import get_settings
    from aicam.main import create_app

    settings = make_notify_settings(tmp_path, notify_transport="real", telegram_bot_token=TG_TOKEN)
    app = create_app(settings)
    app.dependency_overrides[get_session] = lambda: db
    app.dependency_overrides[get_settings] = lambda: settings
    async with AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://testserver") as api:
        h, _ = await login(api, db)
        res = await api.get(URL, headers=h)
        assert res.json()["providers"] == {"TELEGRAM": {"configured": True}, "ZALO_OA": {"configured": False}}
        res = await api.post(URL, headers=h, json={"name": "CSKH", "type": "ZALO_OA", "target": "123",
                                                  "events": ["N04"]})  # fmt: skip
        assert res.status_code == 409
        assert res.json()["error"]["code"] == "PROVIDER_NOT_CONFIGURED"
        assert "Zalo OA" in res.json()["error"]["message"]


async def test_test_send_mock_ok_and_fail(
    notify_api: AsyncClient, db: AsyncSession, notify_settings: Settings
) -> None:
    h, _ = await login(notify_api, db)
    ch = await make_channel(db, "Kho", events=["N02", "N01"])
    res = await notify_api.post(f"{URL}/{ch.id}/test", headers=h)
    assert res.status_code == 200, res.text
    assert res.json() == {"ok": True, "sent_at": "2026-10-07T03:00:00Z"}
    sent = await mock_sent("TELEGRAM")
    assert sent == [
        {
            "target": "-1001234567890",
            "text": "Tin thử từ Hệ thống X — kênh Kho. Bạn sẽ nhận: Camera mất tín hiệu, "
            "Lệch trạng thái mức Cao.",
            "at": "2026-10-07T03:00:00Z",
        }
    ]
    await db.refresh(ch)
    assert ch.last_status == "OK"
    assert ch.last_sent_at == NOW

    notify_settings.notify_mock_fail = "TELEGRAM"
    res = await notify_api.post(f"{URL}/{ch.id}/test", headers=h)
    assert res.status_code == 502
    err = res.json()["error"]
    assert err["code"] == "NOTIFY_SEND_FAILED"
    assert err["details"]["provider_code"] == "MOCK_FAIL"
    res = await notify_api.get(URL, headers=h)
    item = res.json()["items"][0]
    assert item["last_status"] == "ERROR"
    assert item["last_error"]["code"] == "NOTIFY_SEND_FAILED"
    assert item["last_error"]["at"] == "2026-10-07T03:00:00Z"
    audits = await _audit(db, "NOTIFY_TEST")
    assert [a.data["ok"] for a in sorted(audits, key=lambda a: a.id)] == [True, False]  # type: ignore[index]


@respx.mock
async def test_test_send_telegram_fake_server(db: AsyncSession, redis_client: object, tmp_path: Path) -> None:
    """Telegram qua server giả: OK, chat không tồn tại (502 + chữ 02), mạng chặn (504); không lộ token."""
    from aicam.core.db import get_session
    from aicam.core.settings import get_settings
    from aicam.main import create_app

    settings = make_notify_settings(
        tmp_path, notify_transport="real", telegram_bot_token=TG_TOKEN, telegram_api_base=TG_BASE
    )
    route = respx.post(f"{TG_BASE}/bot{TG_TOKEN}/sendMessage")
    app = create_app(settings)
    app.dependency_overrides[get_session] = lambda: db
    app.dependency_overrides[get_settings] = lambda: settings
    async with AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://testserver") as api:
        h, _ = await login(api, db)
        ch = await make_channel(db, "Kho")

        route.mock(return_value=httpx.Response(200, json={"ok": True, "result": {"message_id": 1}}))
        res = await api.post(f"{URL}/{ch.id}/test", headers=h)
        assert res.status_code == 200, res.text
        sent = route.calls.last.request
        assert sent.read().decode().count('"disable_web_page_preview":true') == 1

        route.mock(
            return_value=httpx.Response(
                400, json={"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}
            )
        )
        res = await api.post(f"{URL}/{ch.id}/test", headers=h)
        assert res.status_code == 502
        err = res.json()["error"]
        assert err["message"] == "Telegram không nhận Chat ID này. Kiểm tra bot đã vào nhóm."
        assert err["details"]["provider_code"] == "400"

        route.mock(side_effect=httpx.ConnectTimeout("timed out"))
        res = await api.post(f"{URL}/{ch.id}/test", headers=h)
        assert res.status_code == 504
        assert res.json()["error"] == {
            "code": "NOTIFY_TIMEOUT",
            "message": "Không kết nối được Telegram từ máy chủ (mạng chặn?).",
            "details": {"provider_code": "ConnectTimeout"},
        }
        await db.refresh(ch)
        assert ch.last_status == "ERROR"
        assert TG_TOKEN not in str(ch.last_error)
        audits = await _audit(db, "NOTIFY_TEST")
        assert all(TG_TOKEN not in str(a.data) for a in audits)


async def test_quiet_hours(notify_api: AsyncClient, db: AsyncSession) -> None:
    h, _ = await login(notify_api, db)
    url = "/api/v1/notify/quiet-hours"
    res = await notify_api.put(url, headers=h, json={"enabled": True, "start": "23:00", "end": "23:00"})
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {"end": "Giờ bắt đầu và kết thúc phải khác nhau."}
    res = await notify_api.put(url, headers=h, json={"enabled": True, "start": "7:00", "end": "24:00"})
    assert res.json()["error"]["details"]["fields"] == {
        "start": "Nhập giờ dạng HH:MM.",
        "end": "Nhập giờ dạng HH:MM.",
    }
    res = await notify_api.put(url, headers=h, json={"enabled": False, "start": "21:30", "end": "06:45"})
    assert res.status_code == 200
    assert res.json() == {"enabled": False, "start": "21:30", "end": "06:45"}
    res = await notify_api.get(URL, headers=h)
    assert res.json()["quiet_hours"] == {"enabled": False, "start": "21:30", "end": "06:45"}
    [entry] = await _audit(db, "NOTIFY_SETTINGS_UPDATE")
    assert entry.data == {
        "before": {"enabled": True, "start": "22:00", "end": "07:00"},
        "after": {"enabled": False, "start": "21:30", "end": "06:45"},
    }


async def test_messages_log_and_delete(notify_api: AsyncClient, db: AsyncSession) -> None:
    h, _ = await login(notify_api, db)
    kho = await make_channel(db, "Kho")
    cskh = await make_channel(db, "CSKH", events=["N04"])

    def msg(ch: NotifyChannel, code: str, status: str, age: timedelta, **kw: object) -> NotifyMessage:
        m = NotifyMessage(channel_id=ch.id, event_code=code, severity="HIGH", status=status,
                          items=[{"code": code}], item_count=1, text=f"[CAO] {code}",
                          created_at=NOW - age, send_after=NOW - age, **kw)  # fmt: skip
        db.add(m)
        return m

    msg(kho, "N02", "SENT", timedelta(hours=1), sent_at=NOW - timedelta(minutes=58))
    msg(kho, "N01", "QUEUED", timedelta(minutes=1))
    msg(
        cskh,
        "N04",
        "RETRYING",
        timedelta(hours=2),
        attempts=3,
        last_error="Người nhận chưa quan tâm OA của shop.",
    )
    msg(kho, "N03", "SENT", timedelta(days=31))  # ngoài 30 ngày
    summary = msg(kho, "N03", "HELD", timedelta(hours=3))
    summary.items = [{"code": "N03"}, {"code": "N09"}]
    await db.flush()

    res = await notify_api.get("/api/v1/notify/messages", headers=h)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["total"] == 4
    assert body["page"] == 1
    assert [i["event_code"] for i in body["items"]] == ["N01", "N02", "N04", "N03"]  # mới nhất trước
    assert body["items"][0]["channel"] == {"id": str(kho.id), "name": "Kho"}
    assert body["items"][1]["event_label"] == "Lệch trạng thái mức Cao"
    assert body["items"][3]["event_label"] == "Tóm tắt thông báo"
    res = await notify_api.get(f"/api/v1/notify/messages?channel_id={cskh.id}&status=RETRYING", headers=h)
    assert [i["last_error"] for i in res.json()["items"]] == ["Người nhận chưa quan tâm OA của shop."]

    res = await notify_api.delete(f"{URL}/{kho.id}", headers=h)
    assert res.status_code == 204
    assert await db.get(NotifyChannel, kho.id) is None
    left = await db.scalar(
        select(func.count()).select_from(NotifyMessage).where(NotifyMessage.channel_id == kho.id)
    )
    assert left == 0
    [entry] = await _audit(db, "NOTIFY_CHANNEL_DELETE")
    assert entry.data["dropped_messages"] == 2  # type: ignore[index]  # QUEUED + HELD
    res = await notify_api.delete(f"{URL}/{kho.id}", headers=h)
    assert res.status_code == 404


class _Crashing:
    type = "TELEGRAM"

    async def send(self, target: str, text: str) -> None:
        raise ValueError("lỗi lạ trong nhà cung cấp")


async def test_test_send_unexpected_provider_error_is_502(
    notify_api: AsyncClient, db: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-NT-3: nhà cung cấp ném lỗi lạ (không phải `SendError`) → API-174 502 `NOTIFY_SEND_FAILED`
    `provider_code` = tên lớp lỗi (không 500), kênh `ERROR`, có audit."""
    from aicam.modules.notify import providers

    monkeypatch.setattr(providers, "get_provider", lambda *_: _Crashing())
    h, _ = await login(notify_api, db)
    ch = await make_channel(db, "Kho", events=["N02"])
    res = await notify_api.post(f"{URL}/{ch.id}/test", headers=h)
    assert res.status_code == 502, res.text
    assert res.json()["error"]["details"]["provider_code"] == "ValueError"
    await db.refresh(ch)
    assert ch.last_status == "ERROR"
    assert [a.data["ok"] for a in await _audit(db, "NOTIFY_TEST")] == [False]  # type: ignore[index]
