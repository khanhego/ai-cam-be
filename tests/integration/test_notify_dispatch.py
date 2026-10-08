"""T-227: J-26 điều kiện N01..N09, J-27 BR-36 (bỏ trùng, gom 2 phút, trần 30 / giờ, giờ yên lặng, thử lại
24 giờ), J-28 N10, dọn 30 ngày (02a §7.5; FR-06.07..06.11; NFR-43; AC-54, AC-55).
Nhà cung cấp `mock` (server giả)."""

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.claims.models import Claim
from aicam.modules.notify import dispatch, summary
from aicam.modules.notify.models import NotifyChannel, NotifyEvent, NotifyMessage
from aicam.modules.orders.models import Package, Shop
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.reports import service as reports
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Camera, Station

from .notify_fixtures import make_channel, make_notify_settings, mock_sent
from .returns_helpers import make_order

pytestmark = pytest.mark.integration

# 14:00 giờ VN (ngoài giờ yên lặng 22:00–07:00).
DAY = datetime(2026, 10, 7, 7, 0, tzinfo=UTC)
TICK = timedelta(seconds=15)


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    clock.freeze(DAY)
    # Ổ đĩa máy test thật có thể > 80 % → cố định (N07 có test riêng).
    monkeypatch.setattr(
        reports, "disk_usage", lambda _s: {"total_bytes": 100, "used_bytes": 10, "percent": 10}
    )
    return


@pytest.fixture
def settings(tmp_path: Path, redis_client: object) -> Settings:
    (tmp_path / "video").mkdir(exist_ok=True)
    return make_notify_settings(tmp_path)


# ---------------------------------------------------------------- dữ liệu


async def station(db: AsyncSession, name: str = "Station 01") -> Station:
    st = Station(name=name, is_active=True)
    db.add(st)
    await db.flush()
    return st


async def camera(
    db: AsyncSession,
    st: Station,
    role: str = "CAM2",
    *,
    status: str = "OFFLINE",
    seen: datetime | None = None,
) -> Camera:
    cam = Camera(
        station_id=st.id,
        role=role,
        rtsp_url="rtsp://cam.test/x",
        mediamtx_path=f"cam-{uuid.uuid4()}",
        status=status,
        last_seen_at=seen,
    )
    db.add(cam)
    await db.flush()
    return cam


async def package(db: AsyncSession, code: str) -> Package:
    pkg = Package(tracking_number=code, warehouse_status="PACKED")
    db.add(pkg)
    await db.flush()
    return pkg


async def dropped_session(
    db: AsyncSession, st: Station, pkg: Package, *, status: str = "ABANDONED", **kw: object
) -> PackSession:
    s = PackSession(
        type="RETURN",
        package_id=pkg.id,
        station_id=st.id,
        status=status,
        started_at=clock.now() - timedelta(minutes=5),
        ended_at=clock.now(),
        open_code=pkg.tracking_number,
        **kw,
    )
    db.add(s)
    await db.flush()
    return s


async def recon_high(db: AsyncSession, pkg: Package) -> ReconAlert:
    a = ReconAlert(
        package_id=pkg.id,
        rule="RETURN_OVERDUE",
        severity="HIGH",
        status="OPEN",
        context={},
        context_key=f"k-{uuid.uuid4()}",
        detected_at=clock.now(),
        last_seen_at=clock.now(),
    )
    db.add(a)
    await db.flush()
    return a


async def approval(db: AsyncSession, st: Station, age: timedelta) -> ApprovalRequest:
    a = ApprovalRequest(
        station_id=st.id,
        tracking_number="SPXTSTAP001",
        type="ASSIST",
        status="PENDING",
        created_at=clock.now() - age,
    )
    db.add(a)
    await db.flush()
    return a


async def run_scan(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    return await dispatch.scan(db, settings)


async def run_dispatch(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    return await dispatch.dispatch(db, settings)


async def tick_until(db: AsyncSession, settings: Settings, until: datetime, *, scan: bool = True) -> None:
    """Beat giả: J-27 mỗi 15 giây, J-26 mỗi 30 giây (02a §7) tới `until`."""
    while clock.now() < until:
        clock.advance(TICK)
        if scan and int(clock.now().timestamp()) % 30 == 0:
            await run_scan(db, settings)
        await run_dispatch(db, settings)


async def events(db: AsyncSession, code: str | None = None) -> list[NotifyEvent]:
    q = select(NotifyEvent).order_by(NotifyEvent.dedupe_key)
    if code:
        q = q.where(NotifyEvent.code == code)
    return list((await db.scalars(q)).all())


async def messages(
    db: AsyncSession, channel: NotifyChannel, status: str | None = None
) -> list[NotifyMessage]:
    q = select(NotifyMessage).where(NotifyMessage.channel_id == channel.id).order_by(NotifyMessage.created_at)
    if status:
        q = q.where(NotifyMessage.status == status)
    return list((await db.scalars(q.execution_options(populate_existing=True))).all())


# ---------------------------------------------------------------- J-26 điều kiện


async def test_conditions_n01_to_n09_dedupe(db: AsyncSession, settings: Settings) -> None:
    now = clock.now()
    st = await station(db)
    cam = await camera(db, st, seen=now - timedelta(seconds=90))
    await camera(db, st, "CAM1", seen=now - timedelta(seconds=30))  # mới mất 30 giây → chưa
    await camera(db, await station(db, "Station 02"), status="ONLINE", seen=now - timedelta(hours=1))
    pkg = await package(db, "SPXTSTNT00001")
    alert = await recon_high(db, pkg)
    sess = await dropped_session(db, st, await package(db, "SPXTSTNT00002"))
    wrong = await dropped_session(
        db, st, await package(db, "SPXTSTNT00003"), status="CANCELLED", cancel_reason="WRONG_SCAN"
    )
    order, _ = await make_order(db, 9101)
    shop = Shop(
        platform="TIKTOK",
        platform_shop_id="TTN1",
        name="Áo Đẹp Official",
        auth_status="CONNECTED",
        error_since=now - timedelta(minutes=45),
        last_error={"code": "SYNC_FAILED", "message": "x", "at": clock.iso_z(now)},
    )
    expired = Shop(
        platform="SHOPEE",
        platform_shop_id="SPN1",
        name="Áo Đẹp",
        auth_status="EXPIRED",
        last_error={"code": "AUTH_EXPIRED", "message": "x", "at": "2026-10-07T06:00:00Z"},
    )
    db.add_all([shop, expired])
    await db.flush()
    refund = ReturnCase(
        order_id=order.id,
        kind="REFUND_ONLY",
        status="NO_PARCEL",
        source="PLATFORM",
        platform_status_group="REQUESTED",
        reported_at=now - timedelta(hours=1),
        seller_due_at=now + timedelta(hours=10),
        shop_id=shop.id,
        reason_text="Khách viết: giao thiếu, gọi 0912345678",
    )
    db.add(refund)
    claim_pkg = await package(db, "SPXTSTNT00004")
    soon = Claim(
        package_id=claim_pkg.id,
        type="DAMAGED",
        counterparty="PLATFORM",
        source="MANUAL",
        status="NEW",
        deadline_at=now + timedelta(hours=20),
    )
    late = Claim(
        package_id=claim_pkg.id,
        type="EMPTY_BOX",
        counterparty="PLATFORM",
        source="MANUAL",
        status="NEW",
        deadline_at=now - timedelta(hours=2),
    )
    db.add_all([soon, late])
    appr = await approval(db, st, timedelta(minutes=4))
    await approval(db, await station(db, "Station 03"), timedelta(minutes=1))  # chưa đủ 3 phút
    await db.flush()

    out = await run_scan(db, settings)
    assert out["failed"] == []
    keys = {(e.code, e.dedupe_key) for e in await events(db)}
    assert ("N01", f"cam:{cam.id}:{clock.iso_z(now - timedelta(seconds=90))}") in keys
    assert ("N02", f"alert:{alert.id}") in keys
    assert ("N03", f"sess:{sess.id}") in keys
    assert ("N03", f"sess:{wrong.id}") not in keys  # BR-39: hủy vì quét nhầm không tính N03
    assert ("N04", f"refund:{refund.id}:new") in keys
    assert ("N04", f"refund:{refund.id}:12h") in keys
    assert ("N05", f"claim:{soon.id}:soon:{clock.iso_z(soon.deadline_at)}") in keys  # type: ignore[arg-type]
    assert ("N05", f"claim:{late.id}:overdue:{clock.iso_z(late.deadline_at)}") in keys  # type: ignore[arg-type]
    assert ("N06", f"shop:{shop.id}:err:{clock.iso_z(now - timedelta(minutes=45))}") in keys
    assert ("N06", f"shop:{expired.id}:expired:2026-10-07T06:00:00Z") in keys
    assert ("N09", f"appr:{appr.id}") in keys
    assert len([k for k in keys if k[0] == "N01"]) == 1
    assert len([k for k in keys if k[0] == "N09"]) == 1
    n04 = (await events(db, "N04"))[0]
    assert "reason_text" not in n04.data
    assert "0912345678" not in str(n04.data)

    # BR-36 (1): chạy lại → không sự kiện mới
    total = len(keys)
    out = await run_scan(db, settings)
    assert out["new"] == 0
    assert len(await events(db)) == total


async def test_n07_disk_levels(db: AsyncSession, settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        reports, "disk_usage", lambda _s: {"total_bytes": 100, "used_bytes": 85, "percent": 85}
    )
    await run_scan(db, settings)
    monkeypatch.setattr(
        reports, "disk_usage", lambda _s: {"total_bytes": 100, "used_bytes": 93, "percent": 93}
    )
    await run_scan(db, settings)
    await run_scan(db, settings)
    got = [(e.severity, e.dedupe_key) for e in await events(db, "N07")]
    assert got == [("MEDIUM", "disk:80:2026-10-07"), ("HIGH", "disk:90:2026-10-07")]


# ---------------------------------------------------------------- J-27 gửi + mẫu tin (AC-54)


async def test_routing_template_no_pii(db: AsyncSession, settings: Settings) -> None:
    kho = await make_channel(db, "Kho", target="-1001", events=["N01", "N02", "N03", "N09"])
    cskh = await make_channel(db, "CSKH", target="-1002", events=["N04", "N05"])
    await make_channel(db, "Tắt", target="-1003", events=["N02"], enabled=False)
    st = await station(db)
    order, packages = await make_order(db, 9102)
    shop = Shop(platform="SHOPEE", platform_shop_id="SPN2", name="Áo Đẹp", auth_status="CONNECTED")
    db.add(shop)
    await db.flush()
    order.shop_id = shop.id
    await recon_high(db, packages[0])
    db.add(
        ReturnCase(
            order_id=order.id,
            kind="REFUND_ONLY",
            status="NO_PARCEL",
            source="PLATFORM",
            platform_status_group="REQUESTED",
            reported_at=clock.now(),
            shop_id=shop.id,
            seller_due_at=datetime(2026, 10, 8, 10, 0, tzinfo=UTC),
            reason_text="Nguyễn Văn A, 0912345678, ghi chú: hàng lỗi",
        )
    )
    await db.flush()

    await run_scan(db, settings)
    await run_dispatch(db, settings)
    assert await mock_sent() == []  # gom 2 phút (BR-36 (2))
    await tick_until(db, settings, DAY + timedelta(minutes=2, seconds=30), scan=False)
    sent = await mock_sent()
    by_target = {s["target"]: s["text"] for s in sent}
    assert set(by_target) == {"-1001", "-1002"}  # kênh tắt không nhận
    assert by_target["-1001"] == (
        "[CAO] Lệch trạng thái mức Cao\n• SPXTST0009102 · Shopee · Áo Đẹp · từ 07/10\n"
        "Xem: https://x.local/admin/recon?severity=HIGH"
    )
    case_code = await db.scalar(select(ReturnCase.code).where(ReturnCase.order_id == order.id))
    assert by_target["-1002"] == (
        "[CAO] Chỉ hoàn tiền mới / sắp hạn\n"
        f"• {case_code} · Shopee · Áo Đẹp"
        " · hạn 08/10 17:00 · mới\nXem: https://x.local/admin/returns?tab=NO_PARCEL&pending_only=true"
    )
    for text in by_target.values():
        assert "0912345678" not in text
        assert "Nguyễn Văn A" not in text
    [msg] = await messages(db, kho, "SENT")
    assert msg.item_count == 1
    assert msg.attempts == 1
    await db.refresh(kho)
    assert kho.last_status == "OK"
    assert len(await messages(db, cskh, "SENT")) == 1
    assert st is not None


async def test_open_alert_rerun_three_times_one_message(db: AsyncSession, settings: Settings) -> None:
    kho = await make_channel(db, "Kho", events=["N02"])
    await recon_high(db, await package(db, "SPXTSTNT00010"))
    for _ in range(3):
        await run_scan(db, settings)
        await run_dispatch(db, settings)
        clock.advance(timedelta(minutes=5))
        await run_dispatch(db, settings)
    assert len(await messages(db, kho)) == 1
    assert len(await mock_sent()) == 1


async def test_storm_8_cameras_one_message(db: AsyncSession, settings: Settings) -> None:
    """EX-N3: 8 camera rớt 14:00:05–14:00:40 → 1 tin "Camera mất tín hiệu — 8 mục" (AC-54)."""
    kho = await make_channel(db, "Kho", events=["N01"])
    stations = [await station(db, f"Station {n:02d}") for n in range(1, 5)]
    for i in range(8):
        st = stations[i // 2]
        await camera(db, st, "CAM1" if i % 2 else "CAM2", seen=DAY + timedelta(seconds=5 + i * 5))
    await tick_until(db, settings, DAY + timedelta(minutes=6))
    sent = await mock_sent()
    assert len(sent) == 1
    lines = sent[0]["text"].split("\n")
    assert lines[0] == "[CAO] Camera mất tín hiệu — 8 mục"
    assert "• Station 01 · Cam 2 · từ 14:00" in lines
    assert len(lines) == 10
    assert lines[-1] == "Xem: https://x.local/admin/live"
    [msg] = await messages(db, kho)
    assert msg.item_count == 8


async def test_n01_recovered_before_send(db: AsyncSession, settings: Settings) -> None:
    """EX-N4: camera có lại trong cửa sổ gom → vẫn gửi, dòng ghi "(đã có lại HH:MM)"."""
    await make_channel(db, "Kho", events=["N01"])
    st = await station(db)
    cam = await camera(db, st, seen=DAY - timedelta(seconds=70))
    await run_scan(db, settings)
    await run_dispatch(db, settings)
    clock.advance(timedelta(minutes=1))
    cam.status, cam.last_seen_at = "ONLINE", clock.now()  # 14:01 VN
    await db.flush()
    await tick_until(db, settings, DAY + timedelta(minutes=3), scan=False)
    [sent] = await mock_sent()
    assert "• Station 01 · Cam 2 · từ 13:58 (đã có lại 14:01)" in sent["text"]


async def test_rate_limit_40_per_hour(db: AsyncSession, settings: Settings) -> None:
    """BR-36 (3): 40 tin đến hạn trong 1 giờ → 30 tin + 1 tin tóm tắt (≤ 30 tin trong mọi cửa sổ 60 phút)."""
    kho = await make_channel(db, "Kho", events=["N02", "N03"])
    for i in range(40):
        code = "N02" if i % 2 else "N03"
        db.add(
            NotifyMessage(
                channel_id=kho.id,
                event_code=code,
                severity="HIGH",
                status="QUEUED",
                items=[
                    {
                        "code": code,
                        "severity": "HIGH",
                        "data": {
                            "tracking": f"SPXTSTRL{i:05d}",
                            "station": "Station 01",
                            "kind": "SESSION",
                            "status": "ABANDONED",
                            "at": clock.iso_z(DAY),
                        },
                    }
                ],
                item_count=1,
                created_at=DAY + timedelta(minutes=i),
                send_after=DAY + timedelta(minutes=i),
            )
        )
    await db.flush()
    await run_dispatch(db, settings)
    await tick_until(db, settings, DAY + timedelta(minutes=75), scan=False)
    sent = await messages(db, kho, "SENT")
    assert len(sent) == 31
    times = sorted(m.sent_at for m in sent if m.sent_at)
    for i, t in enumerate(times):
        assert len([x for x in times[i:] if x < t + timedelta(minutes=60)]) <= 30
    summary_msg = sent[-1]
    assert summary_msg.item_count == 10
    assert summary_msg.text is not None
    assert summary_msg.text.startswith("[CAO] Tóm tắt 10 thông báo\n")
    assert len(await messages(db, kho, "SKIPPED")) == 10
    assert len(await mock_sent()) == 31


async def test_quiet_hours_hold_until_7am_high_sent(db: AsyncSession, settings: Settings) -> None:
    """BR-36 (4): 23:10 sự kiện TB → gom lúc 07:00; sự kiện Cao gửi ngay (AC-54)."""
    clock.freeze(datetime(2026, 10, 7, 16, 10, tzinfo=UTC))  # 23:10 VN
    kho = await make_channel(db, "Kho", events=["N01", "N02", "N03", "N09"])
    st = await station(db)
    await camera(db, st, seen=clock.now() - timedelta(minutes=5))  # N01 không tạo trong giờ yên lặng
    await recon_high(db, await package(db, "SPXTSTQH00001"))
    await dropped_session(db, st, await package(db, "SPXTSTQH00002"))
    await approval(db, st, timedelta(minutes=10))
    await run_scan(db, settings)
    assert await events(db, "N01") == []
    await tick_until(db, settings, clock.now() + timedelta(minutes=3), scan=False)
    sent = await mock_sent()
    assert [s["text"].split("\n")[0] for s in sent] == ["[CAO] Lệch trạng thái mức Cao"]
    held = await messages(db, kho, "HELD")
    assert {m.event_code for m in held} == {"N03", "N09"}
    assert {m.send_after for m in held} == {datetime(2026, 10, 8, 0, 0, tzinfo=UTC)}  # 07:00 VN
    clock.freeze(datetime(2026, 10, 8, 0, 0, tzinfo=UTC))
    await run_dispatch(db, settings)
    sent = await mock_sent()
    assert len(sent) == 2
    assert sent[1]["text"].startswith(
        "[TB] Tóm tắt 2 thông báo\n• Phiên mở hoàn bị hủy / bỏ dở: SPXTSTQH00002"
    )
    assert "Yêu cầu duyệt chờ lâu: Station 01 · Gọi quản lý · từ 23:00" in sent[1]["text"]


async def test_outage_2h_resent_then_24h_dropped(db: AsyncSession, settings: Settings) -> None:
    """NFR-43 / EX-N2: mất mạng 2 giờ → gửi bù, không mất; lỗi suốt 24 giờ → "Bị bỏ" (AC-54, AC-55)."""
    kho = await make_channel(db, "Kho", events=["N02"])
    await recon_high(db, await package(db, "SPXTSTOT00001"))
    settings.notify_mock_fail = "TELEGRAM"
    await run_scan(db, settings)
    await tick_until(db, settings, DAY + timedelta(hours=2), scan=False)
    [msg] = await messages(db, kho)
    assert msg.status == "RETRYING"
    assert msg.attempts >= 6
    await db.refresh(kho)
    assert kho.last_status == "ERROR"
    assert kho.last_error is not None
    assert kho.last_error["code"] == "NOTIFY_SEND_FAILED"
    settings.notify_mock_fail = ""
    await tick_until(db, settings, DAY + timedelta(hours=3, minutes=5), scan=False)
    [msg] = await messages(db, kho)
    assert msg.status == "SENT"
    assert len(await mock_sent()) == 1
    await db.refresh(kho)
    assert kho.last_status == "OK"

    # tin thứ hai lỗi suốt 24 giờ → DROPPED
    clock.advance(timedelta(minutes=10))
    await recon_high(db, await package(db, "SPXTSTOT00002"))
    settings.notify_mock_fail = "TELEGRAM"
    await run_scan(db, settings)
    start = clock.now()
    step = timedelta(minutes=5)
    while clock.now() < start + timedelta(hours=24, minutes=10):
        clock.advance(step)
        await run_dispatch(db, settings)
    msgs = await messages(db, kho)
    assert [m.status for m in msgs] == ["SENT", "DROPPED"]
    assert msgs[1].last_error == "Lỗi giả lập (TELEGRAM) — NOTIFY_MOCK_FAIL."
    assert msgs[1].attempts >= 25


async def test_disabled_channel_drops_pending(db: AsyncSession, settings: Settings) -> None:
    kho = await make_channel(db, "Kho", events=["N02"])
    await recon_high(db, await package(db, "SPXTSTDC00001"))
    await run_scan(db, settings)
    await run_dispatch(db, settings)
    kho.enabled = False
    await db.flush()
    await tick_until(db, settings, DAY + timedelta(minutes=3), scan=False)
    [msg] = await messages(db, kho)
    assert msg.status == "DROPPED"
    assert msg.last_error == "Kênh đã tắt — tin bị bỏ."
    assert await mock_sent() == []


async def test_notify_disabled_skips(db: AsyncSession, settings: Settings) -> None:
    settings.notify_enabled = False
    assert await run_scan(db, settings) == {"skipped": "disabled"}
    assert await run_dispatch(db, settings) == {"skipped": "disabled"}


async def test_nfr43_event_to_message_under_3_minutes(db: AsyncSession, settings: Settings) -> None:
    """NFR-43: điều kiện thành đúng → tin tới kênh ≤ 3 phút (J-26 30 giây + gom 2 phút + J-27 15 giây)."""
    await make_channel(db, "Kho", events=["N09"])
    st = await station(db)
    # Yêu cầu duyệt tạo 14:00:01 → điều kiện N09 (chờ > 3 phút) đúng từ 14:03:01.
    db.add(
        ApprovalRequest(
            station_id=st.id,
            tracking_number="SPXTSTLT001",
            type="MISMATCH",
            status="PENDING",
            created_at=DAY + timedelta(seconds=1),
        )
    )
    await db.flush()
    true_at = DAY + timedelta(minutes=3, seconds=1)
    await tick_until(db, settings, DAY + timedelta(minutes=8))
    [sent] = await mock_sent()
    sent_at = datetime.fromisoformat(sent["at"].replace("Z", "+00:00"))
    assert sent_at - true_at <= timedelta(minutes=3)


# ---------------------------------------------------------------- N08 (5 lý do) — sao lưu ON


async def test_n08_five_reasons_one_line_each(db: AsyncSession, world: Any) -> None:
    from aicam.modules.backup.models import BackupObject, BackupRun

    settings = world.settings.model_copy(update={"notify_transport": "mock", "site_address": "x.local"})
    qt = await make_channel(db, "Quản trị", events=["N08"])
    now = clock.now()
    runs = [
        BackupRun(
            kind="DB",
            trigger="SCHEDULE",
            status=s,
            started_at=now - timedelta(hours=h),
            finished_at=now - timedelta(hours=h) + timedelta(minutes=1),
        )
        for s, h in (("SUCCESS", 30), ("FAILED", 12), ("FAILED", 6), ("FAILED", 1))
    ]
    db.add_all(runs)
    clip_a, clip_b = world.clips["CAM1"], world.clips["CAM2"]
    db.add_all(
        [
            BackupObject(
                kind="CLIP",
                clip_id=clip_a.id,
                object_key="backup/evidence/clips/a.enc",
                status="HASH_MISMATCH",
                reason="EVIDENCE",
                created_at=now - timedelta(hours=30),
            ),
            BackupObject(
                kind="CLIP",
                clip_id=clip_b.id,
                object_key="backup/evidence/clips/b.enc",
                status="FAILED",
                last_error="SOURCE_MISSING",
                reason="EVIDENCE",
                attempts=1,
            ),
            BackupObject(
                kind="SNAPSHOT",
                snapshot_id=world.snapshot.id,
                object_key="backup/evidence/s/c.enc",
                status="PENDING",
                reason="EVIDENCE",
                created_at=now - timedelta(hours=25),
            ),
        ]
    )
    await db.flush()
    await dispatch.scan(db, settings)
    keys = sorted(e.dedupe_key for e in await events(db, "N08"))
    assert keys == sorted(
        [
            f"backup:db:{clock.iso_z(runs[0].finished_at)}",  # type: ignore[arg-type]
            f"backup:db2:{runs[2].id}",  # lượt lỗi thứ 2 của chuỗi (DEC-500)
            "backup:ev:2026-10-07",
            *[k for k in keys if k.startswith("backup:hash:")],
            *[k for k in keys if k.startswith("backup:srcmiss:")],
        ]
    )
    assert len([k for k in keys if k.startswith(("backup:hash:", "backup:srcmiss:"))]) == 2
    await tick_until(db, settings, clock.now() + timedelta(minutes=3), scan=False)
    [sent] = await mock_sent()
    assert sent["text"].split("\n") == [
        "[CAO] Sao lưu cloud trễ / lỗi — 5 mục",
        "• Sao lưu DB chưa thành công 30 giờ",
        "• 3 lượt sao lưu DB liền không thành công",
        "• 1 tệp bằng chứng chờ sao lưu quá 24 giờ",
        "• 1 tệp lệch mã băm",
        "• 1 tệp không thấy tại kho",
        "Xem: https://x.local/admin/settings/backup",
    ]
    assert len(await messages(db, qt, "SENT")) == 1
    # Lượt lỗi thứ 4 liền: cùng `backup:db2:` (lượt lỗi thứ 2 không đổi) → không tin mới.
    db.add(BackupRun(kind="DB", trigger="SCHEDULE", status="FAILED", started_at=now - timedelta(minutes=10)))
    await db.flush()
    out = await dispatch.scan(db, settings)
    assert out["new"] == 0


# ---------------------------------------------------------------- J-28, J-11


async def test_daily_summary_matches_d2(db: AsyncSession, settings: Settings) -> None:
    """FR-06.11 / AC-55: tóm tắt 18:00 đúng số D2 (API-32) hôm nay; một lần / ngày."""
    owner = await make_channel(db, "Chủ shop", target="777", events=["N10"])
    st = await station(db)
    pkg = await package(db, "SPXTSTDS00001")
    db.add(
        PackSession(
            type="PACK",
            package_id=pkg.id,
            station_id=st.id,
            status="COMPLETED",
            started_at=DAY,
            ended_at=DAY + timedelta(minutes=1),
            open_code=pkg.tracking_number,
            flags=["HAD_MISMATCH"],
        )
    )
    db.add(
        Claim(
            package_id=pkg.id,
            type="DAMAGED",
            counterparty="PLATFORM",
            source="MANUAL",
            status="NEW",
            deadline_at=DAY - timedelta(hours=1),
        )
    )
    await db.flush()
    clock.freeze(datetime(2026, 10, 7, 11, 0, tzinfo=UTC))  # 18:00 VN
    out = await summary.daily_summary(db, settings)
    assert out == {"new": 1, "dedupe_key": "summary:2026-10-07"}
    assert (await summary.daily_summary(db, settings))["new"] == 0
    await tick_until(db, settings, clock.now() + timedelta(minutes=3), scan=False)
    [sent] = await mock_sent()
    d2 = (await reports.daily(db, None, settings)).counts
    assert sent["target"] == "777"
    assert sent["text"].split("\n") == [
        "[TIN] Tóm tắt ngày 07/10",
        f"• Đã đóng gói: {d2.packed} · từng lệch mã: {d2.had_mismatch}",
        f"• Hàng hoàn nhận: {d2.returns_received} · có vấn đề: {d2.returns_received_issue}",
        f"• Hồ sơ khiếu nại mở: {d2.claims_open} · sắp hạn: {d2.claims_due_soon} · "
        f"quá hạn chưa gửi: {d2.claims_overdue_unsent}",
        f"• Chỉ hoàn tiền chưa xử lý: {d2.refund_only_pending}",
        "Xem: https://x.local/admin",
    ]
    assert d2.packed == 1
    assert d2.had_mismatch == 1
    assert d2.claims_overdue_unsent == 1
    assert len(await messages(db, owner, "SENT")) == 1


async def test_purge_30_days(db: AsyncSession, settings: Settings) -> None:
    kho = await make_channel(db, "Kho", events=["N02"])
    old = DAY - timedelta(days=31)
    db.add_all(
        [
            NotifyMessage(
                channel_id=kho.id,
                event_code="N02",
                severity="HIGH",
                status="SENT",
                items=[],
                created_at=old,
                send_after=old,
            ),
            NotifyMessage(
                channel_id=kho.id,
                event_code="N02",
                severity="HIGH",
                status="SENT",
                items=[],
                created_at=DAY,
                send_after=DAY,
            ),
            NotifyEvent(
                code="N02", severity="HIGH", dedupe_key="alert:old", occurred_at=old, processed_at=old
            ),
            NotifyEvent(
                code="N02", severity="HIGH", dedupe_key="alert:new", occurred_at=DAY, processed_at=DAY
            ),
            NotifyEvent(code="N02", severity="HIGH", dedupe_key="alert:pending", occurred_at=old),
        ]
    )
    await db.flush()
    assert await dispatch.purge(db) == {"notify_messages": 1, "notify_events": 1}
    left = await db.scalar(select(func.count()).select_from(NotifyEvent))
    assert left == 2


async def test_send_one_unexpected_provider_error_retries(
    db: AsyncSession, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-NT-3: lỗi lạ của nhà cung cấp trong J-27 → tin `RETRYING` (không văng khỏi lượt, không kẹt)."""
    from aicam.modules.notify import providers

    class Crashing:
        type = "TELEGRAM"

        async def send(self, target: str, text: str) -> None:
            raise KeyError("x")

    kho = await make_channel(db, "Kho", events=["N02"])
    await recon_high(db, await package(db, "SPXTSTNT30001"))
    await run_scan(db, settings)
    monkeypatch.setattr(providers, "get_provider", lambda *_: Crashing())
    await tick_until(db, settings, clock.now() + timedelta(minutes=3), scan=False)
    [msg] = await messages(db, kho)
    assert msg.status == "RETRYING"
    await db.refresh(kho)
    assert kho.last_error is not None
    assert kho.last_error["provider_code"] == "KeyError"


async def test_send_one_holds_no_db_transaction_while_sending(
    db: AsyncSession, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-NT-2: J-27 không giữ transaction (khóa dòng tin / kênh) trong lúc gọi nhà cung cấp; kênh bị xóa
    trong lúc gửi → bỏ qua, không lỗi."""
    from aicam.modules.notify import providers

    seen: list[bool] = []

    class Probe:
        type = "TELEGRAM"

        async def send(self, target: str, text: str) -> None:
            seen.append(db.in_transaction())

    kho = await make_channel(db, "Kho", events=["N02"])
    await recon_high(db, await package(db, "SPXTSTNT20001"))
    await run_scan(db, settings)
    monkeypatch.setattr(providers, "get_provider", lambda *_: Probe())
    await tick_until(db, settings, clock.now() + timedelta(minutes=3), scan=False)
    assert seen == [False]
    [msg] = await messages(db, kho)
    assert msg.status == "SENT"
