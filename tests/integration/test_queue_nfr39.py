"""T-276: hạ tầng queue (02a §7, §9; NFR-39, NFR-43; DEC-503, DEC-504).

1. Route Celery + worker trong **cả hai** compose: J-04 / J-05 / J-12 → `sync_fast` (`worker-sync -c 3`),
   J-06 / J-13 → `sync` (`worker-sync-long -c 2`), J-26..J-28 → `notify` (`worker-notify -c 1`); mọi queue có
   route đều có worker nghe; prefetch 1 + `acks_late`.
2. 6 shop, 1 shop **luôn timeout**: task thật (`dispatch.run_shop`, adapter mock) — 5 shop xong, shop chậm
   dừng ở ngân sách (không kéo dài theo độ trễ sàn).
3. Mô phỏng 1 giờ đồng hồ giả trên đúng số slot của compose + ngân sách thật: J-04 của 5 shop còn lại chạy mỗi
   chu kỳ (≤ 5 phút), không task nào chờ slot quá 1 chu kỳ của nó; cấu hình Phase 2 (một queue `sync -c 2`)
   thì vi phạm — chứng minh test bắt được hồi quy.
"""

import heapq
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from fnmatch import fnmatch
from itertools import pairwise
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import dispatch
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 7, 1, 0, tzinfo=UTC)
SHOPS = [f"99010{i}" for i in range(1, 7)]
SLOW = SHOPS[-1]


def _route(task: str) -> str:
    from aicam.workers.celery_app import app

    routes = app.conf.task_routes
    if task in routes:
        return str(routes[task]["queue"])
    for pattern, route in routes.items():
        if fnmatch(task, pattern):
            return str(route["queue"])
    return str(app.conf.task_default_queue)


def _workers(compose: str) -> dict[str, int]:
    """queue → tổng concurrency của các worker nghe queue đó (đọc `-Q … -c …` trong compose)."""
    text = (ROOT / "docker" / compose).read_text()
    out: dict[str, int] = {}
    for line in text.splitlines():
        if '"worker", "-Q"' not in line:
            continue
        queues = re.search(r'"-Q", "([^"]+)"', line)
        conc = re.search(r'"-c", "(\d+)"', line)
        assert queues is not None
        for q in queues.group(1).split(","):
            out[q] = out.get(q, 0) + (int(conc.group(1)) if conc else 4)
    return out


# ---------------------------------------------------------------- 1. route + worker


@pytest.mark.parametrize("compose", ["compose.dev.yml", "compose.yml"])
def test_routes_have_workers(compose: str) -> None:
    from aicam.workers.celery_app import app

    workers = _workers(compose)
    assert workers["sync_fast"] == 3
    assert workers["sync"] == 2
    assert workers["notify"] == 1
    assert workers["backup"] == 1
    assert workers["export"] == 1
    for name in ("platforms.sync_orders", "platforms.sync_shop_orders", "platforms.verify_unverified",
                 "platforms.refresh_tokens"):  # fmt: skip
        assert _route(name) == "sync_fast", name
    for name in ("platforms.sync_shipping_status", "platforms.sync_shop_shipping", "platforms.sync_returns",
                 "platforms.sync_shop_returns"):  # fmt: skip
        assert _route(name) == "sync", name
    for name in ("notify.scan", "notify.dispatch", "notify.daily_summary"):
        assert _route(name) == "notify", name
    beat_queues = {_route(e["task"]) for e in app.conf.beat_schedule.values()}
    assert beat_queues <= set(workers), f"queue không có worker: {beat_queues - set(workers)}"
    assert dispatch.QUEUES == {
        dispatch.ORDERS: "sync_fast",
        dispatch.SHIPPING: "sync",
        dispatch.RETURNS: "sync",
    }
    assert app.conf.worker_prefetch_multiplier == 1
    assert app.conf.task_acks_late is True


# ---------------------------------------------------------------- 2. task thật: 6 shop, 1 shop luôn timeout


def _order(sn: str, code: str) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=sn, status="READY_TO_SHIP", tracking_numbers=(code,),
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L"),), created_at=NOW, updated_at=NOW,
        status_group=order_group("READY_TO_SHIP"),
    )  # fmt: skip


async def test_six_shops_one_always_timeout_real_tasks(
    db: AsyncSession,
    test_settings: Settings,
    redis_client: object,
    sent_jobs: list[tuple[str, list[object], str, float]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = False
    test_settings.sync_task_budget_s = 0.4
    mock = MockAdapter()
    shops: dict[str, Shop] = {}
    for i, psid in enumerate(SHOPS):
        for n in range(1, 4):
            mock.put_for_shop(psid, _order(f"2410Q39{i}{n:04d}", f"SPXQ39{i}{n:07d}"))
        shop = Shop(platform="SHOPEE", platform_shop_id=psid, name=f"TST {psid}", auth_status="CONNECTED")
        platforms.store_credentials(
            shop, ShopCredentials(psid, f"acc-{psid}", f"ref-{psid}", NOW + timedelta(hours=4)),
            Cipher(test_settings.fernet_key),
        )  # fmt: skip
        db.add(shop)
        shops[psid] = shop
    await db.flush()
    mock.delay_s_by_shop = {SLOW: 0.3}  # 3 đơn × 0,3 giây > ngân sách 0,4 giây → luôn hết ngân sách
    monkeypatch.setattr(dispatch.registry, "adapter_for", lambda platform, settings: mock)

    out = await dispatch.dispatch(db, test_settings, dispatch.ORDERS)
    assert out["queued"] == 6
    assert {(task, queue) for task, _args, queue, _cd in sent_jobs} == {
        ("platforms.sync_shop_orders", "sync_fast")
    }

    durations: dict[str, float] = {}
    results: dict[str, dict[str, object]] = {}
    for psid, shop in shops.items():
        start = time.monotonic()
        res = await dispatch.run_shop(db, test_settings, dispatch.ORDERS, shop.id)
        durations[psid] = time.monotonic() - start
        results[psid] = res[str(shop.id)]
    for psid in SHOPS[:-1]:
        assert results[psid]["status"] == "OK", (psid, results[psid])
        assert results[psid]["orders"] == 3
    assert results[SLOW]["status"] == "FAILED"
    assert durations[SLOW] < test_settings.sync_task_budget_s + 0.5  # dừng ở ngân sách, không theo độ trễ sàn
    await db.refresh(shops[SLOW])
    assert shops[SLOW].last_sync_cursor is None  # lượt sau làm lại


# ---------------------------------------------------------------- 3. mô phỏng 1 giờ đồng hồ giả


@dataclass(order=True)
class _Job:
    enqueued: float
    seq: int
    kind: str = field(compare=False)
    shop: str | None = field(compare=False)
    duration: float = field(compare=False)
    queue: str = field(compare=False)


@dataclass
class _Stats:
    waits: dict[str, list[float]] = field(default_factory=dict)  # kind → thời gian chờ slot
    j04_starts: dict[str, list[float]] = field(default_factory=dict)  # shop → các lần J-04 bắt đầu


def _simulate(
    slots: dict[str, int], route: dict[str, str], settings: Settings, horizon: float = 3600.0
) -> _Stats:
    """Beat thật (chu kỳ 02a §7) + mỗi worker là `slots[queue]` tiến trình FIFO, prefetch 1."""
    normal = {"J04": 8.0, "J05": 20.0, "J12": 5.0, "J06": 40.0, "J13": 40.0}
    slow = {"J04": settings.sync_task_budget_s + 5, "J06": settings.sync_long_task_budget_s + 5,
            "J13": settings.sync_long_task_budget_s + 5}  # fmt: skip
    beats = {"J04": 300.0, "J05": 600.0, "J12": 1800.0, "J06": 900.0, "J13": 900.0}
    per_shop = {"J04", "J06", "J13"}
    pending: dict[str, list[_Job]] = {q: [] for q in slots}
    busy: dict[str, list[float]] = {q: [] for q in slots}  # heap thời điểm xong
    stats = _Stats()
    seq = 0
    t = 0.0
    while t <= horizon:
        for kind, period in beats.items():
            if t % period == 0:
                targets = SHOPS if kind in per_shop else [None]
                for shop in targets:
                    dur = slow[kind] if shop == SLOW and kind in slow else normal[kind]
                    seq += 1
                    q = route[kind]
                    heapq.heappush(pending[q], _Job(t, seq, kind, shop, dur, q))
        for q, n in slots.items():
            while busy[q] and busy[q][0] <= t:
                heapq.heappop(busy[q])
            while pending[q] and len(busy[q]) < n:
                job = heapq.heappop(pending[q])
                stats.waits.setdefault(job.kind, []).append(t - job.enqueued)
                if job.kind == "J04" and job.shop:
                    stats.j04_starts.setdefault(job.shop, []).append(t)
                heapq.heappush(busy[q], t + job.duration)
        t += 1.0
    return stats


def test_one_hour_fake_clock_six_shops_one_timeout(test_settings: Settings) -> None:
    """NFR-39: 1 giờ, 6 shop (1 shop luôn timeout ở mọi job) trên số slot của `compose.yml` (production)."""
    slots = {q: c for q, c in _workers("compose.yml").items() if q in ("sync_fast", "sync")}
    route = {
        "J04": _route("platforms.sync_shop_orders"),
        "J05": _route("platforms.verify_unverified"),
        "J12": _route("platforms.refresh_tokens"),
        "J06": _route("platforms.sync_shop_shipping"),
        "J13": _route("platforms.sync_shop_returns"),
    }
    stats = _simulate(slots, route, test_settings)
    for psid in SHOPS[:-1]:
        starts = stats.j04_starts[psid]
        for k in range(12):  # mỗi chu kỳ 5 phút trong giờ đều có một lượt J-04 bắt đầu trong chính chu kỳ đó
            assert any(300.0 * k <= x < 300.0 * (k + 1) for x in starts), (psid, k, starts)
        gaps = [b - a for a, b in pairwise(starts)]
        assert max(gaps) <= 300.0, (psid, gaps)  # J-04 mỗi chu kỳ ≤ 5 phút (NFR-38)
    assert max(stats.waits["J04"]) < 300.0  # không task J-04 nào chờ slot quá 1 chu kỳ
    assert max(stats.waits["J06"] + stats.waits["J13"]) < 900.0

    # Phase 2 (một queue `sync -c 2` cho mọi job sàn) → J-04 chờ sau J-06 / J-13 của shop chậm: vi phạm.
    old = _simulate({"sync": 2}, dict.fromkeys(route, "sync"), test_settings)
    assert max(old.waits["J04"]) >= 300.0 or any(
        max(b - a for a, b in pairwise(s)) > 300.0 for s in old.j04_starts.values()
    )
