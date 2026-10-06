"""Vòng đọc khay Cam 2 trong tiến trình `vision` (T-12; FR-03.06, 03.07; 02a §7 Vision).

- Nạp Cam 2 của station đang bật từ DB; mỗi camera một `CameraReader` (thread).
- Nhận `Observation` → khử nhiễu → ghi Redis `tray:{station_id}` (TTL 5 giây, làm mới mỗi khung).
- Tập mã đổi / mất stream > 3 giây → phát `tray.changed`; api gọi `on_tray_changed` (BR-06).
- Nạp lại danh sách camera / ROI khi có `vision.config` (API-64) và định kỳ 10 giây (camera mới, station tắt).
- T-121 (DEC-320): mọi camera (Cam 1 chỉ lấy khung, Cam 2 thêm đọc mã) đẩy JPEG khung mới nhất ~1 lần / giây →
  `FrameWriter` ghi Redis `frame:{camera_id}` (TTL 5 giây) cho API-103 / ảnh lúc đóng gói.
"""

import asyncio
import contextlib
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

import structlog
from redis.asyncio import Redis

from aicam.core import clock
from aicam.core.db import sessionmaker
from aicam.modules.media import frames
from aicam.modules.sessions.tray import announce_tray_changed, clear_tray, write_tray
from aicam.modules.stations import service as stations
from aicam.modules.vision.capture import FRAME_INTERVAL_S, JPEG_QUALITY, CameraReader, Observation
from aicam.modules.vision.reader import Roi
from aicam.modules.vision.tray import LOST_AFTER_S, TrayDebouncer

log = structlog.get_logger()

RELOAD_EVERY_S = 10.0
WATCHDOG_EVERY_S = 1.0


@dataclass(frozen=True)
class Target:
    camera_id: uuid.UUID
    station_id: uuid.UUID
    url: str
    roi: Roi | None
    role: str = "CAM2"


@dataclass(frozen=True)
class FrameOptions:
    enabled: bool = True
    interval_s: float = FRAME_INTERVAL_S
    jpeg_quality: int = JPEG_QUALITY


class FrameWriter:
    """Nhận JPEG từ thread đọc camera, giữ khung mới nhất mỗi camera, ghi Redis ngay (không chờ vòng khay)."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._pending: dict[uuid.UUID, tuple[bytes, datetime]] = {}
        self._wake = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        self.written = 0

    def offer(self, camera_id: uuid.UUID, jpeg: bytes, taken_at: datetime) -> None:
        """Gọi từ thread: thay khung chờ ghi của camera (chỉ giữ khung mới nhất)."""

        def _set() -> None:
            self._pending[camera_id] = (jpeg, taken_at)
            self._wake.set()

        self._loop.call_soon_threadsafe(_set)

    async def flush(self) -> int:
        batch, self._pending = self._pending, {}
        for camera_id, (jpeg, taken_at) in batch.items():
            await frames.store(self._redis, camera_id, jpeg, taken_at)
        self.written += len(batch)
        return len(batch)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=1.0)
            except TimeoutError:
                continue
            self._wake.clear()
            try:
                await self.flush()
            except Exception:  # Redis chập chờn: bỏ lượt, khung sau ghi lại
                log.exception("vision_frame_store_failed")


@dataclass
class _CameraState:
    station_id: uuid.UUID
    debouncer: TrayDebouncer = field(default_factory=TrayDebouncer)
    last_ok: float = field(default_factory=time.monotonic)
    updated_at: datetime = field(default_factory=clock.now)
    frames: int = 0
    decoded: int = 0


class TrayTracker:
    """Phần async không chứa thread: test được bằng cách đưa `Observation` vào tay."""

    def __init__(self, redis: Redis, *, lost_after_s: float = LOST_AFTER_S) -> None:
        self._redis = redis
        self._lost_after = lost_after_s
        self.cameras: dict[uuid.UUID, _CameraState] = {}

    def track(self, camera_id: uuid.UUID, station_id: uuid.UUID) -> None:
        self.cameras.setdefault(camera_id, _CameraState(station_id))

    async def forget(self, camera_id: uuid.UUID) -> None:
        state = self.cameras.pop(camera_id, None)
        if state is not None and state.debouncer.stable is not None:
            await clear_tray(self._redis, state.station_id)
            await announce_tray_changed(self._redis, state.station_id)

    async def handle(self, obs: Observation) -> None:
        state = self.cameras.get(obs.camera_id)
        if state is None:
            return
        if obs.codes is None:
            await self._check_lost(state, obs.at)
            return
        state.last_ok = obs.at
        state.frames += 1
        state.decoded += bool(obs.codes)
        changed = state.debouncer.observe(obs.codes)
        stable = state.debouncer.stable
        if stable is None:
            return
        if changed:
            state.updated_at = clock.now()  # updated_at = lúc đổi trạng thái (02 API-10 `tray.updated_at`)
        # Ghi lại mỗi khung (không chỉ EXPIRE): khóa bị xóa ngoài ý muốn vẫn có lại ngay khung sau.
        await write_tray(self._redis, state.station_id, stable, state.updated_at)
        if changed:
            await announce_tray_changed(self._redis, state.station_id)
            log.info("tray_changed", station_id=str(state.station_id), codes=list(stable))

    async def tick(self, now: float | None = None) -> None:
        """Watchdog: thread có thể treo khi camera rớt — không chờ thread báo."""
        now = time.monotonic() if now is None else now
        for state in list(self.cameras.values()):
            await self._check_lost(state, now)

    async def _check_lost(self, state: _CameraState, now: float) -> None:
        if now - state.last_ok <= self._lost_after:
            return
        if state.debouncer.lose():
            await clear_tray(self._redis, state.station_id)
            await announce_tray_changed(self._redis, state.station_id)
            log.info("tray_unavailable", station_id=str(state.station_id))


ReaderFactory = Callable[[Target, Callable[[Observation], None]], CameraReader]


async def load_targets(rtsp_base: str) -> list[Target]:
    async with sessionmaker()() as session:
        cameras = await stations.vision_cameras(session)
    return [
        Target(c.id, c.station_id, f"{rtsp_base.rstrip('/')}/{c.mediamtx_path}", Roi.from_json(c.roi), c.role)
        for c in cameras
    ]


class TrayRunner:
    """Giữ tập thread khớp với DB; chuyển Observation từ thread sang asyncio."""

    def __init__(
        self,
        redis: Redis,
        code_pattern: re.Pattern[str],
        reader_factory: ReaderFactory | None = None,
        *,
        frame_options: FrameOptions | None = None,
    ):
        self.tracker = TrayTracker(redis)
        self._pattern = code_pattern
        self._factory = reader_factory or self._default_reader
        self.frame_options = frame_options or FrameOptions()
        self.frames = FrameWriter(redis)
        self._readers: dict[uuid.UUID, CameraReader] = {}
        self._targets: dict[uuid.UUID, Target] = {}
        self.queue: asyncio.Queue[Observation] = asyncio.Queue(maxsize=1000)
        self._loop = asyncio.get_running_loop()

    def _emit(self, obs: Observation) -> None:
        """Gọi từ thread đọc camera."""

        def _put() -> None:
            with contextlib.suppress(asyncio.QueueFull):  # api chậm: bỏ khung, không chặn thread
                self.queue.put_nowait(obs)

        self._loop.call_soon_threadsafe(_put)

    def _default_reader(self, target: Target, emit: Callable[[Observation], None]) -> CameraReader:
        """Cam 2: đọc mã khay + khung; Cam 1: chỉ khung (T-121). Tắt khung → chỉ Cam 2 như Phase 1."""
        opts = self.frame_options
        return CameraReader(
            target.camera_id, target.url, target.roi if target.role == "CAM2" else None,
            self._pattern if target.role == "CAM2" else None, emit,
            on_frame=self.frames.offer if opts.enabled else None, frame_interval_s=opts.interval_s,
            jpeg_quality=opts.jpeg_quality,
        )  # fmt: skip

    async def apply(self, targets: list[Target]) -> None:
        if not self.frame_options.enabled:
            targets = [t for t in targets if t.role == "CAM2"]
        wanted = {t.camera_id: t for t in targets}
        for camera_id in set(self._readers) - set(wanted):
            self._readers.pop(camera_id).stop()
            self._targets.pop(camera_id, None)
            await self.tracker.forget(camera_id)
            log.info("vision_camera_removed", camera_id=str(camera_id))
        for camera_id, target in wanted.items():
            current = self._targets.get(camera_id)
            if (
                current is not None
                and current.url == target.url
                and current.station_id == target.station_id
                and current.role == target.role
            ):
                if current.roi != target.roi:
                    self._readers[camera_id].roi = target.roi  # đổi ROI không cần mở lại stream
                    log.info("vision_roi_reloaded", camera_id=str(camera_id))
                self._targets[camera_id] = target
                continue
            if current is not None:
                self._readers.pop(camera_id).stop()
                await self.tracker.forget(camera_id)
            reader = self._factory(target, self._emit)
            self._readers[camera_id] = reader
            self._targets[camera_id] = target
            if target.role == "CAM2":
                self.tracker.track(camera_id, target.station_id)
            reader.start()
            log.info("vision_camera_started", camera_id=str(camera_id), station_id=str(target.station_id))

    def stop(self) -> None:
        for reader in self._readers.values():
            reader.stop()


async def run_tray_loop(
    redis: Redis,
    rtsp_base: str,
    code_pattern: re.Pattern[str],
    stop: asyncio.Event,
    reload: asyncio.Event,
    frame_options: FrameOptions | None = None,
) -> None:
    runner = TrayRunner(redis, code_pattern, frame_options=frame_options)
    writer = asyncio.create_task(runner.frames.run(stop), name="vision-frames")
    next_reload = 0.0
    next_tick = time.monotonic() + WATCHDOG_EVERY_S
    try:
        while not stop.is_set():
            now = time.monotonic()
            if reload.is_set() or now >= next_reload:
                reload.clear()
                try:
                    await runner.apply(await load_targets(rtsp_base))
                except Exception:
                    log.exception("vision_reload_failed")
                next_reload = now + RELOAD_EVERY_S
            if now >= next_tick:
                await runner.tracker.tick(now)
                next_tick = now + WATCHDOG_EVERY_S
            try:
                obs = await asyncio.wait_for(runner.queue.get(), timeout=WATCHDOG_EVERY_S)
            except TimeoutError:
                continue
            try:
                await runner.tracker.handle(obs)
            except Exception:  # Redis chập chờn: bỏ khung này, vòng lặp sống tiếp
                log.exception("vision_handle_failed", camera_id=str(obs.camera_id))
    finally:
        writer.cancel()
        runner.stop()
