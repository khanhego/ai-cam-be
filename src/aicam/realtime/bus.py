"""Nghe kênh Redis trong tiến trình api và gọi handler (camera.health, sau này tray, ws…)."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from redis.asyncio import Redis

log = structlog.get_logger()

Handler = Callable[[dict[str, Any]], Awaitable[None]]


class Bus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = {}

    def on(self, channel: str, handler: Handler) -> None:
        self._handlers.setdefault(channel, []).append(handler)

    async def run(self, redis: Redis) -> None:
        if not self._handlers:
            return
        pubsub = redis.pubsub()
        await pubsub.subscribe(*self._handlers)
        try:
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                await self.dispatch(str(message["channel"]), str(message["data"]))
        finally:
            await pubsub.aclose()  # type: ignore[no-untyped-call]

    async def dispatch(self, channel: str, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("bus_bad_payload", channel=channel)
            return
        for handler in self._handlers.get(channel, []):
            try:
                await handler(payload)
            except Exception:  # một handler lỗi không được làm chết listener
                log.exception("bus_handler_failed", channel=channel)


def start(bus: Bus, redis: Redis) -> asyncio.Task[None]:
    return asyncio.create_task(bus.run(redis), name="aicam-bus")
