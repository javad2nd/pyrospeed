from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class MediaConnectionConfig:
    """
    Optional media DC connection pool.

    This intentionally uses Pyrogram exposed/session factory hooks only.
    It never replaces Pyrogram's Client internals.
    """
    connections: int = 2


class MediaConnectionPool:
    """
    Lightweight pool for media operations.

    The pool is designed to be attached to Pyrogram-compatible clients.
    A connection factory can be supplied by different Pyrogram versions.
    """

    def __init__(self, factory: Any, config: MediaConnectionConfig | None = None):
        self.factory = factory
        self.config = config or MediaConnectionConfig()
        self._queue: asyncio.Queue[Any] = asyncio.Queue(self.config.connections)
        self._started = False

    async def start(self, dc_id: int):
        if self._started:
            return
        for _ in range(self.config.connections):
            session = await self.factory.create_media_session(dc_id)
            await self._queue.put(session)
        self._started = True

    async def acquire(self):
        return await self._queue.get()

    async def release(self, session):
        await self._queue.put(session)

    async def close(self):
        while not self._queue.empty():
            lease = await self._queue.get()
            await lease.close()
        self._started = False
