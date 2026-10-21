from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class MediaConnectionConfig:
    """Optional media connection pool settings."""
    connections: int = 2

    def __post_init__(self) -> None:
        if self.connections < 1:
            raise ValueError("connections must be >= 1")


class MediaConnectionPool:
    """Optional reusable pool; explicitly started and closed by its owner.

    Existing acquire/release APIs are preserved. close() waits for borrowed
    leases so an active transfer cannot have its session closed underneath it.
    """

    def __init__(self, factory: Any, config: MediaConnectionConfig | None = None):
        self.factory = factory
        self.config = config or MediaConnectionConfig()
        self._queue: asyncio.Queue[Any] = asyncio.Queue(self.config.connections)
        self._lock = asyncio.Lock()
        self._started = False
        self._leases: list[Any] = []
        self._closing = False
        self._borrowed = 0
        self._checked_out: set[int] = set()
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self, dc_id: int):
        async with self._lock:
            if self._started:
                return
            if self._closing:
                raise RuntimeError("Pool is closing")
            leases = []
            try:
                for _ in range(self.config.connections):
                    leases.append(await self.factory.create_media_session(dc_id))
            except BaseException:
                await asyncio.gather(*(lease.close() for lease in leases), return_exceptions=True)
                raise
            self._leases = leases
            for lease in leases:
                self._queue.put_nowait(lease)
            self._started = True

    async def acquire(self):
        if not self._started or self._closing:
            raise RuntimeError("Media connection pool is not started or is closing")
        lease = await self._queue.get()
        if self._closing:
            self._queue.put_nowait(lease)
            raise RuntimeError("Media connection pool is closing")
        self._borrowed += 1
        self._checked_out.add(id(lease))
        self._idle.clear()
        return lease

    async def release(self, session):
        if session not in self._leases:
            raise ValueError("Session does not belong to this pool")
        if id(session) not in self._checked_out:
            raise RuntimeError("Session has not been acquired or was already released")
        self._checked_out.remove(id(session))
        self._borrowed -= 1
        self._queue.put_nowait(session)
        if not self._borrowed:
            self._idle.set()

    async def close(self):
        async with self._lock:
            if not self._started:
                return
            self._closing = True
            try:
                await self._idle.wait()
                await asyncio.gather(
                    *(lease.close() for lease in self._leases),
                    return_exceptions=True,
                )
                self._leases.clear()
                while not self._queue.empty():
                    self._queue.get_nowait()
                self._started = False
            finally:
                self._closing = False
