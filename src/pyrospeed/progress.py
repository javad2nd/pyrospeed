from __future__ import annotations

import asyncio
import functools
import inspect
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

ProgressCallback = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class TransferSnapshot:
    current: int
    total: int
    elapsed: float
    bytes_per_second: float

    @property
    def fraction(self) -> float:
        return self.current / self.total if self.total else 0.0

    @property
    def mib_per_second(self) -> float:
        return self.bytes_per_second / (1024 * 1024)


class ProgressDispatcher:
    """Aggregates concurrent completions and throttles user callbacks."""

    def __init__(
        self,
        callback: Optional[ProgressCallback],
        total: int,
        *,
        args: Tuple[Any, ...] = (),
        interval: float = 0.25,
    ) -> None:
        self.callback = callback
        self.total = total
        self.args = args
        self.interval = interval
        self.current = 0
        self.started = time.monotonic()
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def add(self, count: int) -> None:
        if count <= 0:
            return
        should_call = False
        current = 0
        async with self._lock:
            self.current = min(self.total, self.current + count) if self.total else self.current + count
            current = self.current
            now = time.monotonic()
            if self.callback and (
                self.interval == 0
                or (self.total and current >= self.total)
                or now - self._last_call >= self.interval
            ):
                self._last_call = now
                should_call = True
        if should_call:
            await self._invoke(current)

    async def finish(self) -> None:
        # add() already emits a final callback when current reaches total.
        # Avoid calling the same 100% progress event twice.
        if self.total and self.current < self.total:
            await self.add(self.total - self.current)
        elif not self.total and self.callback:
            await self._invoke(self.current)

    def snapshot(self) -> TransferSnapshot:
        elapsed = max(time.monotonic() - self.started, 1e-9)
        return TransferSnapshot(
            current=self.current,
            total=self.total,
            elapsed=elapsed,
            bytes_per_second=self.current / elapsed,
        )

    async def _invoke(self, current: int) -> None:
        if self.callback is None:
            return
        result_args = (current, self.total, *self.args)
        if inspect.iscoroutinefunction(self.callback):
            await self.callback(*result_args)
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, functools.partial(self.callback, *result_args))
