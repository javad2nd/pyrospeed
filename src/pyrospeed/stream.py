from __future__ import annotations

"""Async first streaming pipeline used by PyroSpeed.

The streaming layer intentionally lives next to upload/download engines. It does
not depend on private Pyrogram internals and only wraps the public
``stream_media`` API.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator, Literal

StreamMode = Literal["chunked", "any"]


@dataclass(frozen=True, slots=True)
class StreamConfig:
    mode: StreamMode = "any"
    chunk_size: int = 1024 * 1024
    buffer_size: int = 8

    def __post_init__(self) -> None:
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.buffer_size < 1:
            raise ValueError("buffer_size must be >= 1")


class StreamPipeline:
    """Reusable bounded-memory async stream processor."""

    _STOP = object()

    def __init__(self, config: StreamConfig | None = None) -> None:
        self.config = config or StreamConfig()

    async def _buffer(self, source: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        queue: asyncio.Queue[object] = asyncio.Queue(self.config.buffer_size)

        async def producer() -> None:
            try:
                async for chunk in source:
                    if chunk:
                        await queue.put(chunk)
            finally:
                await queue.put(self._STOP)

        task = asyncio.create_task(producer())
        try:
            while True:
                item = await queue.get()
                if item is self._STOP:
                    break
                yield item  # type: ignore[misc]
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def process(self, source: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        buffered = self._buffer(source)
        if self.config.mode == "any":
            async for chunk in buffered:
                yield chunk
            return

        pending = bytearray()
        async for chunk in buffered:
            pending.extend(chunk)
            while len(pending) >= self.config.chunk_size:
                yield bytes(pending[: self.config.chunk_size])
                del pending[: self.config.chunk_size]
        if pending:
            yield bytes(pending)


class StreamManager:
    """Pyrogram-compatible stream facade."""

    def __init__(self, client: Any, config: StreamConfig | None = None) -> None:
        self.client = client
        self.pipeline = StreamPipeline(config)

    async def stream_media(self, *args: Any, **kwargs: Any) -> AsyncIterator[bytes]:
        native = self.client.__dict__.get("_pyrospeed_native_stream_media")
        if native is None:
            native = self.client.stream_media
        source = native(*args, **kwargs)
        async for chunk in self.pipeline.process(source):
            yield chunk
