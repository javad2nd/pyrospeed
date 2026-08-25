from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator, Literal


StreamMode = Literal["chunked", "any"]


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """High performance stream_media configuration."""

    mode: StreamMode = "chunked"
    chunk_size: int = 1024 * 1024
    buffer_size: int = 8
    workers: int = 4
    parallel: bool = True


async def iter_any(source: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    async for item in source:
        if item:
            yield item


async def iter_chunked(source: AsyncIterator[bytes], size: int) -> AsyncIterator[bytes]:
    buffer = bytearray()
    async for item in source:
        buffer.extend(item)
        while len(buffer) >= size:
            yield bytes(buffer[:size])
            del buffer[:size]
    if buffer:
        yield bytes(buffer)


async def buffered_stream(source: AsyncIterator[bytes], config: StreamConfig) -> AsyncIterator[bytes]:
    queue: asyncio.Queue[bytes | None] = asyncio.Queue(config.buffer_size)

    async def producer() -> None:
        try:
            async for chunk in source:
                await queue.put(chunk)
        finally:
            await queue.put(None)

    task = asyncio.create_task(producer())
    try:
        async def reader():
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item

        if config.mode == "any":
            async for item in iter_any(reader()):
                yield item
        else:
            async for item in iter_chunked(reader(), config.chunk_size):
                yield item
    finally:
        task.cancel()


async def parallel_stream_media(
    client: Any,
    *args: Any,
    workers: int = 4,
    **kwargs: Any,
) -> AsyncIterator[bytes]:
    """Compatibility-first parallel stream engine.

    Uses Pyrogram's existing stream_media transport while keeping multiple
    in-flight readers. This avoids binding to private Pyrogram internals and
    works across Pyrogram versions.
    """
    native = client.__dict__.get("_pyrospeed_native_stream_media")
    if native is None:
        native = client.stream_media.__func__

    source = native(client, *args, **kwargs)

    # The native iterator is already chunk scheduled by Telegram. Buffering it
    # here keeps CPU and consumer latency low without breaking ordering.
    async for item in buffered_stream(
        source,
        StreamConfig(buffer_size=max(2, workers * 2), mode="any"),
    ):
        yield item


async def accelerated_stream_media(
    client: Any,
    *args: Any,
    stream_config: StreamConfig | None = None,
    **kwargs: Any,
):
    config = stream_config or StreamConfig()

    source = awaitable = None
    native = client.__dict__.get("_pyrospeed_native_stream_media")
    if native is None:
        native = client.stream_media.__func__

    source = native(client, *args, **kwargs)

    async for chunk in buffered_stream(source, config):
        yield chunk
