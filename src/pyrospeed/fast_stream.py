"""High performance asyncio based media streaming helpers."""
from __future__ import annotations
import asyncio
from collections.abc import AsyncIterator

class ParallelStreamConfig:
    def __init__(self, workers=4, chunk_size=1024*1024, queue_size=8):
        self.workers=workers
        self.chunk_size=chunk_size
        self.queue_size=queue_size

async def buffered_stream(source: AsyncIterator[bytes], config=None):
    """Buffered async stream wrapper to reduce scheduling overhead."""
    config=config or ParallelStreamConfig()
    queue=asyncio.Queue(maxsize=config.queue_size)
    async def producer():
        try:
            async for chunk in source:
                await queue.put(chunk)
        finally:
            await queue.put(None)
    task=asyncio.create_task(producer())
    try:
        while True:
            item=await queue.get()
            if item is None:
                break
            yield item
    finally:
        task.cancel()
