"""Legacy streaming helper, retained for backward compatibility."""
from __future__ import annotations

from collections.abc import AsyncIterator

from .stream import StreamConfig, StreamPipeline


class ParallelStreamConfig:
    def __init__(self, workers=4, chunk_size=1024 * 1024, queue_size=8):
        # Retained for callers of the legacy API. An ordered source iterator
        # cannot safely be consumed in parallel just by adding workers.
        if workers < 1:
            raise ValueError("workers must be >= 1")
        self.workers = workers
        self.chunk_size = chunk_size
        self.queue_size = queue_size


async def buffered_stream(source: AsyncIterator[bytes], config=None):
    """Buffer a single source without changing the order of its chunks."""
    config = config or ParallelStreamConfig()
    pipeline = StreamPipeline(StreamConfig(
        mode="any", chunk_size=config.chunk_size, buffer_size=config.queue_size
    ))
    processed = pipeline.process(source)
    try:
        async for chunk in processed:
            yield chunk
    finally:
        await processed.aclose()
