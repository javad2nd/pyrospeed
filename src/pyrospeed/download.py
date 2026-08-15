from __future__ import annotations

import asyncio
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from .compat import FrameworkBinding
from .config import DownloadConfig
from .errors import UnknownFileSizeError
from .progress import ProgressCallback, ProgressDispatcher


class _RandomAccessWriter:
    def __init__(self, path: Path, size: int, threaded: bool) -> None:
        self.path = path
        self.size = size
        self.threaded = threaded
        self.fd: Optional[int] = None
        self._lock = Lock()

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
        os.ftruncate(self.fd, self.size)

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def _write_sync(self, offset: int, data: bytes) -> None:
        assert self.fd is not None
        if hasattr(os, "pwrite"):
            view = memoryview(data)
            written = 0
            while written < len(view):
                written += os.pwrite(self.fd, view[written:], offset + written)
            return
        with self._lock:
            os.lseek(self.fd, offset, os.SEEK_SET)
            view = memoryview(data)
            written = 0
            while written < len(view):
                written += os.write(self.fd, view[written:])

    async def write(self, offset: int, data: bytes) -> None:
        if self.threaded:
            await asyncio.to_thread(self._write_sync, offset, data)
        else:
            self._write_sync(offset, data)


class ParallelDownloader:
    """Parallel ranged downloader built on Pyrogram-compatible get_file()."""

    MEDIA_ATTRS = (
        "audio",
        "document",
        "photo",
        "sticker",
        "animation",
        "video",
        "voice",
        "video_note",
        "new_chat_photo",
    )

    def __init__(
        self,
        client: Any,
        config: DownloadConfig,
        *,
        binding: Optional[FrameworkBinding] = None,
    ) -> None:
        self.client = client
        self.config = config
        self.binding = binding or FrameworkBinding.from_client(client)
        self._file_limiter = asyncio.Semaphore(config.max_parallel_files)

    async def download(
        self,
        media: Any,
        file_name: Optional[str] = None,
        *,
        file_size: Optional[int] = None,
        in_memory: bool = False,
        progress: Optional[ProgressCallback] = None,
        progress_args: tuple[Any, ...] = (),
    ) -> Any:
        async with self._file_limiter:
            return await self._download(
                media,
                file_name=file_name,
                file_size=file_size,
                in_memory=in_memory,
                progress=progress,
                progress_args=progress_args,
            )

    async def _download(
        self,
        media: Any,
        *,
        file_name: Optional[str],
        file_size: Optional[int],
        in_memory: bool,
        progress: Optional[ProgressCallback],
        progress_args: tuple[Any, ...],
    ) -> Any:
        resolved = self._resolve_media(media)
        file_id_str = resolved[0]
        media_size = resolved[1]
        media_name = resolved[2]
        size = int(file_size if file_size is not None else media_size or 0)

        if in_memory or size <= 0:
            if size <= 0 and not self.config.fallback_to_native_for_unknown_size:
                raise UnknownFileSizeError(
                    "Parallel download needs file_size; pass it explicitly or use a media object with file_size"
                )
            return await self.client.download_media(
                media,
                file_name=file_name or "downloads/",
                in_memory=in_memory,
                progress=progress,
                progress_args=progress_args,
            )

        file_id_obj = self.binding.decode_file_id(file_id_str)
        target = self._target_path(file_name, media_name, file_id_obj)
        temp = target.with_name(target.name + ".pyrospeed.part")
        writer = _RandomAccessWriter(
            temp,
            size,
            threaded=self.config.disk_io == "thread",
        )
        if self.config.disk_io == "thread":
            await asyncio.to_thread(writer.open)
        else:
            writer.open()

        dispatcher = ProgressDispatcher(
            progress,
            size,
            args=progress_args,
            interval=self.config.progress_interval,
        )
        segment_bytes = self.config.segment_chunks * self.config.chunk_size
        segment_count = math.ceil(size / segment_bytes)
        pending: set[asyncio.Task[None]] = set()
        limiter = asyncio.Semaphore(self.config.workers)
        max_pending = max(1, self.config.workers * self.config.queue_factor)

        async def download_segment(segment_index: int) -> None:
            async with limiter:
                start_chunk = segment_index * self.config.segment_chunks
                byte_offset = start_chunk * self.config.chunk_size
                remaining = size - byte_offset
                chunk_limit = min(
                    self.config.segment_chunks,
                    math.ceil(remaining / self.config.chunk_size),
                )
                cursor = byte_offset
                async for chunk in self.client.get_file(
                    file_id_obj,
                    file_size=size,
                    limit=chunk_limit,
                    offset=start_chunk,
                    progress=None,
                    progress_args=(),
                ):
                    if cursor >= size:
                        break
                    data = chunk[: size - cursor]
                    await writer.write(cursor, data)
                    cursor += len(data)
                    await dispatcher.add(len(data))

        try:
            async with self._download_semaphore_override():
                for segment_index in range(segment_count):
                    pending.add(asyncio.create_task(download_segment(segment_index)))
                    if len(pending) >= max_pending:
                        done, pending = await asyncio.wait(
                            pending,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        await asyncio.gather(*done)
                if pending:
                    await asyncio.gather(*pending)
                    pending.clear()
            await dispatcher.finish()
            # Windows cannot reliably replace/unlink an open file. Close the
            # descriptor before the atomic rename.
            writer.close()
            await asyncio.to_thread(os.replace, temp, target)
            return str(target.resolve())
        except BaseException:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            writer.close()
            try:
                temp.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        finally:
            writer.close()

    def _resolve_media(self, media: Any) -> tuple[str, int, str]:
        obj = media
        if isinstance(media, str):
            return media, 0, ""
        for attr in self.MEDIA_ATTRS:
            candidate = getattr(media, attr, None)
            if candidate is not None:
                obj = candidate
                break
        file_id = getattr(obj, "file_id", None)
        if not isinstance(file_id, str):
            raise ValueError("Expected a file_id string, media object, or Message with downloadable media")
        return (
            file_id,
            int(getattr(obj, "file_size", 0) or 0),
            str(getattr(obj, "file_name", "") or ""),
        )

    def _target_path(self, file_name: Optional[str], media_name: str, file_id_obj: Any) -> Path:
        requested = Path(file_name or "downloads/")
        as_directory = (
            file_name is None
            or str(file_name).endswith(("/", "\\"))
            or requested.is_dir()
        )
        if as_directory:
            name = media_name or (
                f"pyrospeed_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
                f"{getattr(file_id_obj, 'media_id', 'file')}.bin"
            )
            return requested / name
        return requested

    @asynccontextmanager
    async def _download_semaphore_override(self):
        attr = "get_file_semaphore"
        if not self.config.override_client_semaphore or not hasattr(self.client, attr):
            yield
            return
        original = getattr(self.client, attr)
        setattr(self.client, attr, asyncio.Semaphore(self.config.workers))
        try:
            yield
        finally:
            setattr(self.client, attr, original)
