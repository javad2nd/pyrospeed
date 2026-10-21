from __future__ import annotations

import asyncio
import math
import os
import tempfile
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from .compat import FrameworkBinding
from .config import DownloadConfig
from .errors import IncompleteDownloadError, UnknownFileSizeError
from .progress import ProgressCallback, ProgressDispatcher


class _RandomAccessWriter:
    """Writer backed by a unique, exclusively created temporary file."""

    def __init__(self, path: Path, size: int, threaded: bool) -> None:
        self.path = path  # Destination, not the temporary file.
        self.temp_path: Optional[Path] = None
        self.size = size
        self.threaded = threaded
        self.fd: Optional[int] = None
        self._lock = Lock()

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(
            prefix=self.path.name + ".pyrospeed.",
            suffix=".part",
            dir=str(self.path.parent),
        )
        self.fd = fd
        self.temp_path = Path(name)
        try:
            os.ftruncate(fd, self.size)
        except BaseException:
            self.close()
            self.cleanup()
            raise

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def cleanup(self) -> None:
        if self.temp_path is not None:
            self.temp_path.unlink(missing_ok=True)

    def _write_sync(self, offset: int, data: bytes) -> None:
        if self.fd is None:
            raise RuntimeError("Download writer is not open")
        if hasattr(os, "pwrite"):
            view = memoryview(data)
            written = 0
            while written < len(view):
                count = os.pwrite(self.fd, view[written:], offset + written)
                if count <= 0:
                    raise OSError("Short write while saving a download")
                written += count
            return
        with self._lock:
            os.lseek(self.fd, offset, os.SEEK_SET)
            view = memoryview(data)
            written = 0
            while written < len(view):
                count = os.write(self.fd, view[written:])
                if count <= 0:
                    raise OSError("Short write while saving a download")
                written += count

    async def write(self, offset: int, data: bytes) -> None:
        if self.threaded:
            await asyncio.to_thread(self._write_sync, offset, data)
        else:
            self._write_sync(offset, data)


class ParallelDownloader:
    """Parallel ranged downloader built on Pyrogram-compatible get_file()."""

    MEDIA_ATTRS = (
        "audio", "document", "photo", "sticker", "animation", "video",
        "voice", "video_note", "new_chat_photo",
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
        file_id_str, media_size, media_name = self._resolve_media(media)
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
        writer = _RandomAccessWriter(
            target, size, threaded=self.config.disk_io == "thread"
        )
        # mkstemp + ftruncate are short operations. Keep them on the event
        # loop so cancellation cannot leave a still-opening worker thread.
        writer.open()

        dispatcher = ProgressDispatcher(
            progress, size, args=progress_args,
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
                expected = min(segment_bytes, size - byte_offset)
                chunk_limit = math.ceil(expected / self.config.chunk_size)
                received = 0
                async for chunk in self.client.get_file(
                    file_id_obj,
                    file_size=size,
                    limit=chunk_limit,
                    offset=start_chunk,
                    progress=None,
                    progress_args=(),
                ):
                    if not chunk:
                        continue
                    if len(chunk) > expected - received:
                        raise IncompleteDownloadError(
                            f"Segment {segment_index} produced more bytes than expected "
                            f"({received + len(chunk)} > {expected})"
                        )
                    await writer.write(byte_offset + received, chunk)
                    received += len(chunk)
                    await dispatcher.add(len(chunk))
                if received != expected:
                    raise IncompleteDownloadError(
                        f"Segment {segment_index} incomplete: received {received} of {expected} bytes"
                    )

        try:
            # We intentionally never replace client.get_file_semaphore. Other
            # transfers on the same client must not observe a temporary value.
            for segment_index in range(segment_count):
                pending.add(asyncio.create_task(download_segment(segment_index)))
                if len(pending) >= max_pending:
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED,
                    )
                    await asyncio.gather(*done)
            if pending:
                await asyncio.gather(*pending)
                pending.clear()
            await dispatcher.finish()
            writer.close()
            # Atomic replacement also avoids mixing bytes when two independent
            # downloads happen to use the same explicit destination.
            assert writer.temp_path is not None
            await asyncio.to_thread(os.replace, writer.temp_path, target)
            return str(target.resolve())
        except BaseException:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            writer.close()
            writer.cleanup()
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

    @staticmethod
    def _safe_media_name(value: str) -> str:
        # Telegram metadata is untrusted: use only one sanitized leaf filename,
        # on both Unix and Windows (both kinds of path separators are accepted).
        name = value.replace("\\", "/").split("/")[-1]
        name = "".join(c for c in name if ord(c) >= 32 and c not in '<>:"/\\|?*')
        name = name.strip(" .")
        if name.upper().split(".", 1)[0].rstrip(" ") in {
            "CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
            *(f"LPT{i}" for i in range(1, 10)),
        }:
            name = ""
        return name

    def _target_path(self, file_name: Optional[str], media_name: str, file_id_obj: Any) -> Path:
        requested = Path(file_name or "downloads/")
        as_directory = (
            file_name is None
            or str(file_name).endswith(("/", "\\"))
            or requested.is_dir()
        )
        if as_directory:
            name = self._safe_media_name(media_name) or (
                f"pyrospeed_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
                f"{getattr(file_id_obj, 'media_id', 'file')}.bin"
            )
            # The generated fallback must be safe even if a custom decoder
            # returns an unexpected media_id.
            name = self._safe_media_name(name) or "pyrospeed_file.bin"
            return requested / name
        # Explicit file destinations come from the caller, not Telegram metadata.
        return requested

    @asynccontextmanager
    async def _download_semaphore_override(self):
        """Deprecated compatibility hook; never mutate shared Client internals."""
        yield
