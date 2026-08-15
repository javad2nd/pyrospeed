from __future__ import annotations

import asyncio
import hashlib
import io
import math
import os
from pathlib import PurePath
from typing import Any, BinaryIO, Optional, Tuple, Union

from .compat import FrameworkBinding
from .config import UploadConfig
from .progress import ProgressCallback, ProgressDispatcher
from .session_factory import SessionFactory, SessionLease

PathLike = Union[str, os.PathLike[str], PurePath]
Source = Union[PathLike, BinaryIO]


class ParallelUploader:
    """Bounded-memory, multi-session Telegram file uploader."""

    def __init__(
        self,
        client: Any,
        config: UploadConfig,
        *,
        binding: Optional[FrameworkBinding] = None,
        session_factory: Optional[SessionFactory] = None,
        native_save_file: Optional[Any] = None,
    ) -> None:
        self.client = client
        self.config = config
        self.binding = binding or FrameworkBinding.from_client(client)
        self.session_factory = session_factory or SessionFactory(client, self.binding)
        self.native_save_file = native_save_file or client.save_file
        self._file_limiter = asyncio.Semaphore(config.max_parallel_files)

    async def save_file(
        self,
        path: Source,
        file_id: Optional[int] = None,
        file_part: int = 0,
        progress: Optional[ProgressCallback] = None,
        progress_args: Tuple[Any, ...] = (),
    ) -> Any:
        if path is None:
            return None
        async with self._file_limiter:
            return await self._save_file(
                path,
                file_id=file_id,
                file_part=file_part,
                progress=progress,
                progress_args=progress_args,
            )

    async def _save_file(
        self,
        path: Source,
        *,
        file_id: Optional[int],
        file_part: int,
        progress: Optional[ProgressCallback],
        progress_args: Tuple[Any, ...],
    ) -> Any:
        fp, file_name, file_size, opened_here = await self._open_source(path)
        try:
            if file_size <= 0:
                raise ValueError("File size equals 0 B")

            is_big = file_size > self.config.large_file_threshold
            if (
                not is_big
                and file_id is None
                and self.config.small_file_strategy == "native"
            ):
                if opened_here:
                    fp.close()
                    opened_here = False
                return await self.native_save_file(
                    path,
                    file_id=file_id,
                    file_part=file_part,
                    progress=progress,
                    progress_args=progress_args,
                )

            total_parts = math.ceil(file_size / self.config.part_size)
            dc_id = await self.client.storage.dc_id()

            if file_id is not None:
                return await self._resend_part(
                    fp,
                    dc_id=dc_id,
                    file_id=file_id,
                    file_part=file_part,
                    total_parts=total_parts,
                    is_big=is_big,
                )

            upload_id = self.client.rnd_id()
            checksum = hashlib.md5() if not is_big else None
            dispatcher = ProgressDispatcher(
                progress,
                file_size,
                args=progress_args,
                interval=self.config.progress_interval,
            )
            leases: list[SessionLease] = []
            pending: set[asyncio.Task[None]] = set()

            try:
                leases = await self._create_sessions(dc_id)
                limiters = [
                    asyncio.Semaphore(self.config.inflight_per_connection)
                    for _ in leases
                ]
                max_pending = max(
                    1,
                    self.config.max_inflight_chunks * self.config.queue_factor,
                )

                async def upload_one(part_no: int, chunk: bytes) -> None:
                    index = part_no % len(leases)
                    if is_big:
                        rpc = self.binding.raw.functions.upload.SaveBigFilePart(
                            file_id=upload_id,
                            file_part=part_no,
                            file_total_parts=total_parts,
                            bytes=chunk,
                        )
                    else:
                        rpc = self.binding.raw.functions.upload.SaveFilePart(
                            file_id=upload_id,
                            file_part=part_no,
                            bytes=chunk,
                        )
                    async with limiters[index]:
                        await leases[index].session.invoke(rpc)
                    await dispatcher.add(len(chunk))

                await self._seek(fp, 0)
                part_no = 0
                while True:
                    chunk = await self._read(fp, self.config.part_size)
                    if not chunk:
                        break
                    if checksum is not None:
                        checksum.update(chunk)
                    pending.add(asyncio.create_task(upload_one(part_no, chunk)))
                    part_no += 1
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
            except BaseException:
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                raise
            finally:
                if leases:
                    await asyncio.gather(
                        *(lease.close() for lease in leases),
                        return_exceptions=True,
                    )

            if is_big:
                return self.binding.raw.types.InputFileBig(
                    id=upload_id,
                    parts=total_parts,
                    name=file_name,
                )
            return self.binding.raw.types.InputFile(
                id=upload_id,
                parts=total_parts,
                name=file_name,
                md5_checksum=checksum.hexdigest() if checksum is not None else "",
            )
        finally:
            if opened_here and not fp.closed:
                fp.close()

    async def _open_source(self, path: Source) -> tuple[BinaryIO, str, int, bool]:
        def inspect_source() -> tuple[BinaryIO, str, int, bool]:
            if isinstance(path, (str, os.PathLike, PurePath)):
                path_str = os.fspath(path)
                fp = open(path_str, "rb", buffering=0)
                size = os.path.getsize(path_str)
                return fp, os.path.basename(path_str), size, True
            if isinstance(path, io.IOBase):
                name = getattr(path, "name", "file")
                name = "file" if isinstance(name, int) else os.path.basename(str(name))
                current = path.tell()
                path.seek(0, os.SEEK_END)
                size = path.tell()
                path.seek(current, os.SEEK_SET)
                return path, name, size, False
            raise ValueError("Expected a path or seekable binary IO object")

        if self.config.disk_io == "thread":
            return await asyncio.to_thread(inspect_source)
        return inspect_source()

    async def _read(self, fp: BinaryIO, size: int) -> bytes:
        if self.config.disk_io == "thread":
            return await asyncio.to_thread(fp.read, size)
        return fp.read(size)

    async def _seek(self, fp: BinaryIO, offset: int) -> None:
        if self.config.disk_io == "thread":
            await asyncio.to_thread(fp.seek, offset)
        else:
            fp.seek(offset)

    async def _create_sessions(self, dc_id: int) -> list[SessionLease]:
        leases: list[SessionLease] = []
        try:
            for _ in range(self.config.connections):
                leases.append(await self.session_factory.create_media_session(dc_id))
            return leases
        except BaseException:
            if leases:
                await asyncio.gather(
                    *(lease.close() for lease in leases),
                    return_exceptions=True,
                )
            raise

    async def _resend_part(
        self,
        fp: BinaryIO,
        *,
        dc_id: int,
        file_id: int,
        file_part: int,
        total_parts: int,
        is_big: bool,
    ) -> None:
        await self._seek(fp, self.config.part_size * file_part)
        chunk = await self._read(fp, self.config.part_size)
        if not chunk:
            raise ValueError(f"Missing part {file_part} is outside the file")
        lease = await self.session_factory.create_media_session(dc_id)
        try:
            if is_big:
                rpc = self.binding.raw.functions.upload.SaveBigFilePart(
                    file_id=file_id,
                    file_part=file_part,
                    file_total_parts=total_parts,
                    bytes=chunk,
                )
            else:
                rpc = self.binding.raw.functions.upload.SaveFilePart(
                    file_id=file_id,
                    file_part=file_part,
                    bytes=chunk,
                )
            await lease.session.invoke(rpc)
            return None
        finally:
            await lease.close()
