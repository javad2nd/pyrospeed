from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from pyrospeed.config import DownloadConfig, UploadConfig
from pyrospeed.download import ParallelDownloader
from pyrospeed.patch import InstanceUploadPatch
from pyrospeed.progress import ProgressDispatcher
from pyrospeed.session_factory import SessionLease
from pyrospeed.upload import ParallelUploader


class SaveBigFilePart:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class SaveFilePart:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class InputFileBig:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class InputFile:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeFileId:
    def __init__(self, media_id=7):
        self.media_id = media_id

    @classmethod
    def decode(cls, value: str):
        assert value == "fake-id"
        return cls()


RAW = SimpleNamespace(
    functions=SimpleNamespace(
        upload=SimpleNamespace(
            SaveBigFilePart=SaveBigFilePart,
            SaveFilePart=SaveFilePart,
        )
    ),
    types=SimpleNamespace(InputFileBig=InputFileBig, InputFile=InputFile),
)

BINDING = SimpleNamespace(raw=RAW, decode_file_id=FakeFileId.decode)


class FakeStorage:
    async def dc_id(self):
        return 2


class FakeSession:
    def __init__(self):
        self.calls = []
        self.closed = False

    async def invoke(self, rpc):
        await asyncio.sleep(0)
        self.calls.append(rpc)
        return True

    async def stop(self):
        self.closed = True


class FakeFactory:
    def __init__(self):
        self.sessions = []

    async def create_media_session(self, dc_id):
        assert dc_id == 2
        session = FakeSession()
        self.sessions.append(session)
        return SessionLease(session, owned=True)


class FailingSession(FakeSession):
    async def invoke(self, rpc):
        await asyncio.sleep(0)
        self.calls.append(rpc)
        if getattr(rpc, "file_part", -1) == 1:
            raise RuntimeError("synthetic upload failure")
        return True


class FailingFactory(FakeFactory):
    async def create_media_session(self, dc_id):
        assert dc_id == 2
        session = FailingSession()
        self.sessions.append(session)
        return SessionLease(session, owned=True)


class UploadClient:
    def __init__(self):
        self.storage = FakeStorage()
        self.native_calls = 0

    def rnd_id(self):
        return 123456

    async def save_file(self, *args, **kwargs):
        self.native_calls += 1
        return "native"


class UploadTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_large_file_uploads_each_part_once(self):
        client = UploadClient()
        factory = FakeFactory()
        cfg = UploadConfig(
            connections=3,
            inflight_per_connection=2,
            queue_factor=1,
            large_file_threshold=1,
            disk_io="thread",
        )
        uploader = ParallelUploader(
            client,
            cfg,
            binding=BINDING,
            session_factory=factory,
            native_save_file=client.save_file,
        )
        payload = os.urandom(2 * 1024 * 1024 + 123)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(payload)
            name = f.name
        progress = []
        try:
            result = await uploader.save_file(
                name,
                progress=lambda current, total: progress.append((current, total)),
            )
        finally:
            os.unlink(name)

        calls = [call for s in factory.sessions for call in s.calls]
        expected_parts = (len(payload) + cfg.part_size - 1) // cfg.part_size
        self.assertEqual(result.parts, expected_parts)
        self.assertEqual(len(calls), expected_parts)
        self.assertEqual(sorted(c.file_part for c in calls), list(range(expected_parts)))
        self.assertTrue(all(s.closed for s in factory.sessions))
        self.assertEqual(progress[-1], (len(payload), len(payload)))

    async def test_small_file_uses_native(self):
        client = UploadClient()
        factory = FakeFactory()
        uploader = ParallelUploader(
            client,
            UploadConfig(),
            binding=BINDING,
            session_factory=factory,
            native_save_file=client.save_file,
        )
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"x" * 1024)
            name = f.name
        try:
            result = await uploader.save_file(name)
        finally:
            os.unlink(name)
        self.assertEqual(result, "native")
        self.assertEqual(client.native_calls, 1)
        self.assertEqual(factory.sessions, [])

    async def test_small_file_can_use_parallel_save_file_part(self):
        client = UploadClient()
        factory = FakeFactory()
        cfg = UploadConfig(
            connections=2,
            inflight_per_connection=2,
            queue_factor=1,
            small_file_strategy="parallel",
        )
        uploader = ParallelUploader(
            client,
            cfg,
            binding=BINDING,
            session_factory=factory,
            native_save_file=client.save_file,
        )
        payload = os.urandom(1024 * 1024 + 17)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(payload)
            name = f.name
        try:
            result = await uploader.save_file(name)
        finally:
            os.unlink(name)

        calls = [call for session in factory.sessions for call in session.calls]
        self.assertTrue(calls)
        self.assertTrue(all(isinstance(call, SaveFilePart) for call in calls))
        self.assertEqual(result.md5_checksum, hashlib.md5(payload).hexdigest())
        self.assertEqual(client.native_calls, 0)

    async def test_missing_part_resends_only_requested_chunk(self):
        client = UploadClient()
        factory = FakeFactory()
        cfg = UploadConfig(large_file_threshold=1)
        uploader = ParallelUploader(
            client,
            cfg,
            binding=BINDING,
            session_factory=factory,
            native_save_file=client.save_file,
        )
        payload = os.urandom(cfg.part_size * 3 + 9)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(payload)
            name = f.name
        try:
            result = await uploader.save_file(name, file_id=77, file_part=1)
        finally:
            os.unlink(name)

        self.assertIsNone(result)
        calls = [call for session in factory.sessions for call in session.calls]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].file_id, 77)
        self.assertEqual(calls[0].file_part, 1)
        self.assertEqual(calls[0].bytes, payload[cfg.part_size:cfg.part_size * 2])


    async def test_upload_failure_closes_all_owned_sessions(self):
        client = UploadClient()
        factory = FailingFactory()
        cfg = UploadConfig(
            connections=3,
            inflight_per_connection=1,
            queue_factor=1,
            large_file_threshold=1,
        )
        uploader = ParallelUploader(
            client,
            cfg,
            binding=BINDING,
            session_factory=factory,
            native_save_file=client.save_file,
        )
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(os.urandom(cfg.part_size * 4))
            name = f.name
        try:
            with self.assertRaisesRegex(RuntimeError, "synthetic upload failure"):
                await uploader.save_file(name)
        finally:
            os.unlink(name)
        self.assertEqual(len(factory.sessions), cfg.connections)
        self.assertTrue(all(session.closed for session in factory.sessions))



class PatchClient:
    async def save_file(self, value):
        return f"native:{value}"


class StubUploader:
    async def save_file(self, value):
        return f"fast:{value}"


class PatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_patch_is_instance_scoped_and_restored(self):
        a = PatchClient()
        b = PatchClient()
        patch = InstanceUploadPatch(a, StubUploader())
        self.assertEqual(await a.save_file("x"), "native:x")
        patch.install()
        self.assertEqual(await a.save_file("x"), "fast:x")
        self.assertEqual(await b.save_file("x"), "native:x")
        patch.uninstall()
        self.assertEqual(await a.save_file("x"), "native:x")


class DownloadClient:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.get_file_semaphore = asyncio.Semaphore(1)
        self.native_calls = 0

    async def get_file(self, file_id, file_size=0, limit=0, offset=0, progress=None, progress_args=()):
        chunk = 1024 * 1024
        start = offset * chunk
        end = min(len(self.payload), start + limit * chunk)
        cursor = start
        while cursor < end:
            await asyncio.sleep(0)
            data = self.payload[cursor:min(end, cursor + chunk)]
            if not data:
                break
            yield data
            cursor += len(data)

    async def download_media(self, media, **kwargs):
        self.native_calls += 1
        return "native-download"


class FailingDownloadClient(DownloadClient):
    async def get_file(self, file_id, file_size=0, limit=0, offset=0, progress=None, progress_args=()):
        if offset >= 2:
            await asyncio.sleep(0)
            raise RuntimeError("synthetic download failure")
        async for chunk in super().get_file(
            file_id,
            file_size=file_size,
            limit=limit,
            offset=offset,
            progress=progress,
            progress_args=progress_args,
        ):
            yield chunk


class Media:
    file_id = "fake-id"
    file_name = "payload.bin"

    def __init__(self, size):
        self.file_size = size


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_download_reconstructs_file(self):
        payload = os.urandom(5 * 1024 * 1024 + 321)
        client = DownloadClient(payload)
        downloader = ParallelDownloader(
            client,
            DownloadConfig(workers=3, segment_chunks=2, disk_io="thread"),
            binding=BINDING,
        )
        progress = []
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "out.bin"
            result = await downloader.download(
                Media(len(payload)),
                str(target),
                progress=lambda current, total: progress.append((current, total)),
            )
            self.assertEqual(Path(result).read_bytes(), payload)
        self.assertEqual(progress[-1], (len(payload), len(payload)))

    async def test_unknown_size_falls_back_to_native(self):
        client = DownloadClient(b"abc")
        downloader = ParallelDownloader(client, DownloadConfig(), binding=BINDING)
        result = await downloader.download("fake-id")
        self.assertEqual(result, "native-download")
        self.assertEqual(client.native_calls, 1)


    async def test_download_failure_removes_temp_and_restores_semaphore(self):
        payload = os.urandom(6 * 1024 * 1024)
        client = FailingDownloadClient(payload)
        original_semaphore = client.get_file_semaphore
        downloader = ParallelDownloader(
            client,
            DownloadConfig(workers=3, segment_chunks=1, queue_factor=1),
            binding=BINDING,
        )
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "failed.bin"
            with self.assertRaisesRegex(RuntimeError, "synthetic download failure"):
                await downloader.download(Media(len(payload)), str(target))
            self.assertFalse(target.exists())
            self.assertFalse(Path(str(target) + ".pyrospeed.part").exists())
        self.assertIs(client.get_file_semaphore, original_semaphore)



class ProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_progress_callback_is_not_duplicated(self):
        calls = []
        dispatcher = ProgressDispatcher(
            lambda current, total: calls.append((current, total)),
            100,
            interval=0,
        )
        await dispatcher.add(100)
        await dispatcher.finish()
        self.assertEqual(calls, [(100, 100)])


if __name__ == "__main__":
    unittest.main()
