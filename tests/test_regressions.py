"""Offline regression tests for failures not covered by the original suite."""
from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pyrospeed.config import DownloadConfig, UploadConfig
from pyrospeed.download import ParallelDownloader
from pyrospeed.errors import IncompleteDownloadError, TransferConfigurationError
from pyrospeed.fast_stream import ParallelStreamConfig, buffered_stream
from pyrospeed.media_pool import MediaConnectionConfig, MediaConnectionPool
from pyrospeed.patch import InstanceStreamPatch, InstanceUploadPatch, original_method
from pyrospeed.progress import ProgressDispatcher
from pyrospeed.session_factory import SessionFactory, SessionLease
from pyrospeed.stream import StreamManager, StreamPipeline
from pyrospeed.upload import ParallelUploader

from test_core import (
    BINDING, DownloadClient, FakeFactory, FakeSession, Media, PatchClient,
    StubUploader, UploadClient,
)


class TruncatedClient(DownloadClient):
    async def get_file(self, *args, **kwargs):
        async for chunk in super().get_file(*args, **kwargs):
            yield chunk[:-1] if kwargs.get("offset", 0) == 0 else chunk


class OverflowClient(DownloadClient):
    async def get_file(self, *args, **kwargs):
        async for chunk in super().get_file(*args, **kwargs):
            yield chunk + b"extra" if kwargs.get("offset", 0) == 0 else chunk


class DefensiveDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_truncated_segment_never_produces_successful_file(self):
        payload = os.urandom(3 * 1024 * 1024 + 101)
        client = TruncatedClient(payload)
        download = ParallelDownloader(client, DownloadConfig(segment_chunks=1), binding=BINDING)
        with tempfile.TemporaryDirectory() as td:
            dest = Path(td) / "file.bin"
            with self.assertRaises(IncompleteDownloadError):
                await download.download(Media(len(payload)), str(dest))
            self.assertFalse(dest.exists())
            self.assertEqual(list(Path(td).glob("*.part")), [])

    async def test_excess_segment_data_rejected(self):
        payload = b"a" * (1024 * 1024)
        download = ParallelDownloader(OverflowClient(payload), DownloadConfig(), binding=BINDING)
        with tempfile.TemporaryDirectory() as td:
            dest = Path(td) / "file.bin"
            with self.assertRaises(IncompleteDownloadError):
                await download.download(Media(len(payload)), str(dest))
            self.assertFalse(dest.exists())
            self.assertEqual(list(Path(td).glob("*.part")), [])

    async def test_untrusted_media_filename_remains_in_requested_directory(self):
        payload = b"A" * 12
        downloader = ParallelDownloader(DownloadClient(payload), DownloadConfig(), binding=BINDING)
        with tempfile.TemporaryDirectory() as td:
            for name in ("../../escape.bin", "..\\..\\escape.bin", "CON.txt"):
                media = Media(len(payload))
                media.file_name = name
                result = await downloader.download(media, td + "/")
                self.assertEqual(Path(result).parent, Path(td))
                self.assertEqual(Path(result).read_bytes(), payload)
            self.assertFalse((Path(td).parent / "escape.bin").exists())

    async def test_simultaneous_same_destination_is_atomic_and_temp_unique(self):
        first = b"A" * (2 * 1024 * 1024 + 53)
        second = b"B" * (2 * 1024 * 1024 + 53)
        a = ParallelDownloader(DownloadClient(first), DownloadConfig(max_parallel_files=2), binding=BINDING)
        b = ParallelDownloader(DownloadClient(second), DownloadConfig(max_parallel_files=2), binding=BINDING)
        with tempfile.TemporaryDirectory() as td:
            dest = Path(td) / "same.bin"
            await asyncio.gather(
                a.download(Media(len(first)), str(dest)),
                b.download(Media(len(second)), str(dest)),
            )
            self.assertIn(dest.read_bytes(), (first, second))
            self.assertEqual(list(Path(td).glob("*.part")), [])

    async def test_shared_client_semaphore_is_unchanged_while_running(self):
        client = DownloadClient(b"a" * (1024 * 1024))
        semaphore = client.get_file_semaphore
        downloader = ParallelDownloader(client, DownloadConfig(override_client_semaphore=True), binding=BINDING)
        with tempfile.TemporaryDirectory() as td:
            await downloader.download(Media(len(client.payload)), str(Path(td) / "out"))
        self.assertIs(client.get_file_semaphore, semaphore)


class ExtendedPatchClient(PatchClient):
    async def stream_media(self):
        yield b"native"


class DefensivePatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_uninstall_stream_restores_class_defined_method(self):
        client = ExtendedPatchClient()
        manager = StreamManager(client)
        stream_patch = InstanceStreamPatch(client, manager.stream_media)
        stream_patch.install()
        self.assertEqual([c async for c in client.stream_media()], [b"native"])
        stream_patch.uninstall()
        self.assertNotIn("stream_media", client.__dict__)
        self.assertNotIn("_pyrospeed_native_stream_media", client.__dict__)
        self.assertEqual([c async for c in client.stream_media()], [b"native"])

    async def test_multiple_owners_removed_out_of_order(self):
        client = ExtendedPatchClient()
        first = InstanceUploadPatch(client, StubUploader())

        class Second:
            async def save_file(self, value):
                return f"second:{value}"

        second = InstanceUploadPatch(client, Second())
        first.install()
        second.install()
        self.assertEqual(await client.save_file("a"), "second:a")
        self.assertEqual(await original_method(client, "save_file")("a"), "native:a")
        first.uninstall()  # Out-of-order removal must not re-install first.
        self.assertEqual(await client.save_file("a"), "second:a")
        second.uninstall()
        self.assertEqual(await client.save_file("a"), "native:a")
        self.assertNotIn("save_file", client.__dict__)

    async def test_multiple_stream_patches_uninstall_out_of_order(self):
        client = ExtendedPatchClient()
        a = InstanceStreamPatch(client, StreamManager(client).stream_media)
        b = InstanceStreamPatch(client, StreamManager(client).stream_media)
        a.install()
        b.install()
        a.uninstall()
        self.assertEqual([c async for c in client.stream_media()], [b"native"])
        b.uninstall()
        self.assertNotIn("stream_media", client.__dict__)

    async def test_multiple_scopes_are_reference_counted(self):
        client = PatchClient()
        owner = InstanceUploadPatch(client, StubUploader())
        with owner.active():
            with owner.active():
                self.assertEqual(await client.save_file("z"), "fast:z")
            self.assertEqual(await client.save_file("z"), "fast:z")
        self.assertEqual(await client.save_file("z"), "native:z")


class StreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_failure_propagates_to_consumer(self):
        async def source():
            yield b"first"
            raise RuntimeError("stream was interrupted")

        chunks = []
        with self.assertRaisesRegex(RuntimeError, "stream was interrupted"):
            async for chunk in StreamPipeline().process(source()):
                chunks.append(chunk)
        self.assertEqual(chunks, [b"first"])

    async def test_early_stream_close_cancels_producer(self):
        source_closed = asyncio.Event()

        async def source():
            try:
                yield b"one"
                await asyncio.Event().wait()
            finally:
                source_closed.set()

        iterator = StreamPipeline().process(source())
        self.assertEqual(await iterator.__anext__(), b"one")
        await asyncio.wait_for(iterator.aclose(), timeout=2)
        await asyncio.wait_for(source_closed.wait(), timeout=2)

    async def test_legacy_stream_failure_propagates(self):
        async def source():
            yield b"hi"
            raise RuntimeError("fail")

        with self.assertRaisesRegex(RuntimeError, "fail"):
            _ = [chunk async for chunk in buffered_stream(source(), ParallelStreamConfig())]


class ProgressOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_callbacks_are_serialized_in_snapshot_order(self):
        calls = []

        async def callback(current, total):
            if current == 1:
                await asyncio.sleep(0.03)
            calls.append(current)

        dispatcher = ProgressDispatcher(callback, 2, interval=0)
        await asyncio.gather(dispatcher.add(1), dispatcher.add(1))
        await dispatcher.finish()
        self.assertEqual(calls, [1, 2])


class UploadDefenses(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_upload_part_is_failure(self):
        class RejectSession(FakeSession):
            async def invoke(self, rpc):
                return False

        class RejectFactory(FakeFactory):
            async def create_media_session(self, dc_id):
                lease = SessionLease(RejectSession())
                self.sessions.append(lease.session)
                return lease

        client = UploadClient()
        factory = RejectFactory()
        uploader = ParallelUploader(client, UploadConfig(connections=1, small_file_strategy="parallel"),
                                    binding=BINDING, session_factory=factory)
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"abc")
            name = fp.name
        try:
            with self.assertRaisesRegex(OSError, "rejected upload part"):
                await uploader.save_file(name)
        finally:
            os.unlink(name)
        self.assertTrue(all(s.closed for s in factory.sessions))

    async def test_legacy_lower_threshold_cannot_force_big_protocol(self):
        client = UploadClient()
        factory = FakeFactory()
        uploader = ParallelUploader(client, UploadConfig(large_file_threshold=1, connections=1),
                                    binding=BINDING, session_factory=factory)
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"short")
            name = fp.name
        try:
            result = await uploader.save_file(name)
        finally:
            os.unlink(name)
        self.assertEqual(type(result).__name__, "InputFile")
        self.assertEqual(type(factory.sessions[0].calls[0]).__name__, "SaveFilePart")

    async def test_files_above_10_mib_use_big_parts(self):
        client = UploadClient()
        factory = FakeFactory()
        cfg = UploadConfig(connections=2)
        uploader = ParallelUploader(client, cfg, binding=BINDING, session_factory=factory)
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(b"x" * (10 * 1024 * 1024 + 1))
            name = fp.name
        try:
            result = await uploader.save_file(name)
        finally:
            os.unlink(name)
        self.assertEqual(type(result).__name__, "InputFileBig")
        self.assertTrue(all(
            type(rpc).__name__ == "SaveBigFilePart"
            for session in factory.sessions for rpc in session.calls
        ))

    async def test_excess_parts_preflight(self):
        client = UploadClient()
        uploader = ParallelUploader(client, UploadConfig(part_size=1024, large_file_threshold=1),
                                    binding=BINDING, session_factory=FakeFactory())
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.truncate(9 * 1024 * 1024)
            name = fp.name
        try:
            with self.assertRaisesRegex(ValueError, "8000-part"):
                await uploader.save_file(name)
        finally:
            os.unlink(name)

    async def test_invalid_config_enum_rejected(self):
        with self.assertRaises(TransferConfigurationError):
            UploadConfig(disk_io="garbage")
        with self.assertRaises(TransferConfigurationError):
            DownloadConfig(disk_io="garbage")


class BrokenPoolFactory:
    def __init__(self):
        self.leases = []

    async def create_media_session(self, dc_id):
        if len(self.leases) == 1:
            raise RuntimeError("second connection failed")
        lease = SessionLease(FakeSession())
        self.leases.append(lease)
        return lease


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_start_closes_partially_acquired_sessions(self):
        factory = BrokenPoolFactory()
        pool = MediaConnectionPool(factory, MediaConnectionConfig(connections=2))
        with self.assertRaisesRegex(RuntimeError, "second connection failed"):
            await pool.start(2)
        self.assertTrue(factory.leases[0].session.closed)
        self.assertFalse(pool._started)

    async def test_close_waits_for_borrowed_session(self):
        class GoodFactory:
            async def create_media_session(self, dc_id):
                return SessionLease(FakeSession())

        pool = MediaConnectionPool(GoodFactory(), MediaConnectionConfig(connections=1))
        await pool.start(2)
        lease = await pool.acquire()
        closer = asyncio.create_task(pool.close())
        await asyncio.sleep(0)
        self.assertFalse(closer.done())
        self.assertFalse(lease.session.closed)
        await pool.release(lease)
        await asyncio.wait_for(closer, 2)
        self.assertTrue(lease.session.closed)


class SessionFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_start_error_closes_owned_session(self):
        sessions = []

        class Session:
            def __init__(self, *args, **kwargs):
                self.closed = False
                sessions.append(self)

            async def start(self):
                raise RuntimeError("cannot start")

            async def stop(self):
                self.closed = True

        class Storage:
            async def dc_id(self): return 2
            async def test_mode(self): return False
            async def auth_key(self): return b"key"

        client = SimpleNamespace(storage=Storage())
        binding = SimpleNamespace(root="dummy")
        mod = SimpleNamespace(Session=Session, Auth=object)
        with patch("pyrospeed.session_factory.importlib.import_module", return_value=mod):
            with self.assertRaisesRegex(RuntimeError, "cannot start"):
                await SessionFactory(client, binding).create_media_session(2)
        self.assertTrue(sessions[0].closed)


if __name__ == "__main__":
    unittest.main()
