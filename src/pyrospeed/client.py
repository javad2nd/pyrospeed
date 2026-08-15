from __future__ import annotations

from typing import Any, Optional

from .compat import FrameworkBinding, validate_client
from .config import TransferConfig
from .download import ParallelDownloader
from .patch import InstanceUploadPatch
from .upload import ParallelUploader


class PyroSpeed:
    """High-level facade for accelerated Pyrogram-compatible transfers."""

    def __init__(self, client: Any, config: Optional[TransferConfig] = None) -> None:
        validate_client(client)
        self.client = client
        self.config = config or TransferConfig.balanced()
        self.binding = FrameworkBinding.from_client(client)
        self._native_save_file = client.save_file
        self.uploader = ParallelUploader(
            client,
            self.config.upload,
            binding=self.binding,
            native_save_file=self._native_save_file,
        )
        self.downloader = ParallelDownloader(
            client,
            self.config.download,
            binding=self.binding,
        )
        self.patch = InstanceUploadPatch(client, self.uploader)

    def install(self) -> "PyroSpeed":
        """Make all high-level upload methods on this Client use pyrospeed."""
        self.patch.install()
        return self

    def uninstall(self) -> None:
        self.patch.uninstall()

    async def __aenter__(self) -> "PyroSpeed":
        self.install()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.uninstall()

    async def upload_file(self, path: Any, **kwargs: Any) -> Any:
        """Upload a file and return Telegram's InputFile/InputFileBig object."""
        return await self.uploader.save_file(path, **kwargs)

    async def send_document(self, chat_id: Any, document: Any, **kwargs: Any) -> Any:
        """Accelerated drop-in wrapper around Client.send_document()."""
        with self.patch.active():
            return await self.client.send_document(chat_id, document, **kwargs)

    async def send_video(self, chat_id: Any, video: Any, **kwargs: Any) -> Any:
        with self.patch.active():
            return await self.client.send_video(chat_id, video, **kwargs)

    async def send_audio(self, chat_id: Any, audio: Any, **kwargs: Any) -> Any:
        with self.patch.active():
            return await self.client.send_audio(chat_id, audio, **kwargs)

    async def send_animation(self, chat_id: Any, animation: Any, **kwargs: Any) -> Any:
        with self.patch.active():
            return await self.client.send_animation(chat_id, animation, **kwargs)

    async def send(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Accelerate any high-level Client method that internally calls save_file()."""
        method = getattr(self.client, method_name)
        with self.patch.active():
            return await method(*args, **kwargs)

    async def download(self, media: Any, file_name: Optional[str] = None, **kwargs: Any) -> Any:
        return await self.downloader.download(media, file_name=file_name, **kwargs)

    def capabilities(self) -> dict[str, Any]:
        return {
            "framework": self.binding.root,
            "framework_version": self.binding.version,
            "parallel_upload": True,
            "parallel_download": hasattr(self.client, "get_file"),
            "upload_connections": self.config.upload.connections,
            "upload_inflight_per_connection": self.config.upload.inflight_per_connection,
            "upload_buffer_bytes_approx": self.config.upload.approximate_buffer_bytes,
            "download_workers": self.config.download.workers,
        }


def accelerate(client: Any, config: Optional[TransferConfig] = None) -> PyroSpeed:
    return PyroSpeed(client, config=config)
