from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .errors import TransferConfigurationError


@dataclass(frozen=True, slots=True)
class UploadConfig:
    """Tuning for a single large-file upload."""

    connections: int = 4
    inflight_per_connection: int = 2
    queue_factor: int = 2
    part_size: int = 512 * 1024
    large_file_threshold: int = 10 * 1024 * 1024
    progress_interval: float = 0.25
    disk_io: Literal["thread", "inline"] = "thread"
    small_file_strategy: Literal["native", "parallel"] = "native"
    max_parallel_files: int = 1

    def __post_init__(self) -> None:
        if not 1 <= self.connections <= 32:
            raise TransferConfigurationError("upload.connections must be in 1..32")
        if not 1 <= self.inflight_per_connection <= 16:
            raise TransferConfigurationError("upload.inflight_per_connection must be in 1..16")
        if not 1 <= self.queue_factor <= 16:
            raise TransferConfigurationError("upload.queue_factor must be in 1..16")
        if self.part_size <= 0 or self.part_size % 1024 != 0 or 512 * 1024 % self.part_size != 0:
            raise TransferConfigurationError(
                "upload.part_size must divide 512 KiB evenly and be a multiple of 1 KiB"
            )
        if self.progress_interval < 0:
            raise TransferConfigurationError("upload.progress_interval must be >= 0")
        if not 0 <= self.large_file_threshold <= 10 * 1024 * 1024:
            raise TransferConfigurationError(
                "upload.large_file_threshold must be in 0..10 MiB; Telegram requires big-file mode above 10 MiB"
            )
        if not 1 <= self.max_parallel_files <= 16:
            raise TransferConfigurationError("upload.max_parallel_files must be in 1..16")

    @property
    def max_inflight_chunks(self) -> int:
        return self.connections * self.inflight_per_connection

    @property
    def approximate_buffer_bytes(self) -> int:
        return self.max_inflight_chunks * self.queue_factor * self.part_size


@dataclass(frozen=True, slots=True)
class DownloadConfig:
    """Tuning for parallel ranged downloads through Client.get_file()."""

    workers: int = 4
    segment_chunks: int = 4
    chunk_size: int = 1024 * 1024
    queue_factor: int = 2
    progress_interval: float = 0.25
    disk_io: Literal["thread", "inline"] = "thread"
    override_client_semaphore: bool = True
    fallback_to_native_for_unknown_size: bool = True
    max_parallel_files: int = 1

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 32:
            raise TransferConfigurationError("download.workers must be in 1..32")
        if not 1 <= self.segment_chunks <= 128:
            raise TransferConfigurationError("download.segment_chunks must be in 1..128")
        if self.chunk_size != 1024 * 1024:
            raise TransferConfigurationError(
                "download.chunk_size must be 1 MiB because Pyrogram get_file offsets are 1-MiB units"
            )
        if not 1 <= self.queue_factor <= 16:
            raise TransferConfigurationError("download.queue_factor must be in 1..16")
        if self.progress_interval < 0:
            raise TransferConfigurationError("download.progress_interval must be >= 0")
        if not 1 <= self.max_parallel_files <= 16:
            raise TransferConfigurationError("download.max_parallel_files must be in 1..16")


@dataclass(frozen=True, slots=True)
class TransferConfig:
    upload: UploadConfig = field(default_factory=UploadConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)

    @classmethod
    def conservative(cls) -> "TransferConfig":
        return cls(
            upload=UploadConfig(connections=2, inflight_per_connection=1, queue_factor=1),
            download=DownloadConfig(workers=2, segment_chunks=2),
        )

    @classmethod
    def balanced(cls) -> "TransferConfig":
        return cls()

    @classmethod
    def aggressive(cls) -> "TransferConfig":
        return cls(
            upload=UploadConfig(connections=8, inflight_per_connection=2, queue_factor=2),
            download=DownloadConfig(workers=8, segment_chunks=4),
        )
