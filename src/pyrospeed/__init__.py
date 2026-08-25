from .client import PyroSpeed, accelerate
from .config import DownloadConfig, TransferConfig, UploadConfig
from .stream import StreamConfig
from .errors import CompatibilityError, PyroSpeedError, TransferConfigurationError, UnknownFileSizeError
from .progress import TransferSnapshot

__all__ = [
    "PyroSpeed",
    "accelerate",
    "TransferConfig",
    "UploadConfig",
    "DownloadConfig",
    "StreamConfig",
    "TransferSnapshot",
    "PyroSpeedError",
    "CompatibilityError",
    "TransferConfigurationError",
    "UnknownFileSizeError",
]

__version__ = "0.2.0"

from .media_pool import MediaConnectionPool, MediaConnectionConfig
