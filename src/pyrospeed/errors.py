class PyroSpeedError(Exception):
    """Base exception for pyrospeed."""


class CompatibilityError(PyroSpeedError):
    """The supplied client does not expose the required Pyrogram-compatible API."""


class TransferConfigurationError(PyroSpeedError, ValueError):
    """Invalid transfer tuning."""


class UnknownFileSizeError(PyroSpeedError):
    """Parallel download requires a known file size."""


class IncompleteDownloadError(PyroSpeedError, OSError):
    """A media segment was truncated or exceeded its expected byte range."""
