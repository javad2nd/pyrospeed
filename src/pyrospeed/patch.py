from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from .upload import ParallelUploader


_MISSING = object()


class InstanceUploadPatch:
    """Patch only one client instance; never mutates Client globally.

    The scoped context is reference-counted so overlapping async sends using the
    same PyroSpeed object cannot restore ``save_file`` while another send is
    still in progress.
    """

    def __init__(self, client: Any, uploader: ParallelUploader) -> None:
        self.client = client
        self.uploader = uploader
        self._previous: Any = _MISSING
        self._installed = False
        self._manual = False
        self._depth = 0

    def _apply(self) -> None:
        if self._installed:
            return
        self._previous = self.client.__dict__.get("save_file", _MISSING)
        self.client.save_file = self.uploader.save_file
        self._installed = True

    def _restore(self) -> None:
        if not self._installed:
            return
        if self._previous is _MISSING:
            self.client.__dict__.pop("save_file", None)
        else:
            self.client.save_file = self._previous
        self._installed = False
        self._previous = _MISSING

    def install(self) -> None:
        self._manual = True
        self._apply()

    def uninstall(self) -> None:
        self._manual = False
        if self._depth == 0:
            self._restore()

    @contextmanager
    def active(self):
        self._depth += 1
        self._apply()
        try:
            yield self
        finally:
            self._depth -= 1
            if self._depth == 0 and not self._manual:
                self._restore()


class InstanceStreamPatch:
    """Scoped stream_media monkey patch using PyroSpeed async pipeline."""

    def __init__(self, client: Any, stream_wrapper: Any) -> None:
        self.client = client
        self.stream_wrapper = stream_wrapper
        self.previous = _MISSING

    def install(self) -> None:
        if self.previous is not _MISSING:
            return
        self.previous = self.client.__dict__.get("stream_media", _MISSING)
        if "_pyrospeed_native_stream_media" not in self.client.__dict__:
            self.client._pyrospeed_native_stream_media = self.client.stream_media
        self.client.stream_media = self.stream_wrapper

    def uninstall(self) -> None:
        if self.previous is _MISSING:
            return
        self.client.__dict__.pop("_pyrospeed_native_stream_media", None)
        self.client.__dict__.pop("stream_media", None)
        if self.previous is not _MISSING:
            self.client.stream_media = self.previous
        self.previous = _MISSING
