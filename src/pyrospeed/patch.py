from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from .upload import ParallelUploader


_MISSING = object()
_STATE_ATTR = "_pyrospeed_patch_states"
_NATIVE_STREAM_ATTR = "_pyrospeed_native_stream_media"


class _PatchState:
    """Per-client, per-method patch stack, including inherited original methods."""

    def __init__(self, client: Any, method: str) -> None:
        self.client = client
        self.method = method
        self.original_slot = client.__dict__.get(method, _MISSING)
        self.original_method = getattr(client, method)
        self.owners: list[Any] = []
        self.native_stream_slot = client.__dict__.get(_NATIVE_STREAM_ATTR, _MISSING)

    def refresh(self) -> None:
        if self.owners:
            self.client.__dict__[self.method] = self.owners[-1].replacement
            if self.method == "stream_media":
                self.client.__dict__[_NATIVE_STREAM_ATTR] = self.original_method
            return

        if self.original_slot is _MISSING:
            self.client.__dict__.pop(self.method, None)
        else:
            self.client.__dict__[self.method] = self.original_slot
        if self.method == "stream_media":
            if self.native_stream_slot is _MISSING:
                self.client.__dict__.pop(_NATIVE_STREAM_ATTR, None)
            else:
                self.client.__dict__[_NATIVE_STREAM_ATTR] = self.native_stream_slot


def original_method(client: Any, method: str) -> Any:
    """Return the unpatched method, even if another PyroSpeed facade is active."""
    state = client.__dict__.get(_STATE_ATTR, {}).get(method)
    return state.original_method if state is not None else getattr(client, method)


class _InstancePatch:
    def __init__(self, client: Any, method: str, replacement: Any) -> None:
        self.client = client
        self.method = method
        self.replacement = replacement
        self._installed = False

    def _apply(self) -> None:
        if self._installed:
            return
        states = self.client.__dict__.setdefault(_STATE_ATTR, {})
        state = states.get(self.method)
        if state is None:
            state = states[self.method] = _PatchState(self.client, self.method)
        state.owners.append(self)
        self._installed = True
        state.refresh()

    def _restore(self) -> None:
        if not self._installed:
            return
        states = self.client.__dict__[_STATE_ATTR]
        state = states[self.method]
        state.owners.remove(self)
        self._installed = False
        state.refresh()
        if not state.owners:
            states.pop(self.method)
        if not states:
            self.client.__dict__.pop(_STATE_ATTR, None)


class InstanceUploadPatch(_InstancePatch):
    """Reversible instance-only patch; concurrent scopes and multiple owners are safe."""

    def __init__(self, client: Any, uploader: ParallelUploader) -> None:
        self.uploader = uploader
        super().__init__(client, "save_file", uploader.save_file)
        self._manual = False
        self._depth = 0

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


class InstanceStreamPatch(_InstancePatch):
    """Reversible stream_media patch preserving class-level and instance methods."""

    def __init__(self, client: Any, stream_wrapper: Any) -> None:
        super().__init__(client, "stream_media", stream_wrapper)

    def install(self) -> None:
        # Some compatible clients have no stream_media. Upload/download still work.
        if not hasattr(self.client, "stream_media"):
            return
        self._apply()

    def uninstall(self) -> None:
        self._restore()
