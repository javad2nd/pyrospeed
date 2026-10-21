from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from typing import Any, Optional

from .compat import FrameworkBinding
from .errors import CompatibilityError


@dataclass(slots=True)
class SessionLease:
    session: Any
    owned: bool = True

    async def close(self) -> None:
        if self.owned and hasattr(self.session, "stop"):
            await self.session.stop()


class SessionFactory:
    """Creates independent media sessions without binding to one fork version."""

    def __init__(self, client: Any, binding: FrameworkBinding) -> None:
        self.client = client
        self.binding = binding

    async def create_media_session(self, dc_id: int) -> SessionLease:
        # Some forks expose a temporary session helper. Prefer it when its
        # signature explicitly supports temporary=True.
        get_session = getattr(self.client, "get_session", None)
        if callable(get_session):
            try:
                params = inspect.signature(get_session).parameters
            except (TypeError, ValueError):
                params = {}
            if "temporary" in params:
                session = await get_session(dc_id, is_media=True, temporary=True)
                return SessionLease(session=session, owned=True)

        # Stock Pyrogram and most forks export Session/Auth from <root>.session.
        try:
            session_mod = importlib.import_module(f"{self.binding.root}.session")
            Session = getattr(session_mod, "Session")
            Auth = getattr(session_mod, "Auth")
        except Exception as exc:  # noqa: BLE001
            raise CompatibilityError(
                f"Cannot construct media session for {self.binding.root}: {exc}"
            ) from exc

        storage = self.client.storage
        test_mode = await storage.test_mode()
        home_dc = await storage.dc_id()
        if dc_id == home_dc:
            auth_key = await storage.auth_key()
        else:
            auth_key = await Auth(self.client, dc_id, test_mode).create()

        session = Session(self.client, dc_id, auth_key, test_mode, is_media=True)
        try:
            await session.start()
            if dc_id != home_dc:
                exported = await self.client.invoke(
                    self.binding.raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                await session.invoke(
                    self.binding.raw.functions.auth.ImportAuthorization(
                        id=exported.id,
                        bytes=exported.bytes,
                    )
                )
        except BaseException:
            # A partially-started session must not survive failed authorization
            # or cancellation. Preserve the original error on cleanup failure.
            try:
                await session.stop()
            except BaseException:
                pass
            raise
        return SessionLease(session=session, owned=True)
