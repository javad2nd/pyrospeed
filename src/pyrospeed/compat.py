from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Optional

from .errors import CompatibilityError


@dataclass(frozen=True, slots=True)
class FrameworkBinding:
    root: str
    raw: Any
    file_id_class: Optional[type]
    stop_transmission: tuple[type[BaseException], ...]
    version: str

    @classmethod
    def from_client(cls, client: Any) -> "FrameworkBinding":
        candidates: list[str] = []
        module_root = client.__class__.__module__.split(".", 1)[0]
        if module_root:
            candidates.append(module_root)
        for root in ("pyrogram", "hydrogram"):
            if root not in candidates:
                candidates.append(root)

        errors: list[str] = []
        for root in candidates:
            try:
                package = importlib.import_module(root)
                raw = importlib.import_module(f"{root}.raw")
            except Exception as exc:  # noqa: BLE001 - diagnostics need all failures
                errors.append(f"{root}: {type(exc).__name__}: {exc}")
                continue

            if not hasattr(raw, "functions") or not hasattr(raw, "types"):
                errors.append(f"{root}: raw API missing functions/types")
                continue

            file_id_class = None
            try:
                file_id_module = importlib.import_module(f"{root}.file_id")
                file_id_class = getattr(file_id_module, "FileId", None)
            except Exception:
                pass

            stop_types: list[type[BaseException]] = []
            stop = getattr(package, "StopTransmission", None)
            if isinstance(stop, type) and issubclass(stop, BaseException):
                stop_types.append(stop)

            return cls(
                root=root,
                raw=raw,
                file_id_class=file_id_class,
                stop_transmission=tuple(stop_types),
                version=str(getattr(package, "__version__", "unknown")),
            )

        raise CompatibilityError(
            "Could not locate a Pyrogram-compatible framework. Tried: " + "; ".join(errors)
        )

    def decode_file_id(self, value: str) -> Any:
        if self.file_id_class is None or not hasattr(self.file_id_class, "decode"):
            raise CompatibilityError(f"{self.root}.file_id.FileId.decode is unavailable")
        return self.file_id_class.decode(value)


def validate_client(client: Any) -> None:
    required = ("save_file", "download_media", "get_file", "storage", "rnd_id")
    missing = [name for name in required if not hasattr(client, name)]
    if missing:
        raise CompatibilityError(
            "Client is not sufficiently Pyrogram-compatible; missing: " + ", ".join(missing)
        )
