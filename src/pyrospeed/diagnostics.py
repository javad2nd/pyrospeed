from __future__ import annotations

import importlib
import importlib.metadata
import json
import platform
import sys


def _pkg(name: str) -> dict[str, object]:
    try:
        version = importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return {"installed": False}
    info: dict[str, object] = {"installed": True, "version": version}
    try:
        module_name = "pyrogram" if name in {"kurigram", "pyrofork"} else name
        module = importlib.import_module(module_name)
        info["import"] = module.__name__
        info["runtime_version"] = getattr(module, "__version__", "unknown")
    except Exception as exc:  # noqa: BLE001
        info["import_error"] = f"{type(exc).__name__}: {exc}"
    return info


def collect() -> dict[str, object]:
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "implementations": {
            name: _pkg(name)
            for name in ("pyrogram", "kurigram", "pyrofork", "hydrogram", "tgcrypto")
        },
    }


def main() -> None:
    print(json.dumps(collect(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
