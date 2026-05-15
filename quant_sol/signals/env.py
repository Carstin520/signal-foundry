from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


def load_local_env(path: Path = Path(".env")) -> Mapping[str, str]:
    """Load simple KEY=VALUE pairs from .env without overriding shell env vars."""
    if not path.exists():
        return {}
    loaded = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = _clean_value(value)
        if not key:
            continue
        if key not in os.environ:
            os.environ[key] = value
            loaded[key] = value
    return loaded


def has_secret(name: str) -> bool:
    value = os.getenv(name)
    return bool(value and value.strip())


def masked_secret(name: str) -> str:
    value = os.getenv(name) or ""
    if not value:
        return "missing"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def _clean_value(value: str) -> str:
    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        return cleaned[1:-1]
    return cleaned
