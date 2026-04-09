"""
Secrets loader — legge .env o env vars di sistema.

MAI hardcodare key/token nei file sorgente.
Tutti i secrets devono venire da qui.

Usage:
    from src.core.secrets import get_secret
    databento_key = get_secret("DATABENTO_API_KEY")
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

_ENV_LOADED = False


def _load_env_file() -> None:
    """Carica .env file dalla root del progetto."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value

    _ENV_LOADED = True


def get_secret(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """Ritorna un secret da env var o .env file.

    Args:
        name: nome della variabile (es. "DATABENTO_API_KEY")
        default: valore default se non trovato
        required: se True, solleva ValueError se mancante

    Returns:
        Il valore del secret, o default, o None.
    """
    _load_env_file()
    value = os.environ.get(name, default)
    if required and not value:
        raise ValueError(f"Required secret '{name}' not found in env or .env file")
    return value
