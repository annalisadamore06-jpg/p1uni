"""
Logger - Logging centralizzato per P1UNI

SPECIFICHE PER QWEN:
- Un logger per modulo (e.g., "p1uni.ingestion.databento")
- Output: console (INFO+) e file rotante (DEBUG+) in logs/
- Formato: "[2026-04-08 14:30:00 UTC] [INFO] [ingestion.databento] Flushed 1500 trades"
- File rotante: 10MB max, 5 file backup
- Livello configurabile da settings.yaml
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> None:
    """Configura il logging centralizzato per tutto il sistema.

    Args:
        log_level: Livello minimo per console (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory per i file di log
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Formato
    fmt = "[%(asctime)s UTC] [%(levelname)s] [%(name)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    import time as _time
    formatter = logging.Formatter(fmt, datefmt=datefmt)
    formatter.converter = _time.gmtime  # Use actual UTC timestamps

    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Prevent duplicate handlers on repeated calls
    if root.handlers:
        root.handlers.clear()

    # Console handler (INFO+)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    console.setFormatter(formatter)
    root.addHandler(console)

    # File handler rotante (DEBUG+, 10MB, 5 backup)
    file_handler = RotatingFileHandler(
        log_path / "p1uni.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # File separato per errori
    error_handler = RotatingFileHandler(
        log_path / "p1uni_errors.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root.addHandler(error_handler)

    logging.info(f"Logging initialized: level={log_level}, dir={log_path}")
