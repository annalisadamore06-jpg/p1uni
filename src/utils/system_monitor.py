"""
System Monitor - Controllo salute CPU/RAM/Connessioni

SPECIFICHE PER QWEN:
- Ogni 60 secondi: check CPU, RAM, disco, connessioni DB
- Ogni heartbeat_interval_min: invia heartbeat Telegram
- Se anomalia: alert immediato Telegram
- Thresholds: CPU > 90%, RAM > 95%, Disco < 10GB
"""

import logging
import time
from typing import Any

import psutil

logger = logging.getLogger("p1uni.utils.monitor")


class SystemMonitor:
    """Monitora la salute del sistema.

    QWEN: esegui in un thread separato (Thread 4 nel main.py).
    """

    def __init__(self, config: dict[str, Any], telegram_bot: Any = None) -> None:
        self.config = config
        self.telegram = telegram_bot
        self.check_interval_sec: int = 60
        self.heartbeat_interval_min: int = config.get("telegram", {}).get("heartbeat_interval_min", 5)
        self.running: bool = False
        self._last_heartbeat: float = 0.0

    def start(self) -> None:
        """Avvia il loop di monitoraggio."""
        self.running = True
        logger.info("System monitor started")

        while self.running:
            try:
                status = self.check_health()
                self._check_alerts(status)
                self._maybe_heartbeat()
                time.sleep(self.check_interval_sec)
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                time.sleep(10)

    def check_health(self) -> dict[str, Any]:
        """Raccoglie metriche di sistema."""
        cpu_pct = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("C:\\")

        status = {
            "cpu_percent": cpu_pct,
            "ram_used_gb": mem.used / (1024 ** 3),
            "ram_total_gb": mem.total / (1024 ** 3),
            "ram_percent": mem.percent,
            "disk_free_gb": disk.free / (1024 ** 3),
            "disk_percent": disk.percent,
        }

        logger.debug(
            f"Health: CPU={cpu_pct:.0f}% RAM={mem.percent:.0f}% "
            f"Disk={disk.free / (1024**3):.0f}GB free"
        )
        return status

    def _check_alerts(self, status: dict[str, Any]) -> None:
        """Controlla se ci sono anomalie e invia alert."""
        if self.telegram is None:
            return

        if status["cpu_percent"] > 90:
            self.telegram.send_alert(f"CPU alta: {status['cpu_percent']:.0f}%", "WARNING")

        if status["ram_percent"] > 95:
            self.telegram.send_alert(
                f"RAM critica: {status['ram_percent']:.0f}% "
                f"({status['ram_used_gb']:.0f}/{status['ram_total_gb']:.0f} GB)",
                "ERROR",
            )

        if status["disk_free_gb"] < 10:
            self.telegram.send_alert(
                f"Disco quasi pieno: {status['disk_free_gb']:.1f} GB liberi",
                "WARNING",
            )

    def _maybe_heartbeat(self) -> None:
        """Invia heartbeat se e' passato abbastanza tempo."""
        now = time.time()
        if now - self._last_heartbeat >= self.heartbeat_interval_min * 60:
            if self.telegram is not None:
                self.telegram.send_heartbeat()
            self._last_heartbeat = now

    def stop(self) -> None:
        """Ferma il monitor."""
        self.running = False
        logger.info("System monitor stopped")
