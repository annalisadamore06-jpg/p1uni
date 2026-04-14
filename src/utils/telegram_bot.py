"""
Telegram Bot - Invio alert via Telegram

SPECIFICHE PER QWEN:
- Token e chat_id da config (settings.yaml)
- send_signal(): quando si apre un trade
- send_alert(): errori, warning, info
- send_daily_summary(): fine giornata con P&L
- Rate limit: max 1 messaggio ogni 3 secondi
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("p1uni.utils.telegram")


class TelegramBot:
    """Invia notifiche via Telegram Bot API.

    QWEN: usa l'API HTTP di Telegram direttamente (requests).
    Non serve la libreria python-telegram-bot per invio semplice.
    """

    BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, config: dict[str, Any]) -> None:
        tg_cfg = config.get("telegram", {})
        self.token: str = tg_cfg.get("token", "")
        self.chat_id: str = tg_cfg.get("chat_id", "")
        self.rate_limit_sec: float = tg_cfg.get("rate_limit_sec", 3.0)
        self._last_send: float = 0.0
        self._consecutive_failures: int = 0
        self.enabled: bool = bool(self.token and self.chat_id)

        if not self.enabled:
            logger.warning("Telegram bot disabled: missing token or chat_id")

    def _send(self, text: str) -> bool:
        """Invia un messaggio con rate limiting. NON blocca mai il thread."""
        if not self.enabled:
            return False

        # Rate limit: skip se troppo frequente (NON sleep, NON bloccare)
        now = time.time()
        if now - self._last_send < self.rate_limit_sec:
            return False  # skip silenziosamente

        # Se l'ultimo invio ha fallito, aspetta piu' a lungo (backoff)
        if self._consecutive_failures > 0:
            backoff = min(60, self.rate_limit_sec * (2 ** self._consecutive_failures))
            if now - self._last_send < backoff:
                return False

        try:
            url = self.BASE_URL.format(token=self.token)
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }, timeout=5)  # timeout ridotto a 5s per non bloccare

            if resp.status_code == 200:
                self._last_send = now
                self._consecutive_failures = 0
                return True
            else:
                self._consecutive_failures += 1
                if self._consecutive_failures <= 2:  # log solo i primi errori
                    logger.error(f"Telegram API error: {resp.status_code}")
                return False
        except Exception as e:
            self._consecutive_failures += 1
            if self._consecutive_failures <= 2:
                logger.error(f"Telegram send error: {type(e).__name__}")
            return False

    def send_signal(self, direction: str, confidence: float, spot: float, phase: str) -> bool:
        """Notifica apertura trade."""
        emoji = "\U0001f7e2" if direction == "LONG" else "\U0001f534"  # green/red circle
        safe_phase = phase.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        text = (
            f"{emoji} <b>SIGNAL: {direction}</b>\n"
            f"Confidence: {confidence:.1%}\n"
            f"Spot: {spot:.2f}\n"
            f"Phase: {safe_phase}\n"
            f"Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )
        return self._send(text)

    def send_alert(self, message: str, level: str = "INFO") -> bool:
        """Invia alert generico. Escape HTML per evitare errori 400."""
        # Escape caratteri HTML nel messaggio (evita <HTTPSConnection> etc.)
        safe_msg = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        # Tronca messaggi troppo lunghi
        if len(safe_msg) > 500:
            safe_msg = safe_msg[:497] + "..."
        icons = {"INFO": "\u2139\ufe0f", "WARNING": "\u26a0\ufe0f", "ERROR": "\u274c", "CRITICAL": "\U0001f6a8"}
        icon = icons.get(level, "\u2139\ufe0f")
        text = f"{icon} <b>{level}</b>: {safe_msg}"
        return self._send(text)

    def send_daily_summary(self, risk_status: dict[str, Any]) -> bool:
        """Invia riassunto giornaliero."""
        pnl = risk_status.get("daily_pnl_pts", 0)
        pnl_usd = risk_status.get("daily_pnl_usd", 0)
        trades = risk_status.get("total_trades", 0)
        wr = risk_status.get("win_rate", 0)
        emoji = "\U0001f4b0" if pnl >= 0 else "\U0001f4c9"

        text = (
            f"{emoji} <b>DAILY SUMMARY</b>\n"
            f"PnL: {pnl:+.1f} pts (${pnl_usd:+.0f})\n"
            f"Trades: {trades}\n"
            f"Win Rate: {wr:.0%}\n"
            f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        )
        return self._send(text)

    def send_heartbeat(self) -> bool:
        """Heartbeat: sistema vivo."""
        text = "\u2764\ufe0f System alive - " + datetime.now(timezone.utc).strftime("%H:%M UTC")
        return self._send(text)
