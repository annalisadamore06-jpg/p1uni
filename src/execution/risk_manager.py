"""
Risk Manager - Il "Freno a Mano" automatico del sistema di trading.

Se le condizioni di rischio non sono soddisfatte, NESSUN ordine passa,
indipendentemente dalla forza del segnale ML.

CIRCUIT BREAKERS (kill switch):
  1. Max Daily Drawdown: se P&L < -max_daily_loss -> BLOCCA tutto
  2. Max Consecutive Losses: se streak >= N -> BLOCCA per cooldown
  3. Max Daily Trades: limite assoluto giornaliero
  4. Profit Lock: se P&L > +target -> riduci size 50%
  5. Volatilita Estrema: se vol > soglia -> riduci size o blocca

POSITION SIZE DINAMICA:
  Size = Base * ConfidenceFactor * (AvgVol / CurrentVol)
  Arrotondato per difetto. Minimo 1 contratto.

PRE-FLIGHT CHECK (validate_order):
  - R/R ratio >= 1.5
  - Esposizione massima non superata
  - Nessun duplicato (no secondo LONG se gia aperto)

PERSISTENZA:
  Stato salvato in risk_state.json ad ogni cambio.
  Al riavvio, carica lo stato e mantiene i limiti.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger("p1uni.execution.risk_manager")


# ============================================================
# Costanti default
# ============================================================
ES_POINT_VALUE = 50.0  # 1 punto ES = $50


class RiskManager:
    """Gestisce il rischio: circuit breakers, position sizing, pre-flight checks.

    Thread-safe. Stato persistente su file.
    """

    def __init__(self, config: dict[str, Any], db: Any = None) -> None:
        risk_cfg = config.get("risk", {})

        # --- Circuit Breaker thresholds ---
        self.max_daily_loss_pts: float = float(risk_cfg.get("max_daily_loss_pts", 40))
        self.max_consec_losses: int = int(risk_cfg.get("max_consec_losses", 3))
        self.max_daily_trades: int = int(risk_cfg.get("max_daily_trades", 10))
        self.profit_lock_pts: float = float(risk_cfg.get("profit_lock_pts", 60))
        self.profit_lock_size_factor: float = float(risk_cfg.get("profit_lock_size_factor", 0.5))
        self.vol_extreme_threshold: float = float(risk_cfg.get("vol_extreme_threshold", 0.001))

        # --- Position sizing ---
        self.base_size: int = int(risk_cfg.get("base_size", 1))
        self.min_size: int = 1
        self.max_size: int = int(risk_cfg.get("max_size", 5))
        self.avg_vol: float = float(risk_cfg.get("avg_vol", 0.0004))  # volatilita media storica

        # --- Pre-flight ---
        self.min_rr_ratio: float = float(risk_cfg.get("min_rr_ratio", 1.5))
        self.max_exposure_contracts: int = int(risk_cfg.get("max_exposure_contracts", 3))

        # --- Cooldown ---
        self.consec_loss_cooldown_sec: int = int(risk_cfg.get("consec_loss_cooldown_sec", 3600))

        # --- Reset ---
        self.reset_hour_utc: int = int(risk_cfg.get("reset_hour_utc", 22))

        # --- Stato corrente ---
        self._lock = threading.Lock()
        self.daily_pnl_pts: float = 0.0
        self.consecutive_losses: int = 0
        self.total_trades_today: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        self.daily_peak_pnl_pts: float = 0.0   # per trailing drawdown

        self._daily_locked: bool = False
        self._lock_reason: str = ""
        self._consec_loss_locked_until: datetime | None = None

        # Posizioni aperte
        self._open_positions: list[dict[str, Any]] = []

        # Persistenza
        base_dir = Path(config.get("_base_dir", "."))
        self._state_file = base_dir / "data" / "risk_state.json"
        self._state_date: str = ""

        # DB per leggere volatilita corrente
        self.db = db

        # Carica stato persistente
        self._load_state()

    # ============================================================
    # Circuit Breakers — is_trading_blocked()
    # ============================================================

    def is_trading_blocked(self) -> tuple[bool, str]:
        """Controlla TUTTI i circuit breakers.

        Returns:
            (blocked: bool, reason: str). Se blocked=True, reason spiega perche.
        """
        with self._lock:
            # 1. Lock giornaliero esplicito
            if self._daily_locked:
                return True, self._lock_reason

            # 2. Max daily drawdown
            if self.daily_pnl_pts <= -self.max_daily_loss_pts:
                self._set_daily_lock(
                    f"Daily drawdown limit: {self.daily_pnl_pts:+.1f} pts "
                    f"(max: -{self.max_daily_loss_pts})"
                )
                return True, self._lock_reason

            # 3. Max consecutive losses (con cooldown temporale)
            if self.consecutive_losses >= self.max_consec_losses:
                if self._consec_loss_locked_until is not None:
                    now = datetime.now(timezone.utc)
                    if now < self._consec_loss_locked_until:
                        remaining = (self._consec_loss_locked_until - now).total_seconds()
                        return True, (
                            f"Consecutive loss cooldown: {self.consecutive_losses} losses, "
                            f"{remaining/60:.0f} min remaining"
                        )
                    else:
                        # Cooldown scaduto: resetta streak parzialmente
                        self._consec_loss_locked_until = None
                        # Non resettare consecutive_losses: servira un win per quello
                else:
                    # Attiva cooldown
                    self._consec_loss_locked_until = (
                        datetime.now(timezone.utc) + timedelta(seconds=self.consec_loss_cooldown_sec)
                    )
                    return True, (
                        f"Consecutive losses: {self.consecutive_losses} "
                        f"(max: {self.max_consec_losses}). Cooldown {self.consec_loss_cooldown_sec}s"
                    )

            # 4. Max daily trades
            if self.total_trades_today >= self.max_daily_trades:
                return True, (
                    f"Max daily trades reached: {self.total_trades_today}/{self.max_daily_trades}"
                )

            return False, ""

    def _set_daily_lock(self, reason: str) -> None:
        """Blocca il trading per tutto il giorno."""
        self._daily_locked = True
        self._lock_reason = reason
        logger.warning(f"TRADING LOCKED: {reason}")
        self._save_state()

    # ============================================================
    # Position Size Dinamica
    # ============================================================

    def calculate_position_size(
        self,
        signal_confidence: float,
        current_vol: float = 0.0,
        ticker: str = "ES",
    ) -> int:
        """Calcola la position size dinamica.

        Formula: Size = Base * ConfidenceFactor * VolFactor * ProfitLockFactor
        Arrotondato per difetto. Minimo 1, Massimo max_size.

        Args:
            signal_confidence: Probabilita dal modello ML (0.0 - 1.0)
            current_vol: Volatilita corrente (vol_5min dal feature builder)
            ticker: Ticker (per regole specifiche, ora solo ES)

        Returns:
            Numero di contratti (0 se rischio troppo alto).
        """
        with self._lock:
            # Se bloccato, size = 0
            blocked, reason = self.is_trading_blocked()
            if blocked:
                logger.info(f"Size = 0 (blocked: {reason})")
                return 0

            # Base size
            size = float(self.base_size)

            # Fattore Confidenza
            # conf < 0.55 -> 0.5x, conf 0.60 -> 1.0x, conf > 0.80 -> 1.5x
            if signal_confidence < 0.55:
                conf_factor = 0.5
            elif signal_confidence < 0.65:
                conf_factor = 0.8
            elif signal_confidence < 0.75:
                conf_factor = 1.0
            elif signal_confidence < 0.85:
                conf_factor = 1.2
            else:
                conf_factor = 1.5
            size *= conf_factor

            # Fattore Volatilita (inverse vol scaling)
            # size *= avg_vol / current_vol
            if current_vol > 0 and self.avg_vol > 0:
                vol_factor = self.avg_vol / current_vol
                # Clamp: non meno di 0.3x, non piu di 2.0x
                vol_factor = max(0.3, min(2.0, vol_factor))
                size *= vol_factor

                # Volatilita estrema: blocca
                if current_vol > self.vol_extreme_threshold * 3:
                    logger.warning(f"EXTREME volatility: {current_vol:.6f} > 3x threshold")
                    return 0

            # Profit Lock: riduci size se in profitto alto
            if self.daily_pnl_pts >= self.profit_lock_pts:
                size *= self.profit_lock_size_factor
                logger.info(
                    f"Profit lock active: PnL={self.daily_pnl_pts:+.1f} pts >= "
                    f"{self.profit_lock_pts}, size reduced to {self.profit_lock_size_factor}x"
                )

            # Arrotonda per difetto, min 1, max max_size
            final_size = max(self.min_size, min(self.max_size, int(size)))

            logger.debug(
                f"Size calc: base={self.base_size} * conf={conf_factor:.1f} "
                f"* vol_f={vol_factor if current_vol > 0 else 'N/A'} "
                f"-> {final_size} contracts"
            )
            return final_size

    # ============================================================
    # Pre-Flight Check — validate_order()
    # ============================================================

    def validate_order(
        self,
        side: str,
        price: float,
        sl: float,
        tp: float,
    ) -> tuple[bool, str]:
        """Validazione pre-ordine. Ultimo gate prima di NinjaTrader.

        Args:
            side: "LONG" o "SHORT"
            price: Prezzo di ingresso
            sl: Stop Loss
            tp: Take Profit

        Returns:
            (valid: bool, reason: str)
        """
        with self._lock:
            # 1. R/R ratio
            if side == "LONG":
                risk = abs(price - sl)
                reward = abs(tp - price)
            else:
                risk = abs(sl - price)
                reward = abs(price - tp)

            if risk <= 0:
                return False, "Risk = 0 (SL == Entry)"

            rr_ratio = reward / risk
            if rr_ratio < self.min_rr_ratio:
                return False, (
                    f"R/R ratio {rr_ratio:.2f} < min {self.min_rr_ratio} "
                    f"(risk={risk:.2f}, reward={reward:.2f})"
                )

            # 2. Esposizione massima
            total_exposure = len(self._open_positions)
            if total_exposure >= self.max_exposure_contracts:
                return False, (
                    f"Max exposure reached: {total_exposure}/{self.max_exposure_contracts} "
                    f"contracts open"
                )

            # 3. Nessun duplicato: no secondo LONG se gia aperto LONG
            for pos in self._open_positions:
                if pos.get("side") == side:
                    return False, (
                        f"Duplicate {side}: already have open {side} position"
                    )

            return True, "OK"

    # ============================================================
    # Trade Result — record_trade_result()
    # ============================================================

    def record_trade_result(self, pnl_pts: float, side: str = "") -> None:
        """Registra il risultato di un trade completato.

        Aggiorna contatori, controlla circuit breakers, salva stato.
        """
        with self._lock:
            self.daily_pnl_pts += pnl_pts
            self.total_trades_today += 1

            # Aggiorna peak per trailing drawdown
            if self.daily_pnl_pts > self.daily_peak_pnl_pts:
                self.daily_peak_pnl_pts = self.daily_pnl_pts

            if pnl_pts > 0:
                self.winning_trades += 1
                self.consecutive_losses = 0
                self._consec_loss_locked_until = None  # reset cooldown su win
            elif pnl_pts < 0:
                self.losing_trades += 1
                self.consecutive_losses += 1
            # pnl_pts == 0: breakeven

            # Rimuovi posizione dalla lista
            if side:
                self._open_positions = [
                    p for p in self._open_positions if p.get("side") != side
                ]

            logger.info(
                f"Trade result: {pnl_pts:+.1f} pts | "
                f"Daily PnL: {self.daily_pnl_pts:+.1f} pts | "
                f"W/L: {self.winning_trades}/{self.losing_trades} | "
                f"Consec losses: {self.consecutive_losses} | "
                f"Trades: {self.total_trades_today}/{self.max_daily_trades}"
            )

            # Check circuit breakers
            self.is_trading_blocked()

            self._save_state()

    # ============================================================
    # Position tracking
    # ============================================================

    def register_open_position(self, side: str, price: float, size: int) -> None:
        """Registra l'apertura di una posizione."""
        with self._lock:
            self._open_positions.append({
                "side": side,
                "entry_price": price,
                "size": size,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            })
            self._save_state()

    def get_open_positions(self) -> list[dict[str, Any]]:
        """Ritorna le posizioni aperte."""
        with self._lock:
            return list(self._open_positions)

    def force_close_all(self, reason: str) -> None:
        """Emergenza: chiudi tutte le posizioni e blocca il trading.

        Chiamato da main.py o comando Telegram.
        """
        with self._lock:
            logger.warning(f"FORCE CLOSE ALL: {reason}")
            self._open_positions.clear()
            self._set_daily_lock(f"FORCE CLOSE: {reason}")

    # ============================================================
    # Daily Reset
    # ============================================================

    def reset_daily(self) -> None:
        """Reset tutti i contatori giornalieri. Chiamare alle 22:00 UTC."""
        with self._lock:
            self.daily_pnl_pts = 0.0
            self.consecutive_losses = 0
            self.total_trades_today = 0
            self.winning_trades = 0
            self.losing_trades = 0
            self.daily_peak_pnl_pts = 0.0
            self._daily_locked = False
            self._lock_reason = ""
            self._consec_loss_locked_until = None
            self._open_positions.clear()
            logger.info("Risk manager: daily reset completed")
            self._save_state()

    # ============================================================
    # Persistenza — risk_state.json
    # ============================================================

    def _save_state(self) -> None:
        """Salva lo stato corrente su file JSON."""
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            state = {
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "daily_pnl_pts": self.daily_pnl_pts,
                "daily_pnl_usd": self.daily_pnl_pts * ES_POINT_VALUE,
                "consecutive_losses": self.consecutive_losses,
                "total_trades_today": self.total_trades_today,
                "winning_trades": self.winning_trades,
                "losing_trades": self.losing_trades,
                "daily_peak_pnl_pts": self.daily_peak_pnl_pts,
                "daily_locked": self._daily_locked,
                "lock_reason": self._lock_reason,
                "consec_loss_locked_until": (
                    self._consec_loss_locked_until.isoformat()
                    if self._consec_loss_locked_until else None
                ),
                "open_positions": self._open_positions,
            }
            # Scrittura atomica
            tmp = self._state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2, default=str))
            tmp.replace(self._state_file)
        except Exception as e:
            logger.error(f"Failed to save risk state: {e}")

    def _load_state(self) -> None:
        """Carica stato da file JSON (per riavvio a caldo).

        Carica solo se il file e' di OGGI. Se e' di ieri, ignora
        (i contatori devono partire da zero per il nuovo giorno).
        """
        try:
            if not self._state_file.exists():
                return

            data = json.loads(self._state_file.read_text())
            saved_date = data.get("date", "")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            if saved_date != today:
                logger.info(f"Risk state from {saved_date}, today is {today} — starting fresh")
                return

            self.daily_pnl_pts = float(data.get("daily_pnl_pts", 0))
            self.consecutive_losses = int(data.get("consecutive_losses", 0))
            self.total_trades_today = int(data.get("total_trades_today", 0))
            self.winning_trades = int(data.get("winning_trades", 0))
            self.losing_trades = int(data.get("losing_trades", 0))
            self.daily_peak_pnl_pts = float(data.get("daily_peak_pnl_pts", 0))
            self._daily_locked = bool(data.get("daily_locked", False))
            self._lock_reason = str(data.get("lock_reason", ""))
            self._open_positions = data.get("open_positions", [])

            locked_until = data.get("consec_loss_locked_until")
            if locked_until:
                self._consec_loss_locked_until = datetime.fromisoformat(locked_until)

            logger.info(
                f"Risk state restored: PnL={self.daily_pnl_pts:+.1f} pts, "
                f"trades={self.total_trades_today}, losses_streak={self.consecutive_losses}, "
                f"locked={self._daily_locked}"
            )
        except Exception as e:
            logger.warning(f"Failed to load risk state (clean start): {e}")

    # ============================================================
    # Status per monitoring / Telegram
    # ============================================================

    def get_status(self) -> dict[str, Any]:
        """Snapshot per monitoring e Telegram alerts."""
        with self._lock:
            blocked, reason = self.is_trading_blocked()
            return {
                "daily_pnl_pts": self.daily_pnl_pts,
                "daily_pnl_usd": self.daily_pnl_pts * ES_POINT_VALUE,
                "consecutive_losses": self.consecutive_losses,
                "total_trades": self.total_trades_today,
                "trades_remaining": max(0, self.max_daily_trades - self.total_trades_today),
                "win_rate": (
                    self.winning_trades / self.total_trades_today
                    if self.total_trades_today > 0 else 0.0
                ),
                "daily_peak_pnl_pts": self.daily_peak_pnl_pts,
                "drawdown_from_peak_pts": self.daily_peak_pnl_pts - self.daily_pnl_pts,
                "locked": self._daily_locked,
                "lock_reason": self._lock_reason,
                "blocked": blocked,
                "block_reason": reason,
                "open_positions": len(self._open_positions),
                "remaining_loss_budget_pts": self.max_daily_loss_pts + self.daily_pnl_pts,
                "remaining_loss_budget_usd": (self.max_daily_loss_pts + self.daily_pnl_pts) * ES_POINT_VALUE,
            }
