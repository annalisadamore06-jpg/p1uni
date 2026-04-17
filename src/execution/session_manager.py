"""
Session Manager - Macchina a stati finiti (FSM) per le 5 fasi di trading.

Il "Cervello Temporale" del sistema. Determina in quale fase di mercato
ci troviamo, quali livelli sono attivi, e triggera gli eventi di freeze MR/OR.

LE 5 FASI (orari UTC):
  1. NIGHT_EARLY  (00:01 - 02:15) — OR Frozen (giorno prec.), 1DTE, FULL
  2. NIGHT_FULL   (02:15 - 09:00) — OR Frozen + Range Live notturni
  3. MORNING_EU   (09:00 - 15:30) — MR Frozen (freeze a 09:00) + Range Live
  4. AFTERNOON_US (15:30 - 21:55) — OR Frozen (freeze a 15:30) + GEX Live + Range Live
  5. HALT         (21:55 - 00:01) — Stop trading, snapshot finale, weekend check

EVENTI CRITICI:
  09:00 UTC -> FREEZE MR: cattura range correnti come MR_Frozen
  15:30 UTC -> FREEZE OR: cattura range correnti come OR_Frozen
  21:55 UTC -> HALT: stop trading, chiudi posizioni
  22:00 UTC -> SNAPSHOT: salva livelli statici per domani

PERSISTENZA:
  Se il sistema si riavvia a meta giornata (es: 12:00), il SessionManager
  determina immediatamente la fase corrente e recupera i livelli frozen
  dal DB (daily_ranges_snapshot) senza ricalcolarli.

SINGLETON: una sola istanza per processo.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, time as dtime, timezone, date, timedelta
from enum import Enum
from typing import Any, Callable

from src.core.database import DatabaseManager

logger = logging.getLogger("p1uni.execution.session_manager")


# ============================================================
# MarketPhase Enum
# ============================================================

class MarketPhase(Enum):
    """Le 5 fasi di trading + stati speciali."""
    NIGHT_EARLY = "night_early"      # 00:01 - 02:15 UTC
    NIGHT_FULL = "night_full"        # 02:15 - 09:00 UTC
    MORNING_EU = "morning_eu"        # 09:00 - 15:30 UTC
    AFTERNOON_US = "afternoon_us"    # 15:30 - 21:55 UTC
    HALT = "halt"                    # 21:55 - 00:01 UTC
    WEEKEND = "weekend"              # Sab 01:00 - Dom 22:00 UTC


# ============================================================
# Livelli attivi per fase
# ============================================================

# Quali livelli sono validi e attivi per ogni fase
PHASE_ACTIVE_LEVELS: dict[MarketPhase, list[str]] = {
    MarketPhase.NIGHT_EARLY: ["OR_FROZEN_PREV", "FULL_GEX", "DTE_1"],
    MarketPhase.NIGHT_FULL: ["OR_FROZEN_PREV", "RANGE_LIVE"],
    MarketPhase.MORNING_EU: ["MR_FROZEN", "RANGE_LIVE"],
    MarketPhase.AFTERNOON_US: ["OR_FROZEN", "GEX_LIVE", "RANGE_LIVE"],
    MarketPhase.HALT: [],
    MarketPhase.WEEKEND: [],
}

# Confidence override per fase
# NOTA: scala confidence = abs(avg_proba - 0.5) * 2  (range 0.0 - 1.0)
#   0.20 = WEAK_SIGNAL   (avg_proba 0.60, WR 72.5%) — minimo accettabile
#   0.30 = NORMAL_SIGNAL (avg_proba 0.65, WR 75.9%) — conservativo
#   0.40 = STRONG_SIGNAL (avg_proba 0.70, WR 79.7%) — molto conservativo
#   0.50 = SUPER_SIGNAL  (avg_proba 0.75, WR 84.6%) — solo i migliori
PHASE_MIN_CONFIDENCE: dict[MarketPhase, float] = {
    MarketPhase.NIGHT_EARLY: 0.40,   # conservativo di notte (STRONG_SIGNAL)
    MarketPhase.NIGHT_FULL: 0.30,    # notte piena (NORMAL_SIGNAL)
    MarketPhase.MORNING_EU: 0.20,    # mattina EU (WEAK_SIGNAL accettato)
    MarketPhase.AFTERNOON_US: 0.20,  # pomeriggio US (WEAK_SIGNAL accettato)
    MarketPhase.HALT: 1.0,           # nessun trade
    MarketPhase.WEEKEND: 1.0,        # nessun trade
}

# Max trades per fase
PHASE_MAX_TRADES: dict[MarketPhase, int] = {
    MarketPhase.NIGHT_EARLY: 1,
    MarketPhase.NIGHT_FULL: 2,
    MarketPhase.MORNING_EU: 999,
    MarketPhase.AFTERNOON_US: 999,
    MarketPhase.HALT: 0,
    MarketPhase.WEEKEND: 0,
}


# ============================================================
# Confini temporali delle fasi (ora UTC)
# ============================================================

class _PhaseBoundary:
    """Definisce inizio e fine di una fase con orari UTC."""
    __slots__ = ("phase", "start", "end")

    def __init__(self, phase: MarketPhase, start: dtime, end: dtime) -> None:
        self.phase = phase
        self.start = start
        self.end = end

    def contains(self, t: dtime) -> bool:
        """Verifica se l'orario cade in questa fase.
        Gestisce il wrap-around di mezzanotte.
        """
        if self.start <= self.end:
            return self.start <= t < self.end
        else:
            # Wrap: es 21:55 -> 00:01
            return t >= self.start or t < self.end


# Ordine: dalle piu specifiche alle piu generiche
_PHASE_SCHEDULE: list[_PhaseBoundary] = [
    _PhaseBoundary(MarketPhase.HALT,         dtime(21, 55), dtime(0, 1)),
    _PhaseBoundary(MarketPhase.NIGHT_EARLY,  dtime(0, 1),   dtime(2, 15)),
    _PhaseBoundary(MarketPhase.NIGHT_FULL,   dtime(2, 15),  dtime(9, 0)),
    _PhaseBoundary(MarketPhase.MORNING_EU,   dtime(9, 0),   dtime(15, 30)),
    _PhaseBoundary(MarketPhase.AFTERNOON_US, dtime(15, 30), dtime(21, 55)),
]

# Orari critici per eventi di freeze
FREEZE_MR_TIME = dtime(9, 0)    # 09:00 UTC
FREEZE_OR_TIME = dtime(15, 30)  # 15:30 UTC
HALT_TIME = dtime(21, 55)       # 21:55 UTC
SNAPSHOT_TIME = dtime(22, 0)    # 22:00 UTC


# ============================================================
# SessionManager — Singleton
# ============================================================

class SessionManager:
    """Macchina a stati finiti per le fasi di trading.

    Singleton: una sola istanza per processo.
    Thread-safe: tutti gli accessi allo stato sono protetti da lock.

    Usage:
        sm = SessionManager(config, db)
        phase = sm.get_current_phase()
        levels = sm.get_active_levels()
        if sm.is_trading_allowed():
            # trade
    """

    _instance: SessionManager | None = None
    _lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> SessionManager:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        db: DatabaseManager | None = None,
    ) -> None:
        if self._initialized:
            return
        if config is None or db is None:
            raise ValueError("First SessionManager init requires config and db")

        self.config = config
        self.db = db

        # Stato corrente
        self._state_lock = threading.RLock()
        self._current_phase: MarketPhase = MarketPhase.HALT
        self._phase_date: date | None = None
        self._trades_this_phase: int = 0

        # Livelli frozen correnti
        self._mr_frozen: dict[str, float] = {}
        self._or_frozen: dict[str, float] = {}
        self._or_frozen_prev: dict[str, float] = {}  # dal giorno precedente

        # Tracking eventi triggerati oggi (evita duplicati)
        self._events_fired_today: set[str] = set()
        self._events_date: date | None = None

        # Callbacks per eventi
        self._on_freeze_callbacks: list[Callable[[str, dict], None]] = []

        # Crea tabelle se non esistono (per DB locale nuovo)
        self._ensure_tables()

        # Carica stato dal DB (per riavvio a caldo)
        self._load_state_from_db()

        # Aggiorna fase corrente
        self.update()

        self._initialized = True
        logger.info(f"SessionManager initialized. Phase: {self._current_phase.value}")

    def _ensure_tables(self) -> None:
        """Crea tabelle necessarie se non esistono (DB locale nuovo)."""
        try:
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS daily_ranges_snapshot (
                    session_date DATE, ticker VARCHAR DEFAULT 'ES',
                    source VARCHAR, mr1d DOUBLE, mr1u DOUBLE,
                    mr2d DOUBLE, mr2u DOUBLE, or1d DOUBLE, or1u DOUBLE,
                    or2d DOUBLE, or2u DOUBLE, vwap DOUBLE,
                    session_high DOUBLE, session_low DOUBLE,
                    close_price DOUBLE, is_final BOOLEAN DEFAULT FALSE,
                    frozen_at TIMESTAMP
                )
            """)
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS intraday_ranges_stream (
                    ts_utc TIMESTAMP, session_date DATE,
                    ticker VARCHAR DEFAULT 'ES', source VARCHAR DEFAULT 'P1_LITE',
                    spot DOUBLE, running_high DOUBLE, running_low DOUBLE,
                    running_vwap DOUBLE, volume_cumulative BIGINT,
                    mr1d DOUBLE, mr1u DOUBLE, or1d DOUBLE, or1u DOUBLE,
                    is_frozen BOOLEAN DEFAULT FALSE
                )
            """)
            self.db.execute_write("""
                CREATE TABLE IF NOT EXISTS settlement_ranges (
                    date DATE, vwap DOUBLE, settlement DOUBLE,
                    mr1d DOUBLE, mr1u DOUBLE, mr2d DOUBLE, mr2u DOUBLE,
                    or1d DOUBLE, or1u DOUBLE, or2d DOUBLE, or2u DOUBLE,
                    skew DOUBLE, vix DOUBLE
                )
            """)
        except Exception as e:
            logger.warning(f"Table creation: {e}")

    # ============================================================
    # API pubblica
    # ============================================================

    def get_current_phase(self, now: datetime | None = None) -> MarketPhase:
        """Ritorna la fase di mercato corrente.

        Args:
            now: Timestamp UTC (default: now). Per testing.
        """
        if now is None:
            now = datetime.now(timezone.utc)
        return self._resolve_phase(now)

    def get_active_levels(self) -> list[str]:
        """Ritorna i livelli attivi per la fase corrente.

        Es: ["MR_FROZEN", "RANGE_LIVE"] durante la mattina.
        """
        with self._state_lock:
            return list(PHASE_ACTIVE_LEVELS.get(self._current_phase, []))

    def get_min_confidence(self) -> float:
        """Ritorna la confidence minima per la fase corrente."""
        with self._state_lock:
            return PHASE_MIN_CONFIDENCE.get(self._current_phase, 0.60)

    def get_max_trades(self) -> int:
        """Ritorna il max trades per la fase corrente."""
        with self._state_lock:
            return PHASE_MAX_TRADES.get(self._current_phase, 0)

    def is_trading_allowed(self) -> bool:
        """True se il trading e' consentito nella fase corrente."""
        with self._state_lock:
            if self._current_phase in (MarketPhase.HALT, MarketPhase.WEEKEND):
                return False
            if self._trades_this_phase >= PHASE_MAX_TRADES.get(self._current_phase, 0):
                return False
            return True

    def record_trade(self) -> None:
        """Registra un trade eseguito nella fase corrente."""
        with self._state_lock:
            self._trades_this_phase += 1

    def get_frozen_levels(self) -> dict[str, dict[str, float]]:
        """Ritorna tutti i livelli frozen correnti."""
        with self._state_lock:
            return {
                "mr_frozen": dict(self._mr_frozen),
                "or_frozen": dict(self._or_frozen),
                "or_frozen_prev": dict(self._or_frozen_prev),
            }

    def register_freeze_callback(self, callback: Callable[[str, dict], None]) -> None:
        """Registra un callback per eventi di freeze.

        Il callback riceve (event_name: str, levels: dict).
        Es: callback("freeze_mr", {"mr1d": 5380, "mr1u": 5460, ...})
        """
        self._on_freeze_callbacks.append(callback)

    # ============================================================
    # Update: chiamare periodicamente (ogni secondo o tick)
    # ============================================================

    def update(self, now: datetime | None = None) -> MarketPhase:
        """Aggiorna la fase corrente e triggera eventi se necessario.

        Chiamare dal main loop ogni ~1 secondo.

        Returns:
            La fase corrente dopo l'aggiornamento.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        new_phase = self._resolve_phase(now)
        today = now.date()

        with self._state_lock:
            # Reset eventi giornalieri se cambia giorno
            if self._events_date != today:
                self._events_fired_today.clear()
                self._events_date = today

            old_phase = self._current_phase

            # Transizione di fase?
            if new_phase != old_phase:
                self._on_phase_transition(old_phase, new_phase, now)
                self._current_phase = new_phase
                self._trades_this_phase = 0

            # Check eventi time-based (indipendenti dalla transizione)
            self._check_timed_events(now)

        return new_phase

    # ============================================================
    # Risoluzione fase
    # ============================================================

    def _resolve_phase(self, now: datetime) -> MarketPhase:
        """Determina la fase da orario UTC.

        Controlla prima il weekend, poi le fasi temporali.
        """
        # Weekend check
        if self._is_weekend(now):
            return MarketPhase.WEEKEND

        t = now.time()
        for boundary in _PHASE_SCHEDULE:
            if boundary.contains(t):
                return boundary.phase

        # Fallback (non dovrebbe mai succedere)
        logger.warning(f"No phase matched for {t}, defaulting to HALT")
        return MarketPhase.HALT

    @staticmethod
    def _is_weekend(now: datetime) -> bool:
        """Mercato ES chiuso: Sab dopo 01:00 UTC - Dom prima 22:00 UTC."""
        wd = now.weekday()  # 0=Mon, 5=Sat, 6=Sun
        h = now.hour

        if wd == 5 and h >= 1:    # Sabato dopo 01:00
            return True
        if wd == 6 and h < 22:    # Domenica prima delle 22:00
            return True
        return False

    # ============================================================
    # Transizioni di fase
    # ============================================================

    def _on_phase_transition(
        self,
        old: MarketPhase,
        new: MarketPhase,
        now: datetime,
    ) -> None:
        """Gestisce la transizione tra fasi."""
        logger.info(f"Phase transition: {old.value} -> {new.value} at {now.strftime('%H:%M:%S')} UTC")

        # Da qualsiasi cosa a MORNING_EU: freeze MR implicito
        if new == MarketPhase.MORNING_EU and "freeze_mr" not in self._events_fired_today:
            self._fire_freeze_mr(now)

        # Da qualsiasi cosa a AFTERNOON_US: freeze OR implicito
        if new == MarketPhase.AFTERNOON_US and "freeze_or" not in self._events_fired_today:
            self._fire_freeze_or(now)

        # Entrata in HALT: stop trading
        if new == MarketPhase.HALT:
            logger.warning("HALT phase: trading stopped, close all positions")

    # ============================================================
    # Eventi time-based
    # ============================================================

    def _check_timed_events(self, now: datetime) -> None:
        """Check eventi basati sull'orario esatto (per sicurezza, oltre alla transizione)."""
        t = now.time()

        # 09:00 UTC -> freeze MR
        if t >= FREEZE_MR_TIME and "freeze_mr" not in self._events_fired_today:
            if self._current_phase in (MarketPhase.MORNING_EU, MarketPhase.NIGHT_FULL):
                self._fire_freeze_mr(now)

        # 15:30 UTC -> freeze OR
        if t >= FREEZE_OR_TIME and "freeze_or" not in self._events_fired_today:
            if self._current_phase in (MarketPhase.AFTERNOON_US, MarketPhase.MORNING_EU):
                self._fire_freeze_or(now)

        # 22:00 UTC -> snapshot finale
        if t >= SNAPSHOT_TIME and "snapshot_final" not in self._events_fired_today:
            if self._current_phase == MarketPhase.HALT:
                self._fire_snapshot_final(now)

    # ============================================================
    # Freeze events
    # ============================================================

    def _fire_freeze_mr(self, now: datetime) -> None:
        """Evento FREEZE MR: cattura i range correnti come MR Frozen."""
        self._events_fired_today.add("freeze_mr")
        logger.info("EVENT: FREEZE MR — capturing morning ranges")

        # Carica i range correnti dal DB (ultimi range live)
        levels = self._load_current_ranges(now)
        self._mr_frozen = levels

        # Salva nel DB
        self._save_frozen_levels("mr", levels, now)

        # Notifica callbacks
        for cb in self._on_freeze_callbacks:
            try:
                cb("freeze_mr", levels)
            except Exception as e:
                logger.error(f"Freeze MR callback error: {e}")

    def _fire_freeze_or(self, now: datetime) -> None:
        """Evento FREEZE OR: cattura i range correnti come OR Frozen."""
        self._events_fired_today.add("freeze_or")
        logger.info("EVENT: FREEZE OR — capturing opening ranges")

        levels = self._load_current_ranges(now)

        # OR Frozen precedente diventa "prev" per la notte
        self._or_frozen_prev = dict(self._or_frozen)
        self._or_frozen = levels

        self._save_frozen_levels("or", levels, now)

        for cb in self._on_freeze_callbacks:
            try:
                cb("freeze_or", levels)
            except Exception as e:
                logger.error(f"Freeze OR callback error: {e}")

    def _fire_snapshot_final(self, now: datetime) -> None:
        """Evento SNAPSHOT FINALE (22:00): salva tutti i livelli per domani."""
        self._events_fired_today.add("snapshot_final")
        logger.info("EVENT: SNAPSHOT FINALE — saving all levels for next session")

        try:
            session_date = now.date()
            self.db.execute_write(
                """UPDATE daily_ranges_snapshot
                   SET is_final = TRUE
                   WHERE session_date = ? AND ticker = 'ES'""",
                [session_date],
            )
            logger.info(f"Snapshot finalized for {session_date}")
        except Exception as e:
            logger.error(f"Snapshot final error: {e}")

    # ============================================================
    # DB: carica/salva livelli
    # ============================================================

    def _load_state_from_db(self) -> None:
        """Carica i livelli frozen dal DB per riavvio a caldo.

        Se il sistema riparte a 12:00, deve trovare i livelli MR frozen
        gia salvati alle 09:00 e usarli immediatamente.
        """
        try:
            today = datetime.now(timezone.utc).date()

            # Cerca snapshot di oggi
            df = self.db.execute_read(
                """SELECT mr1d, mr1u, mr2d, mr2u, or1d, or1u, or2d, or2u,
                          vwap, session_high, session_low, is_final, frozen_at
                   FROM daily_ranges_snapshot
                   WHERE session_date = ? AND ticker = 'ES'
                   ORDER BY frozen_at DESC
                   LIMIT 2""",
                [today],
            )

            if not df.empty:
                row = df.iloc[0]
                # Determina se e' MR o OR dai campi presenti
                levels = {
                    col: float(row[col])
                    for col in ["mr1d", "mr1u", "mr2d", "mr2u",
                                "or1d", "or1u", "or2d", "or2u", "vwap"]
                    if row.get(col) is not None and str(row[col]) != "nan"
                }

                # Se abbiamo frozen_at, determina se e' MR (mattina) o OR (pomeriggio)
                frozen_at = row.get("frozen_at")
                if frozen_at is not None:
                    if isinstance(frozen_at, datetime):
                        frozen_hour = frozen_at.hour
                    else:
                        frozen_hour = 9  # default
                    if frozen_hour < 15:
                        self._mr_frozen = levels
                        self._events_fired_today.add("freeze_mr")
                        logger.info(f"Restored MR frozen from DB: {levels}")
                    else:
                        self._or_frozen = levels
                        self._events_fired_today.add("freeze_or")
                        logger.info(f"Restored OR frozen from DB: {levels}")

                self._events_date = today

            # Cerca anche snapshot del giorno precedente per OR_FROZEN_PREV
            yesterday = today - timedelta(days=1)
            df_prev = self.db.execute_read(
                """SELECT or1d, or1u, or2d, or2u
                   FROM daily_ranges_snapshot
                   WHERE session_date = ? AND ticker = 'ES' AND is_final = TRUE
                   ORDER BY frozen_at DESC LIMIT 1""",
                [yesterday],
            )
            if not df_prev.empty:
                row_prev = df_prev.iloc[0]
                self._or_frozen_prev = {
                    col: float(row_prev[col])
                    for col in ["or1d", "or1u", "or2d", "or2u"]
                    if row_prev.get(col) is not None and str(row_prev[col]) != "nan"
                }
                logger.info(f"Restored OR frozen prev from DB: {self._or_frozen_prev}")

        except Exception as e:
            logger.warning(f"Failed to load state from DB (clean start): {e}")

    def _load_current_ranges(self, now: datetime) -> dict[str, float]:
        """Carica i range live correnti dal DB (ultimo tick P1-Lite)."""
        try:
            df = self.db.execute_read(
                """SELECT running_high, running_low, running_vwap,
                          mr1d, mr1u, or1d, or1u, spot
                   FROM intraday_ranges_stream
                   WHERE session_date = ? AND ticker = 'ES'
                   ORDER BY ts_utc DESC
                   LIMIT 1""",
                [now.date()],
            )
            if df.empty:
                logger.warning("No current ranges in DB for freeze")
                return {}

            row = df.iloc[0]
            return {
                col: float(row[col])
                for col in df.columns
                if row.get(col) is not None and str(row[col]) != "nan"
            }
        except Exception as e:
            logger.error(f"Failed to load current ranges: {e}")
            return {}

    def _save_frozen_levels(
        self, freeze_type: str, levels: dict[str, float], now: datetime,
    ) -> None:
        """Salva i livelli frozen nel DB (daily_ranges_snapshot)."""
        try:
            self.db.execute_write(
                """INSERT INTO daily_ranges_snapshot
                   (session_date, ticker, source, mr1d, mr1u,
                    or1d, or1u, vwap, session_high, session_low, frozen_at)
                   VALUES (?, 'ES', 'SESSION_MGR', ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    now.date(),
                    levels.get("mr1d"), levels.get("mr1u"),
                    levels.get("or1d"), levels.get("or1u"),
                    levels.get("running_vwap") or levels.get("vwap"),
                    levels.get("running_high"),
                    levels.get("running_low"),
                    now,
                ],
            )
            logger.info(f"Saved {freeze_type} frozen levels for {now.date()}")
        except Exception as e:
            logger.error(f"Failed to save frozen levels: {e}")

    # ============================================================
    # Status per monitoring
    # ============================================================

    def get_status(self) -> dict[str, Any]:
        """Snapshot completo dello stato per monitoring/dashboard."""
        with self._state_lock:
            return {
                "current_phase": self._current_phase.value,
                "phase_date": str(self._phase_date) if self._phase_date else None,
                "trades_this_phase": self._trades_this_phase,
                "max_trades": PHASE_MAX_TRADES.get(self._current_phase, 0),
                "min_confidence": PHASE_MIN_CONFIDENCE.get(self._current_phase, 0.60),
                "active_levels": PHASE_ACTIVE_LEVELS.get(self._current_phase, []),
                "trading_allowed": self.is_trading_allowed(),
                "events_fired_today": list(self._events_fired_today),
                "mr_frozen": dict(self._mr_frozen),
                "or_frozen": dict(self._or_frozen),
                "or_frozen_prev": dict(self._or_frozen_prev),
            }

    # ============================================================
    # Reset singleton (per testing)
    # ============================================================

    @classmethod
    def reset(cls) -> None:
        """Reset singleton. Solo per test."""
        with cls._lock:
            cls._instance = None
