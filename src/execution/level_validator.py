"""
Level Validator - Verifica livelli MR/OR prima di autorizzare un trade

SPECIFICHE PER QWEN:
Legge da settlement_ranges (524+ righe, solo ES):
- mr1d, mr1u, mr2d, mr2u (mid-ranges)
- or1d, or1u, or2d, or2u (opening-ranges)
- vwap, settlement

Regole:
- LONG consentito solo se spot >= mr1d (supporto mid-range 1 down)
- SHORT consentito solo se spot <= mr1u (resistenza mid-range 1 up)
- Se spot dentro OR range (or1d < spot < or1u): cautela, ridurre size
- Se spot fuori MR2: segnale forte di breakout, autorizzare con alta confidenza
"""

import logging
from datetime import datetime, timezone, date
from typing import Any

from src.core.database import DatabaseManager

logger = logging.getLogger("p1uni.execution.level_validator")


class LevelValidator:
    """Valida i segnali contro i livelli di prezzo (MR/OR/VWAP).

    QWEN: questo modulo consulta settlement_ranges per i livelli del giorno.
    Se non ci sono livelli per oggi, usa quelli dell'ultimo giorno disponibile.
    """

    def __init__(self, config: dict[str, Any], db: DatabaseManager) -> None:
        self.config = config
        self.db = db
        self._levels: dict[str, float] = {}
        self._levels_date: date | None = None

    def _load_levels(self) -> None:
        """Carica i livelli MR/OR piu recenti dal DB.

        QWEN: query settlement_ranges per l'ultimo giorno disponibile.
        """
        try:
            df = self.db.execute_read(
                """SELECT date, vwap, settlement,
                          mr1d, mr1u, mr2d, mr2u,
                          or1d, or1u, or2d, or2u
                   FROM settlement_ranges
                   ORDER BY date DESC
                   LIMIT 1"""
            )
            if df.empty:
                logger.warning("No settlement_ranges data available")
                return

            row = df.iloc[0]
            self._levels = {col: float(row[col]) for col in df.columns if col != "date" and row[col] is not None}
            self._levels_date = row["date"]
            logger.info(f"Loaded levels for {self._levels_date}: {self._levels}")
        except Exception as e:
            logger.error(f"Failed to load levels: {e}")

    def check(self, spot: float, direction: str) -> bool:
        """Verifica se un trade e' consentito ai livelli correnti.

        Args:
            spot: Prezzo spot corrente
            direction: "LONG" o "SHORT"

        Returns:
            True se il trade e' autorizzato, False altrimenti.
        """
        # Ricarica livelli se obsoleti
        today = datetime.now(timezone.utc).date()
        if self._levels_date != today or not self._levels:
            self._load_levels()

        if not self._levels:
            logger.warning("No levels available, allowing trade by default")
            return True

        mr1d = self._levels.get("mr1d", 0)
        mr1u = self._levels.get("mr1u", float("inf"))
        mr2d = self._levels.get("mr2d", 0)
        mr2u = self._levels.get("mr2u", float("inf"))

        if direction == "LONG":
            # LONG solo se spot >= mr1d (sopra il supporto)
            if spot < mr1d:
                logger.info(f"LONG rejected: spot {spot} < mr1d {mr1d}")
                return False

        elif direction == "SHORT":
            # SHORT solo se spot <= mr1u (sotto la resistenza)
            if spot > mr1u:
                logger.info(f"SHORT rejected: spot {spot} > mr1u {mr1u}")
                return False

        # Breakout check: se fuori MR2, trade forte autorizzato
        if spot < mr2d or spot > mr2u:
            logger.info(f"Breakout detected: spot {spot} outside MR2 [{mr2d}, {mr2u}]")

        return True

    def get_size_modifier(self, spot: float) -> float:
        """Ritorna un modificatore di size basato sulla posizione nei livelli.

        QWEN: se spot dentro OR range -> ridurre size (0.5x)
        Se breakout oltre MR2 -> size piena (1.0x)
        """
        if not self._levels:
            return 1.0

        or1d = self._levels.get("or1d", 0)
        or1u = self._levels.get("or1u", float("inf"))

        if or1d < spot < or1u:
            logger.info(f"Spot {spot} inside OR range [{or1d}, {or1u}], reducing size")
            return 0.5

        return 1.0
