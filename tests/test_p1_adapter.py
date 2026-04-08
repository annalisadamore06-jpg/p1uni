"""
Test per P1LiteAdapter - timestamp Zurigo, snapshot MR/OR, ticker mapping.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.adapter_p1 import P1LiteAdapter


def _make_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "_base_dir": str(tmp_path),
        "database": {"path": ":memory:", "threads": 1, "memory_limit": "256MB"},
        "p1lite": {
            "enabled": True,
            "url": "http://fake.p1lite.local/stream",
            "frequency_hz": 1,
            "ticker": "ES",
            "snapshot_events": ["morning_range_freeze", "overnight_range_freeze"],
        },
    }


def _make_live_msg(spot: float = 5420.5, ts: str = "2026-04-08T16:30:00") -> dict[str, Any]:
    """Range live P1-Lite. Timestamp in ora locale Zurigo (CEST = UTC+2)."""
    return {
        "timestamp": ts,  # Zurigo time, naive!
        "ticker": "ES",
        "spot": spot,
        "high": 5445.0,
        "low": 5400.0,
        "vwap": 5422.3,
        "volume": 1500000,
        "mr1d": 5380.0,
        "mr1u": 5460.0,
        "or1d": 5405.0,
        "or1u": 5435.0,
    }


def _make_snapshot_msg() -> dict[str, Any]:
    """Snapshot MR freeze."""
    return {
        "event": "morning_range_freeze",
        "is_frozen": True,
        "timestamp": "2026-04-08T15:30:00",  # Zurigo
        "ticker": "ES",
        "spot": 5420.5,
        "data": {
            "mr1d": 5380.0, "mr1u": 5460.0,
            "mr2d": 5340.0, "mr2u": 5500.0,
            "or1d": 5405.0, "or1u": 5435.0,
            "or2d": 5390.0, "or2u": 5450.0,
            "vwap": 5422.3,
        },
    }


class TestP1LiteAdapter:

    # ============================================================
    # Test Timestamp (CRUCIALE)
    # ============================================================

    def test_timestamp_zurich_to_utc_cest(self, tmp_path: Path) -> None:
        """Timestamp naive 16:30 Zurigo (CEST aprile) deve diventare 14:30 UTC.
        CEST = Central European Summer Time = UTC+2.
        """
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)
        adapter._running = True

        msg = _make_live_msg(ts="2026-04-08T16:30:00")  # Zurigo CEST
        record = adapter._parse_p1_payload(msg, is_snapshot=False)
        assert record is not None

        # Normalizza come farebbe process_message
        normalized = adapter.normalizer.normalize(record, "P1_LITE")
        ts_utc = normalized["ts_utc"]

        assert ts_utc is not None
        assert ts_utc.tzinfo is not None
        # CEST = UTC+2, quindi 16:30 Zurigo = 14:30 UTC
        assert ts_utc.hour == 14, f"Expected 14 (UTC), got {ts_utc.hour}"
        assert ts_utc.minute == 30

    def test_timestamp_zurich_to_utc_cet(self, tmp_path: Path) -> None:
        """Timestamp naive 09:00 Zurigo (CET gennaio) deve diventare 08:00 UTC.
        CET = Central European Time = UTC+1.
        """
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = _make_live_msg(ts="2026-01-15T09:00:00")  # Zurigo CET (inverno)
        record = adapter._parse_p1_payload(msg, is_snapshot=False)
        normalized = adapter.normalizer.normalize(record, "P1_LITE")
        ts_utc = normalized["ts_utc"]

        assert ts_utc is not None
        # CET = UTC+1, quindi 09:00 Zurigo = 08:00 UTC
        assert ts_utc.hour == 8, f"Expected 8 (UTC), got {ts_utc.hour}"

    # ============================================================
    # Test Snapshot detection
    # ============================================================

    def test_snapshot_detected_by_is_frozen(self, tmp_path: Path) -> None:
        """Campo is_frozen=True deve essere riconosciuto come snapshot."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = {"is_frozen": True, "spot": 5420.5, "timestamp": "2026-04-08T15:30:00"}
        assert adapter._is_snapshot_event(msg) is True

    def test_snapshot_detected_by_event(self, tmp_path: Path) -> None:
        """Campo event='morning_range_freeze' deve essere riconosciuto."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = {"event": "morning_range_freeze", "spot": 5420.5}
        assert adapter._is_snapshot_event(msg) is True

        msg2 = {"event": "overnight_range_freeze", "spot": 5420.5}
        assert adapter._is_snapshot_event(msg2) is True

    def test_live_not_detected_as_snapshot(self, tmp_path: Path) -> None:
        """Un messaggio live normale NON deve essere snapshot."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = _make_live_msg()
        assert adapter._is_snapshot_event(msg) is False

    def test_snapshot_has_is_frozen_flag(self, tmp_path: Path) -> None:
        """Record da snapshot deve avere is_frozen=True."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = _make_snapshot_msg()
        record = adapter._parse_p1_payload(msg, is_snapshot=True)
        assert record is not None
        assert record["is_frozen"] is True

    def test_snapshot_writes_to_daily_snapshot(self, tmp_path: Path) -> None:
        """Snapshot deve scrivere in daily_ranges_snapshot via DB."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)
        adapter._running = True
        adapter._session_date = datetime(2026, 4, 8).date()
        adapter._session_high = 5445.0
        adapter._session_low = 5400.0

        msg = _make_snapshot_msg()
        record = adapter._parse_p1_payload(msg, is_snapshot=True)
        adapter._handle_snapshot_event(record, msg)

        # Verifica che execute_write sia stato chiamato per daily_ranges_snapshot
        calls = [str(c) for c in db.execute_write.call_args_list]
        assert any("daily_ranges_snapshot" in c for c in calls), \
            f"Expected INSERT INTO daily_ranges_snapshot, got: {calls}"

    # ============================================================
    # Test Ticker
    # ============================================================

    def test_ticker_es_spx_normalized(self, tmp_path: Path) -> None:
        """Ticker ES_SPX deve essere normalizzato a ES."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = _make_live_msg()
        msg["ticker"] = "ES_SPX"

        record = adapter._parse_p1_payload(msg, is_snapshot=False)
        assert record is not None

        normalized = adapter.normalizer.normalize(record, "P1_LITE")
        assert normalized["ticker"] == "ES"

    def test_ticker_default_from_config(self, tmp_path: Path) -> None:
        """Se ticker mancante nel messaggio, usa quello dal config."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = {"spot": 5420.5, "timestamp": "2026-04-08T14:30:00"}
        # Nessun ticker nel messaggio
        record = adapter._parse_p1_payload(msg, is_snapshot=False)
        assert record is not None
        assert record["ticker"] == "ES"  # dal config

    # ============================================================
    # Test session state
    # ============================================================

    def test_session_high_low_tracking(self, tmp_path: Path) -> None:
        """Running high/low devono essere aggiornati ad ogni tick."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)
        adapter._running = True

        # Primo tick
        record1 = {"timestamp": "2026-04-08T14:30:00Z", "spot": 5420.0, "ticker": "ES"}
        adapter._update_session_state(record1)
        assert adapter._session_high == 5420.0
        assert adapter._session_low == 5420.0

        # Tick piu alto
        record2 = {"timestamp": "2026-04-08T14:31:00Z", "spot": 5430.0, "ticker": "ES"}
        adapter._update_session_state(record2)
        assert adapter._session_high == 5430.0
        assert adapter._session_low == 5420.0

        # Tick piu basso
        record3 = {"timestamp": "2026-04-08T14:32:00Z", "spot": 5410.0, "ticker": "ES"}
        adapter._update_session_state(record3)
        assert adapter._session_high == 5430.0
        assert adapter._session_low == 5410.0

    def test_session_reset_on_new_date(self, tmp_path: Path) -> None:
        """Cambio data sessione deve resettare high/low."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)
        adapter._running = True

        # Giorno 1
        r1 = {"timestamp": "2026-04-08T14:30:00Z", "spot": 5420.0, "ticker": "ES"}
        adapter._update_session_state(r1)
        assert adapter._session_high == 5420.0

        # Giorno 2 (nuova sessione)
        r2 = {"timestamp": "2026-04-09T10:00:00Z", "spot": 5500.0, "ticker": "ES"}
        adapter._update_session_state(r2)
        assert adapter._session_high == 5500.0
        assert adapter._session_low == 5500.0  # reset

    # ============================================================
    # Test messaggi malformati
    # ============================================================

    def test_malformed_json_ignored(self, tmp_path: Path) -> None:
        """JSON rotto non deve crashare."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)
        adapter._running = True

        adapter._handle_p1_message("not json {{{")
        assert adapter._queue.qsize() == 0

    def test_message_without_spot_skipped(self, tmp_path: Path) -> None:
        """Messaggio senza spot/price deve essere scartato."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)

        msg = {"timestamp": "2026-04-08T14:30:00", "ticker": "ES"}
        record = adapter._parse_p1_payload(msg, is_snapshot=False)
        assert record is None

    # ============================================================
    # Test stats
    # ============================================================

    def test_stats_include_session_info(self, tmp_path: Path) -> None:
        """get_stats() deve includere info sessione."""
        db = MagicMock()
        adapter = P1LiteAdapter(_make_config(tmp_path), db)
        adapter._running = True
        adapter._session_date = datetime(2026, 4, 8).date()
        adapter._session_high = 5445.0
        adapter._session_low = 5400.0
        adapter._session_tick_count = 1500

        stats = adapter.get_stats()
        assert stats["session_date"] == "2026-04-08"
        assert stats["session_high"] == 5445.0
        assert stats["session_low"] == 5400.0
        assert stats["session_ticks"] == 1500
