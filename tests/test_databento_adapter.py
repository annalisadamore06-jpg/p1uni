"""
Test per DatabentoAdapter - nanosec, fixed-point price, symbol mapping, aggressor.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.ingestion.adapter_databento import (
    DatabentoAdapter,
    _extract_ticker_root,
    _convert_price,
    _convert_timestamp,
    _convert_side,
)


def _make_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "_base_dir": str(tmp_path),
        "database": {"path": ":memory:", "threads": 1, "memory_limit": "256MB"},
        "databento": {
            "api_key": "db-FAKE-KEY",
            "symbol": "ES.c.0",
            "dataset": "GLBX.MDP3",
            "schema": "trades",
            "filter_irregular": False,
        },
    }


class _FakeRecord:
    """Simula un record Databento con attributi."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


# ============================================================
# Test Timestamp (nanosecondi)
# ============================================================

class TestTimestampConversion:

    def test_nanoseconds_to_utc(self) -> None:
        """Nanosecondi Databento convertiti correttamente a UTC."""
        # 2024-04-05 18:14:38.123456789 UTC (approssimato)
        ts_ns = 1712345678_123_456_789
        result = _convert_timestamp(ts_ns)
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.year == 2024
        assert result.month == 4
        assert result.day == 5

    def test_nanoseconds_known_value(self) -> None:
        """Verifica conversione con valore noto."""
        # 2026-04-08 14:30:00.000000000 UTC
        # Unix epoch: 1775665800 secondi
        ts_ns = 1775665800_000_000_000
        result = _convert_timestamp(ts_ns)
        assert result is not None
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 8
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 0

    def test_milliseconds(self) -> None:
        """Millisecondi (> 1e12 < 1e15) convertiti correttamente."""
        ts_ms = 1775665800_000  # stessa data in ms
        result = _convert_timestamp(ts_ms)
        assert result is not None
        assert result.year == 2026
        assert result.hour == 14

    def test_seconds(self) -> None:
        """Secondi normali convertiti."""
        ts_s = 1775665800
        result = _convert_timestamp(ts_s)
        assert result is not None
        assert result.year == 2026

    def test_none(self) -> None:
        assert _convert_timestamp(None) is None

    def test_zero(self) -> None:
        assert _convert_timestamp(0) is None

    def test_negative(self) -> None:
        assert _convert_timestamp(-1) is None

    def test_nan(self) -> None:
        assert _convert_timestamp(float("nan")) is None

    def test_datetime_passthrough(self) -> None:
        """Datetime gia formato viene convertito a UTC."""
        dt = datetime(2026, 4, 8, 14, 30, tzinfo=timezone.utc)
        result = _convert_timestamp(dt)
        assert result == dt


# ============================================================
# Test Prezzo (fixed-point)
# ============================================================

class TestPriceConversion:

    def test_normal_float(self) -> None:
        """Prezzo float normale non viene scalato."""
        assert _convert_price(5023.25) == 5023.25

    def test_fixed_point_int64(self) -> None:
        """Prezzo fixed-point Databento (>1e10) diviso per 1e9."""
        # 5023.25 * 1e9 = 5023250000000
        result = _convert_price(5023_250_000_000)
        assert result is not None
        assert abs(result - 5023.25) < 0.001

    def test_fixed_point_5420(self) -> None:
        """Prezzo ES tipico come fixed-point."""
        # 5420.50 * 1e9
        result = _convert_price(5420_500_000_000)
        assert result is not None
        assert abs(result - 5420.5) < 0.001

    def test_small_float_not_scaled(self) -> None:
        """Prezzo piccolo (< 1e10) non viene diviso."""
        assert _convert_price(100.5) == 100.5
        assert _convert_price(9999.99) == 9999.99

    def test_zero(self) -> None:
        """Prezzo zero ritorna None."""
        assert _convert_price(0) is None

    def test_negative(self) -> None:
        """Prezzo negativo ritorna None."""
        assert _convert_price(-5023.25) is None

    def test_none(self) -> None:
        assert _convert_price(None) is None

    def test_nan(self) -> None:
        assert _convert_price(float("nan")) is None

    def test_inf(self) -> None:
        assert _convert_price(float("inf")) is None

    def test_string_price(self) -> None:
        """Prezzo come stringa convertito."""
        assert _convert_price("5023.25") == 5023.25


# ============================================================
# Test Symbol Mapping
# ============================================================

class TestSymbolMapping:

    def test_quarterly_es(self) -> None:
        """ESH6, ESM6, ESU6, ESZ5 -> 'ES'."""
        for sym in ("ESH6", "ESM6", "ESU6", "ESZ5", "ESH25"):
            result = _extract_ticker_root(sym)
            assert result == "ES", f"Failed for {sym}: got {result}"

    def test_quarterly_nq(self) -> None:
        """NQH6, NQM6 -> 'NQ'."""
        for sym in ("NQH6", "NQM6", "NQU5"):
            result = _extract_ticker_root(sym)
            assert result == "NQ", f"Failed for {sym}: got {result}"

    def test_continuous_contract(self) -> None:
        """ES.c.0 -> 'ES'."""
        assert _extract_ticker_root("ES.c.0") == "ES"
        assert _extract_ticker_root("NQ.c.0") == "NQ"

    def test_plain_ticker(self) -> None:
        """'ES' rimane 'ES'."""
        assert _extract_ticker_root("ES") == "ES"
        assert _extract_ticker_root("NQ") == "NQ"

    def test_case_insensitive(self) -> None:
        """Lowercase funziona."""
        assert _extract_ticker_root("esh6") == "ES"
        assert _extract_ticker_root("es.c.0") == "ES"

    def test_empty(self) -> None:
        assert _extract_ticker_root("") == ""


# ============================================================
# Test Side/Aggressor
# ============================================================

class TestSideConversion:

    def test_int_buyer(self) -> None:
        assert _convert_side(1) == "B"

    def test_int_seller(self) -> None:
        assert _convert_side(2) == "A"

    def test_string_buy(self) -> None:
        for s in ("B", "BUY", "BUYER", "1", "BID", "b", "buy"):
            assert _convert_side(s) == "B", f"Failed for {s}"

    def test_string_sell(self) -> None:
        for s in ("A", "ASK", "SELL", "SELLER", "2", "a"):
            assert _convert_side(s) == "A", f"Failed for {s}"

    def test_default_seller(self) -> None:
        """Valore sconosciuto -> default seller (conservativo)."""
        assert _convert_side(None) == "A"
        assert _convert_side(99) == "A"


# ============================================================
# Test DatabentoAdapter._parse_trade
# ============================================================

class TestParseTradeRecord:

    def test_complete_trade(self, tmp_path: Path) -> None:
        """Trade completo con tutti i campi."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=5420_500_000_000,  # fixed-point
            size=10,
            side=1,  # buyer
            flags=0,
            symbol="ESM6",
            trade_condition="0",
        )
        result = adapter._parse_trade(record)

        assert result is not None
        assert result["ts_event"].year == 2026
        assert result["ts_event"].hour == 14
        assert abs(result["price"] - 5420.5) < 0.001
        assert result["size"] == 10
        assert result["side"] == "B"
        assert result["ticker"] == "ES"
        assert result["source_type"] == "DATABENTO_STD"
        assert result["freq_type"] == "TICK"

    def test_normal_float_price(self, tmp_path: Path) -> None:
        """Trade con prezzo float normale (non scaled)."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=5420.5,  # gia float
            size=5,
            side=2,  # seller
            flags=0,
        )
        result = adapter._parse_trade(record)

        assert result is not None
        assert result["price"] == 5420.5
        assert result["side"] == "A"

    def test_zero_size_skipped(self, tmp_path: Path) -> None:
        """Trade con size=0 deve essere scartato."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=5420.5,
            size=0,
            side=1,
            flags=0,
        )
        result = adapter._parse_trade(record)
        assert result is None

    def test_none_price_skipped(self, tmp_path: Path) -> None:
        """Trade senza prezzo deve essere scartato."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=None,
            size=10,
            side=1,
            flags=0,
        )
        result = adapter._parse_trade(record)
        assert result is None

    def test_filter_irregular_trade(self, tmp_path: Path) -> None:
        """Con filter_irregular=True, trade non-regular scartato."""
        config = _make_config(tmp_path)
        config["databento"]["filter_irregular"] = True

        db = MagicMock()
        adapter = DatabentoAdapter(config, db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=5420.5,
            size=10,
            side=1,
            flags=0,
            trade_condition="Z",  # Late trade
        )
        result = adapter._parse_trade(record)
        assert result is None

    def test_keep_irregular_by_default(self, tmp_path: Path) -> None:
        """Di default, trade irregolari vengono tenuti (con sale_condition)."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=5420.5,
            size=10,
            side=1,
            flags=0,
            trade_condition="Z",
        )
        result = adapter._parse_trade(record)
        assert result is not None
        assert result["sale_condition"] == "Z"

    def test_symbol_fallback_to_config(self, tmp_path: Path) -> None:
        """Se il record non ha symbol, usa quello dal config."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)

        record = _FakeRecord(
            ts_event=1775665800_000_000_000,
            price=5420.5,
            size=10,
            side=1,
            flags=0,
            # Nessun campo symbol
        )
        result = adapter._parse_trade(record)
        assert result is not None
        assert result["ticker"] == "ES"  # da config symbol="ES.c.0"

    def test_stats_include_databento_info(self, tmp_path: Path) -> None:
        """get_stats() deve includere info Databento."""
        db = MagicMock()
        adapter = DatabentoAdapter(_make_config(tmp_path), db)
        adapter._running = True

        stats = adapter.get_stats()
        assert stats["symbol"] == "ES.c.0"
        assert stats["schema"] == "trades"
        assert stats["dataset"] == "GLBX.MDP3"
