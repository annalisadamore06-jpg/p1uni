"""
Integration Test - End-to-end test del sistema P1UNI

SPECIFICHE PER QWEN:
1. Mock dati in ingresso (simula tick da WS, P1, Databento)
2. Verifica normalizzazione: ticker aliases + colonne
3. Verifica validazione: rifiuta record con spot=0 o timestamp futuro
4. Verifica feature builder: dato N trades mockati, calcola 17 feature
5. Verifica model ensemble: con feature mockate, predict() ritorna confidence e direction
6. Verifica signal engine: rispetta fase temporale, non tradare in STOP
7. Verifica risk manager: blocca se daily loss >= 40pt
8. Verifica ninja bridge: segnale arriva al mock TCP server solo se tutto ok
"""

import json
import socket
import threading
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.core.normalizer import (
    normalize_ticker,
    normalize_columns,
    normalize_timestamp_utc,
    normalize_databento_trade,
    UniversalNormalizer,
    GEX_COLUMN_MAP,
    VALID_TICKERS,
    _clean_numeric,
    _clean_integer,
    _clean_string,
)
from src.core.validator import DataValidator
from src.execution.risk_manager import RiskManager


# ============================================================
# TEST 1: Normalizzazione Ticker
# ============================================================

class TestNormalizer:
    """Test del normalizer: ticker, colonne, timestamp."""

    def test_ticker_es_variants(self) -> None:
        """ES e tutte le sue varianti devono normalizzarsi a 'ES'."""
        for variant in ["ES", "ES_SPX", "ES.c.0", "ESM6", "ESZ5", "ESH6", "ESU5"]:
            assert normalize_ticker(variant) == "ES", f"Failed for {variant}"

    def test_ticker_spy_variants(self) -> None:
        """SPX e SPY devono normalizzarsi a 'SPY'."""
        for variant in ["SPX", "SPY", "SPDR"]:
            assert normalize_ticker(variant) == "SPY", f"Failed for {variant}"

    def test_ticker_all_9_gold_standard(self) -> None:
        """Tutti i 9 ticker Gold Standard devono mappare a se stessi."""
        for ticker in VALID_TICKERS:
            assert normalize_ticker(ticker) == ticker, f"Failed for {ticker}"

    def test_ticker_case_insensitive(self) -> None:
        """La mappatura deve funzionare case-insensitive."""
        assert normalize_ticker("es") == "ES"
        assert normalize_ticker("vix") == "VIX"
        assert normalize_ticker("Spy") == "SPY"

    def test_ticker_unknown(self) -> None:
        """Ticker sconosciuto deve ritornare None."""
        assert normalize_ticker("UNKNOWN") is None
        assert normalize_ticker("") is None
        assert normalize_ticker(None) is None

    def test_ticker_whitespace(self) -> None:
        """Ticker con spazi deve essere trimmato."""
        assert normalize_ticker("  ES  ") == "ES"
        assert normalize_ticker(" SPY ") == "SPY"

    def test_column_gex_mapping(self) -> None:
        """Colonne GEX devono essere mappate correttamente."""
        record = {
            "call_gex": 5480,
            "gamma_flip": 5400,
            "price": 5420.5,
            "symbol": "ES",
        }
        normalized = normalize_columns(record, GEX_COLUMN_MAP)
        assert normalized["call_wall_vol"] == 5480
        assert normalized["zero_gamma"] == 5400
        assert normalized["spot"] == 5420.5
        assert normalized["ticker"] == "ES"

    def test_column_mapping_immutable(self) -> None:
        """normalize_columns NON deve modificare il record originale."""
        original = {"call_gex": 5480, "price": 5420.5}
        original_copy = original.copy()
        normalize_columns(original, GEX_COLUMN_MAP)
        assert original == original_copy, "Original record was mutated!"

    def test_timestamp_utc_string(self) -> None:
        """Timestamp ISO8601 deve essere parsato come UTC."""
        ts = normalize_timestamp_utc("2026-04-08T14:30:00Z", source="WS")
        assert ts is not None
        assert ts.tzinfo is not None
        assert ts.hour == 14
        assert ts.minute == 30

    def test_timestamp_databento_nanosec(self) -> None:
        """Timestamp Databento nanosec deve essere convertito."""
        ts_ns = 1775665800_000_000_000
        ts = normalize_timestamp_utc(ts_ns, source="DATABENTO")
        assert ts is not None
        assert ts.year == 2026
        assert ts.tzinfo is not None

    def test_timestamp_p1lite_zurich(self) -> None:
        """P1-Lite timestamp Zurigo deve essere convertito a UTC.
        CET = UTC+1, CEST = UTC+2. Aprile = CEST (UTC+2).
        """
        # 2026-04-08 16:30:00 Zurigo (CEST) = 14:30:00 UTC
        ts = normalize_timestamp_utc("2026-04-08T16:30:00", source="P1_LITE")
        assert ts is not None
        assert ts.hour == 14, f"Expected 14 (UTC), got {ts.hour}"
        assert ts.minute == 30

    def test_timestamp_none(self) -> None:
        """None deve ritornare None."""
        assert normalize_timestamp_utc(None) is None

    def test_timestamp_nan_inf(self) -> None:
        """NaN e Inf devono ritornare None."""
        assert normalize_timestamp_utc(float("nan")) is None
        assert normalize_timestamp_utc(float("inf")) is None

    def test_normalize_databento_trade(self) -> None:
        """Trade Databento normalizzato correttamente."""
        raw = {
            "ts_event": 1775665800_000_000_000,
            "price": 5420.5,
            "size": 10,
            "side": 1,
            "flags": 0,
        }
        result = normalize_databento_trade(raw)
        assert result["side"] == "B"
        assert result["price"] == 5420.5
        assert result["size"] == 10

    def test_normalize_databento_immutable(self) -> None:
        """normalize_databento_trade NON deve modificare il record originale."""
        raw = {"ts_event": 1775665800_000_000_000, "price": 5420.5, "size": 10, "side": 1, "flags": 0}
        raw_copy = raw.copy()
        normalize_databento_trade(raw)
        assert raw == raw_copy, "Original record was mutated!"

    def test_normalize_databento_scaled_price(self) -> None:
        """Prezzo Databento scaled (> 1e12) deve essere diviso per 1e9."""
        raw = {"ts_event": 1775665800_000_000_000, "price": 5420_500_000_000, "size": 1, "side": "B", "flags": 0}
        result = normalize_databento_trade(raw)
        assert abs(result["price"] - 5420.5) < 0.01


# ============================================================
# TEST 1b: Pulizia Numerica
# ============================================================

class TestNumericCleaning:
    """Test delle funzioni di pulizia numerica."""

    def test_clean_numeric_float(self) -> None:
        assert _clean_numeric(5420.5) == 5420.5

    def test_clean_numeric_string(self) -> None:
        assert _clean_numeric("5420.50") == 5420.5

    def test_clean_numeric_empty_string(self) -> None:
        assert _clean_numeric("") is None

    def test_clean_numeric_none(self) -> None:
        assert _clean_numeric(None) is None

    def test_clean_numeric_nan(self) -> None:
        assert _clean_numeric(float("nan")) is None

    def test_clean_numeric_inf(self) -> None:
        assert _clean_numeric(float("inf")) is None

    def test_clean_numeric_negative_inf(self) -> None:
        assert _clean_numeric(float("-inf")) is None

    def test_clean_integer_from_string(self) -> None:
        assert _clean_integer("10") == 10
        assert _clean_integer("10.5") == 10

    def test_clean_integer_none(self) -> None:
        assert _clean_integer(None) is None

    def test_clean_string_empty(self) -> None:
        assert _clean_string("") is None
        assert _clean_string("  ") is None

    def test_clean_string_whitespace(self) -> None:
        assert _clean_string("  hello  ") == "hello"


# ============================================================
# TEST 1c: UniversalNormalizer classe
# ============================================================

class TestUniversalNormalizer:
    """Test della classe UniversalNormalizer."""

    def setup_method(self) -> None:
        self.normalizer = UniversalNormalizer()

    def test_normalize_ws_record(self) -> None:
        """Record WS normalizzato con colonne Gold Standard."""
        raw = {
            "symbol": "ES", "price": "5420.50", "call_gex": "5480",
            "gamma_flip": "5400", "timestamp": "2026-04-08T14:30:00Z",
        }
        result = self.normalizer.normalize(raw, source_type="WS")
        assert result["ticker"] == "ES"
        assert result["spot"] == 5420.5
        assert result["call_wall_vol"] == 5480.0
        assert result["zero_gamma"] == 5400.0
        assert result["source"] == "WS"
        assert result["ts_utc"] is not None
        assert result["ts_utc"].tzinfo is not None

    def test_normalize_databento_record(self) -> None:
        """Record Databento normalizzato."""
        raw = {"ts_event": 1775665800_000_000_000, "price": 5420.5, "size": 10, "side": 1, "flags": 0}
        result = self.normalizer.normalize(raw, source_type="DATABENTO")
        assert result["side"] == "B"
        assert result["price"] == 5420.5

    def test_normalize_p1lite_timestamp(self) -> None:
        """P1-Lite timestamp Zurigo convertito a UTC."""
        raw = {"timestamp": "2026-04-08T16:30:00", "spot": "5420.50"}
        result = self.normalizer.normalize(raw, source_type="P1_LITE")
        assert result["ts_utc"].hour == 14  # CEST: -2h
        assert result["spot"] == 5420.5
        assert result["ticker"] == "ES"

    def test_normalize_immutability(self) -> None:
        """normalize() NON deve modificare il record originale."""
        raw = {"symbol": "ES", "price": "5420.50", "timestamp": "2026-04-08T14:30:00Z"}
        raw_copy = {"symbol": "ES", "price": "5420.50", "timestamp": "2026-04-08T14:30:00Z"}
        self.normalizer.normalize(raw, source_type="WS")
        assert raw == raw_copy, "Original record was mutated!"

    def test_normalize_batch(self) -> None:
        """normalize_batch deve processare una lista."""
        records = [
            {"ts_event": 1775665800_000_000_000, "price": 5420.5, "size": 10, "side": 1, "flags": 0},
            {"ts_event": 1775665801_000_000_000, "price": 5421.0, "size": 5, "side": 2, "flags": 0},
        ]
        results = self.normalizer.normalize_batch(records, source_type="DATABENTO")
        assert len(results) == 2
        assert results[0]["side"] == "B"
        assert results[1]["side"] == "A"

    def test_normalize_empty_string_to_none(self) -> None:
        """Stringhe vuote devono diventare None."""
        raw = {"symbol": "ES", "price": "", "timestamp": "2026-04-08T14:30:00Z", "call_gex": "  "}
        result = self.normalizer.normalize(raw, source_type="WS")
        assert result["spot"] is None
        assert result["call_wall_vol"] is None

    def test_normalize_none_record_raises(self) -> None:
        """None come record deve sollevare ValueError."""
        with pytest.raises(ValueError, match="Invalid record type"):
            self.normalizer.normalize(None, source_type="WS")

    def test_normalize_non_dict_raises(self) -> None:
        """Tipo non-dict deve sollevare ValueError."""
        with pytest.raises(ValueError, match="Invalid record type"):
            self.normalizer.normalize("not a dict", source_type="WS")
        with pytest.raises(ValueError, match="Invalid record type"):
            self.normalizer.normalize([1, 2, 3], source_type="WS")


# ============================================================
# TEST 2: Validazione
# ============================================================

class TestValidator:
    """Test del validator: regole di ingresso dati."""

    def setup_method(self) -> None:
        self.validator = DataValidator(db_manager=None)

    def test_valid_record(self) -> None:
        """Record valido deve passare."""
        record = {
            "ticker": "ES",
            "spot": 5420.50,
            "ts_utc": datetime(2026, 4, 8, 14, 30, tzinfo=timezone.utc),
        }
        errors = self.validator.validate_record(record, "gex_summary")
        assert len(errors) == 0

    def test_invalid_ticker(self) -> None:
        """Ticker non valido deve essere rifiutato."""
        record = {"ticker": "INVALID", "spot": 100, "ts_utc": datetime.now(timezone.utc)}
        errors = self.validator.validate_record(record, "gex_summary")
        assert any("ticker" in str(e).lower() for e in errors)

    def test_spot_zero(self) -> None:
        """Spot = 0 deve essere rifiutato."""
        record = {"ticker": "ES", "spot": 0, "ts_utc": datetime.now(timezone.utc)}
        errors = self.validator.validate_record(record, "gex_summary")
        assert any("spot" in str(e).lower() for e in errors)

    def test_spot_negative(self) -> None:
        """Spot negativo deve essere rifiutato."""
        record = {"ticker": "ES", "spot": -100, "ts_utc": datetime.now(timezone.utc)}
        errors = self.validator.validate_record(record, "gex_summary")
        assert any("spot" in str(e).lower() for e in errors)

    def test_future_timestamp(self) -> None:
        """Timestamp nel futuro deve essere rifiutato."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        record = {"ticker": "ES", "spot": 5420, "ts_utc": future}
        errors = self.validator.validate_record(record, "gex_summary")
        assert any("futuro" in str(e).lower() for e in errors)

    def test_old_timestamp(self) -> None:
        """Timestamp troppo vecchio deve essere rifiutato."""
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        record = {"ticker": "ES", "spot": 5420, "ts_utc": old}
        errors = self.validator.validate_record(record, "gex_summary")
        assert any("vecchio" in str(e).lower() for e in errors)

    def test_required_fields_null(self) -> None:
        """Campi obbligatori NULL devono essere rifiutati."""
        record = {"ticker": "ES", "spot": 5420}
        errors = self.validator.validate_record(
            record, "gex_summary", required_fields=["ts_utc"]
        )
        assert any("ts_utc" in str(e) for e in errors)


# ============================================================
# TEST 3: Risk Manager
# ============================================================

class TestRiskManager:
    """Test del risk manager."""

    def setup_method(self) -> None:
        self.config = {
            "risk": {
                "max_daily_loss_pts": 40,
                "max_consec_losses": 5,
                "reset_hour_utc": 22,
            }
        }
        self.rm = RiskManager(self.config)

    def test_initial_can_trade(self) -> None:
        """Inizialmente il trading deve essere consentito."""
        assert self.rm.can_trade() is True

    def test_daily_loss_limit(self) -> None:
        """Dopo aver perso 40+ punti, il trading deve essere bloccato."""
        self.rm.update_trade_result(-20)  # -20 pts
        assert self.rm.can_trade() is True
        self.rm.update_trade_result(-21)  # -41 pts totali
        assert self.rm.can_trade() is False

    def test_consecutive_losses(self) -> None:
        """Dopo 5 loss consecutive, il trading deve essere bloccato."""
        for _ in range(5):
            self.rm.update_trade_result(-2)  # piccole perdite
        assert self.rm.can_trade() is False

    def test_win_resets_consec(self) -> None:
        """Un trade positivo resetta il contatore consecutive losses."""
        self.rm.update_trade_result(-2)
        self.rm.update_trade_result(-2)
        self.rm.update_trade_result(-2)
        assert self.rm.consecutive_losses == 3
        self.rm.update_trade_result(5)  # win!
        assert self.rm.consecutive_losses == 0

    def test_daily_reset(self) -> None:
        """Reset giornaliero deve azzerare tutto."""
        self.rm.update_trade_result(-30)
        self.rm.reset_daily()
        assert self.rm.daily_pnl_pts == 0
        assert self.rm.consecutive_losses == 0
        assert self.rm.can_trade() is True

    def test_status(self) -> None:
        """get_status() deve ritornare dati corretti."""
        self.rm.update_trade_result(10)
        status = self.rm.get_status()
        assert status["daily_pnl_pts"] == 10
        assert status["daily_pnl_usd"] == 500  # 10 * $50
        assert status["total_trades"] == 1
        assert status["win_rate"] == 1.0


# ============================================================
# TEST 4: Ticker completezza
# ============================================================

class TestTickerCompleteness:
    """Verifica che tutti i 9 ticker siano coperti."""

    def test_all_valid_tickers(self) -> None:
        """I 9 ticker Gold Standard devono essere definiti."""
        expected = {"ES", "NQ", "SPY", "QQQ", "VIX", "IWM", "TLT", "GLD", "UVXY"}
        assert VALID_TICKERS == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
