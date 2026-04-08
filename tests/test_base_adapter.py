"""
Test per BaseAdapter - batch writing, retry, quarantena, backpressure.
"""

from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.base_adapter import BaseAdapter, AdapterMetrics


# ============================================================
# Adapter concreto per test
# ============================================================

class FakeAdapter(BaseAdapter):
    """Adapter concreto per testing. Non si connette a nulla."""

    def __init__(self, config: dict, db: Any, **kwargs: Any) -> None:
        super().__init__(
            adapter_name="fake",
            source_type="WS",
            target_table="gex_summary",
            config=config,
            db=db,
            **kwargs,
        )
        self._connected = False
        self._records_to_feed: list[dict] = []

    def connect(self) -> None:
        self._connected = True

    def listen(self) -> None:
        for record in self._records_to_feed:
            if not self._running:
                break
            self.process_message(record)

    def disconnect(self) -> None:
        self._connected = False


def _make_config(tmp_path: Path) -> dict:
    return {
        "_base_dir": str(tmp_path),
        "database": {"path": ":memory:", "threads": 1, "memory_limit": "256MB"},
    }


def _make_gex_record(spot: float = 5420.5, ticker: str = "ES") -> dict:
    return {
        "symbol": ticker,
        "price": str(spot),
        "timestamp": "2026-04-08T14:30:00Z",
        "hub": "classic",
        "aggregation": "gex_full",
    }


# ============================================================
# Test AdapterMetrics
# ============================================================

class TestAdapterMetrics:

    def test_thread_safe_increments(self) -> None:
        """Incrementi concorrenti devono essere consistenti."""
        m = AdapterMetrics()
        n_threads = 10
        n_per_thread = 1000

        def inc():
            for _ in range(n_per_thread):
                m.inc_received()

        threads = [threading.Thread(target=inc) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert m.records_received == n_threads * n_per_thread

    def test_get_stats(self) -> None:
        m = AdapterMetrics()
        m.inc_received(100)
        m.inc_validated(90)
        m.inc_quarantined(10)
        m.inc_batches()
        stats = m.get_stats()
        assert stats["records_received"] == 100
        assert stats["records_validated_ok"] == 90
        assert stats["records_quarantined"] == 10
        assert stats["batches_written"] == 1
        assert stats["last_write_ts"] is not None


# ============================================================
# Test BaseAdapter
# ============================================================

class TestBaseAdapter:

    def test_process_message_normalizes_and_queues(self, tmp_path: Path) -> None:
        """process_message deve normalizzare e accodare."""
        db = MagicMock()
        adapter = FakeAdapter(_make_config(tmp_path), db)
        adapter._running = True

        record = _make_gex_record()
        adapter.process_message(record)

        assert adapter._queue.qsize() == 1
        assert adapter.metrics.records_received == 1

        queued = adapter._queue.get_nowait()
        assert queued["ticker"] == "ES"
        assert queued["source"] == "WS"

    def test_process_message_rejects_none(self, tmp_path: Path) -> None:
        """process_message con None non deve crashare."""
        db = MagicMock()
        adapter = FakeAdapter(_make_config(tmp_path), db)
        adapter._running = True

        adapter.process_message(None)  # type: ignore
        # Non crasha, ma non accoda nulla
        assert adapter._queue.qsize() == 0

    def test_backpressure_warning(self, tmp_path: Path) -> None:
        """Queue piena deve generare warning/scartare il piu vecchio."""
        db = MagicMock()
        telegram = MagicMock()
        adapter = FakeAdapter(_make_config(tmp_path), db, telegram=telegram)
        adapter._running = True
        adapter.QUEUE_MAX_SIZE = 5
        adapter.QUEUE_WARN_SIZE = 3

        # Ricrea queue con size ridotto per test
        adapter._queue = queue.Queue(maxsize=5)

        # Riempi la queue
        for i in range(5):
            adapter._queue.put({"i": i})

        # Il prossimo deve scartare il piu vecchio
        adapter.process_message(_make_gex_record())

        assert adapter._queue.qsize() == 5  # rimane piena ma non crasha

    def test_flush_queue_writes_batch(self, tmp_path: Path) -> None:
        """_flush_queue deve validare e scrivere su DB."""
        db = MagicMock()
        # Mock: execute_write non fa nulla, execute_read ritorna empty
        db.get_writer.return_value = MagicMock()

        adapter = FakeAdapter(_make_config(tmp_path), db)
        adapter._running = True

        # Popola la queue con record validi
        for _ in range(10):
            adapter._queue.put(_make_gex_record())

        # Mock del validator per far passare tutto
        adapter.validator = MagicMock()
        adapter.validator.validate_batch_and_quarantine.return_value = pd.DataFrame(
            [_make_gex_record() for _ in range(10)]
        )

        adapter._flush_queue()

        # Verifica che il batch sia stato scritto
        assert adapter.metrics.batches_written == 1
        assert adapter.metrics.records_validated_ok == 10

    def test_db_retry_then_quarantine_file(self, tmp_path: Path) -> None:
        """Se il DB fallisce dopo tutti i retry, scrivi in quarantena file."""
        db = MagicMock()
        writer = MagicMock()
        writer.execute.side_effect = Exception("DB locked")
        db.get_writer.return_value = writer

        config = _make_config(tmp_path)
        adapter = FakeAdapter(config, db)
        adapter._running = True
        adapter.DB_RETRY_DELAYS = (0.01, 0.01)  # retry veloci per test

        df = pd.DataFrame([_make_gex_record() for _ in range(5)])
        adapter._write_to_db_with_retry(df)

        # Verifica quarantena file creata
        quarantine_files = list((tmp_path / "data" / "quarantine").rglob("*.json"))
        assert len(quarantine_files) == 1

        # Verifica contenuto
        data = json.loads(quarantine_files[0].read_text())
        assert data["adapter"] == "fake"
        assert data["record_count"] == 5
        assert "DB locked" in data["error_reason"]

    def test_quarantine_creates_date_folder(self, tmp_path: Path) -> None:
        """La quarantena deve creare la cartella YYYY-MM-DD se non esiste."""
        db = MagicMock()
        config = _make_config(tmp_path)
        adapter = FakeAdapter(config, db)

        df = pd.DataFrame([{"a": 1}])
        adapter._write_to_quarantine_file(df, "test error")

        # Verifica che la cartella con data esiste
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_dir = tmp_path / "data" / "quarantine" / today
        assert day_dir.exists()
        assert len(list(day_dir.glob("*.json"))) == 1

    def test_10k_records_throughput(self, tmp_path: Path) -> None:
        """10.000 record accodati velocemente non devono bloccare."""
        db = MagicMock()
        adapter = FakeAdapter(_make_config(tmp_path), db)
        adapter._running = True

        start = time.perf_counter()
        for _ in range(10_000):
            adapter.process_message(_make_gex_record())
        elapsed = time.perf_counter() - start

        assert adapter._queue.qsize() == 10_000
        assert adapter.metrics.records_received == 10_000
        # Deve completare in meno di 5 secondi (normalizzazione + queue)
        assert elapsed < 5.0, f"Too slow: {elapsed:.2f}s"

    def test_get_stats(self, tmp_path: Path) -> None:
        """get_stats deve ritornare metriche + queue size."""
        db = MagicMock()
        adapter = FakeAdapter(_make_config(tmp_path), db)
        adapter._running = True
        adapter._queue.put({"x": 1})

        stats = adapter.get_stats()
        assert stats["queue_size"] == 1
        assert stats["adapter_name"] == "fake"
        assert stats["target_table"] == "gex_summary"
        assert stats["running"] is True
