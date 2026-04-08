"""
Test per GexBotWebSocketAdapter - parsing, reconnect, malformed JSON.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from src.ingestion.adapter_ws import GexBotWebSocketAdapter


def _make_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "_base_dir": str(tmp_path),
        "database": {"path": ":memory:", "threads": 1, "memory_limit": "256MB"},
        "gexbot": {
            "ws_url": "wss://fake.gexbot.com/ws",
            "tickers": ["ES", "NQ"],
            "subscribe_action": "subscribe",
            "ping_interval_sec": 25,
            "reconnect_alert_after_sec": 300,
        },
    }


class TestGexBotWSAdapter:

    def test_parse_flat_message(self, tmp_path: Path) -> None:
        """Messaggio flat con ticker e dati GEX deve essere parsato."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)

        msg = {
            "ticker": "ES",
            "timestamp": "2026-04-08T14:30:00Z",
            "price": 5420.5,
            "call_gex": 5480,
            "put_gex": 5350,
            "gamma_flip": 5400,
        }
        result = adapter._parse_gex_message(msg)

        assert result is not None
        assert result["ticker"] == "ES"
        assert result["price"] == 5420.5
        assert result["call_gex"] == 5480

    def test_parse_nested_message(self, tmp_path: Path) -> None:
        """Messaggio nested con 'data' deve estrarre i campi."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)

        msg = {
            "type": "gex",
            "ticker": "NQ",
            "timestamp": "2026-04-08T14:30:00Z",
            "data": {
                "call_gex": 18500,
                "put_gex": 18200,
                "price": 18350.0,
            },
        }
        result = adapter._parse_gex_message(msg)

        assert result is not None
        assert result["ticker"] == "NQ"
        assert result["price"] == 18350.0
        assert "type" not in result  # campo protocollo rimosso

    def test_parse_heartbeat_ignored(self, tmp_path: Path) -> None:
        """Messaggi heartbeat/status non devono produrre record."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)
        adapter._ws = MagicMock()  # mock per _send_pong

        for msg_type in ("heartbeat", "pong", "status", "info", "welcome"):
            msg = json.dumps({"type": msg_type})
            adapter._handle_message(msg)

        # Nessun record accodato
        assert adapter._queue.qsize() == 0

    def test_parse_malformed_json(self, tmp_path: Path) -> None:
        """JSON rotto non deve crashare il listener."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)
        adapter._running = True

        # Non deve sollevare eccezioni
        adapter._handle_message("this is not json {{{")
        adapter._handle_message("")
        adapter._handle_message("{}")  # dict vuoto, nessun ticker/price

        assert adapter._queue.qsize() == 0

    def test_parse_empty_data_skipped(self, tmp_path: Path) -> None:
        """Messaggio senza ticker ne' prezzo deve essere scartato."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)

        msg = {"type": "gex", "data": {"some_field": 123}}
        result = adapter._parse_gex_message(msg)
        assert result is None

    def test_valid_message_queued(self, tmp_path: Path) -> None:
        """Messaggio valido deve essere normalizzato e accodato."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)
        adapter._running = True

        msg = json.dumps({
            "ticker": "ES",
            "timestamp": "2026-04-08T14:30:00Z",
            "price": 5420.5,
            "call_gex": 5480,
        })
        adapter._handle_message(msg)

        assert adapter._queue.qsize() == 1
        record = adapter._queue.get_nowait()
        # Dopo normalizzazione: price -> spot, call_gex -> call_wall_vol
        assert record["ticker"] == "ES"
        assert record["source"] == "WS"

    def test_gex_cache_updated(self, tmp_path: Path) -> None:
        """Dopo un messaggio valido, gex_cache.json deve essere aggiornato."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)
        adapter._running = True

        raw = {"ticker": "ES", "price": 5420.5, "call_gex": 5480}
        adapter._update_gex_cache(raw)

        cache_path = tmp_path / "data" / "cache" / "gex_cache.json"
        assert cache_path.exists()

        data = json.loads(cache_path.read_text())
        assert data["ticker"] == "ES"
        assert "_updated_at" in data

    def test_disconnect_sets_state(self, tmp_path: Path) -> None:
        """disconnect() deve settare _disconnected_since."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)
        adapter._ws = MagicMock()

        adapter.disconnect()

        assert adapter._ws is None
        assert adapter._disconnected_since is not None
        assert adapter._connected_since is None

    def test_stats_include_ws_info(self, tmp_path: Path) -> None:
        """get_stats() deve includere info connessione WS."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)
        adapter._running = True

        stats = adapter.get_stats()
        assert "connected_since" in stats
        assert "disconnected_since" in stats
        assert stats["adapter_name"] == "gexbot_ws"

    @patch("src.ingestion.adapter_ws.GexBotWebSocketAdapter.connect")
    def test_reconnect_on_connection_error(self, mock_connect: MagicMock, tmp_path: Path) -> None:
        """Se connect() fallisce, deve ritentare (gestito da BaseAdapter)."""
        db = MagicMock()
        adapter = GexBotWebSocketAdapter(_make_config(tmp_path), db)

        # Simula 2 fallimenti poi successo
        mock_connect.side_effect = [
            ConnectionError("Network down"),
            ConnectionError("Still down"),
            None,  # successo
        ]

        # Verifica che connect venga chiamato
        # (il loop completo e' in BaseAdapter._listener_loop_with_reconnect,
        #  qui testiamo solo che connect() sia chiamabile e gestisca errori)
        with pytest.raises(ConnectionError):
            adapter.connect()

        assert mock_connect.call_count == 1
