"""
GexBot WebSocket Adapter — Azure Web PubSub + Protobuf + Zstandard

Basato su codice ufficiale: https://github.com/nfa-llc/quant-python-sockets

Protocollo:
  1. GET /negotiate con API key -> ottiene websocket_urls per ogni hub
  2. Connette a hub via Azure Web PubSub client
  3. Joina gruppi: {prefix}_{ticker}_{package}_{category}
  4. Riceve Protobuf compresso con Zstandard -> decomprime -> parsa
  5. Invia a BaseAdapter.process_message() -> queue -> batch writer -> DB

Hubs: classic, state_gex, state_greeks_zero, state_greeks_one, orderflow
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from google.protobuf import any_pb2

from src.core.database import DatabaseManager
from src.ingestion.base_adapter import BaseAdapter

logger = logging.getLogger("p1uni.ingestion.gexbot")

NEGOTIATE_URL = "https://api.gexbot.com/negotiate"

# Aggiungi path per proto generati
_INGESTION_DIR = str(Path(__file__).parent)
if _INGESTION_DIR not in sys.path:
    sys.path.insert(0, _INGESTION_DIR)


class GexBotWebSocketAdapter(BaseAdapter):
    """Adapter GexBot via Azure Web PubSub. Eredita BaseAdapter."""

    def __init__(self, config: dict[str, Any], db: DatabaseManager, telegram: Any = None) -> None:
        gexbot_cfg = config.get("gexbot", {})
        super().__init__(
            adapter_name="gexbot_ws", source_type="WS", target_table="gex_summary",
            config=config, db=db, telegram=telegram,
        )
        self.api_key: str = gexbot_cfg.get("api_key", os.environ.get("GEXBOT_API_KEY", ""))
        self.tickers: list[str] = gexbot_cfg.get("tickers", ["ES_SPX"])
        self.hubs_config: dict[str, list[str]] = gexbot_cfg.get("hubs", {
            "classic": ["gex_full", "gex_zero", "gex_one"],
            "state_greeks_zero": ["delta_zero", "gamma_zero", "vanna_zero", "charm_zero"],
            "state_greeks_one": ["delta_one", "gamma_one", "vanna_one", "charm_one"],
            "orderflow": ["orderflow"],
        })
        self._clients: list[Any] = []
        self._prefix: str = "red"
        base_dir = Path(config.get("_base_dir", "."))
        self._gex_cache_path = base_dir / "data" / "cache" / "gex_cache.json"

    def connect(self) -> None:
        """Negozia con GexBot API e connette a tutti gli hub."""
        if not self.api_key:
            raise ConnectionError("gexbot.api_key non configurato")

        from decompression_utils import decompress_gex_message, decompress_greek_message, decompress_orderflow_message
        self._decompress_gex = decompress_gex_message
        self._decompress_greek = decompress_greek_message
        self._decompress_orderflow = decompress_orderflow_message

        self._log.info("Negotiating with GexBot API...")
        resp = requests.get(NEGOTIATE_URL, headers={"Authorization": f"Basic {self.api_key}"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if "websocket_urls" not in data:
            raise ConnectionError(f"Negotiate failed: {data}")

        self._prefix = data.get("prefix", "red")
        ws_urls = data["websocket_urls"]
        self._log.info(f"Negotiated OK. Prefix={self._prefix}. Hubs={list(ws_urls.keys())}")

        from azure.messaging.webpubsubclient import WebPubSubClient
        from azure.messaging.webpubsubclient.models import CallbackType

        for hub_key, url in ws_urls.items():
            groups = self._build_groups(hub_key)
            if not groups:
                continue

            client = WebPubSubClient(url)

            def _on_conn(event, hk=hub_key, grps=groups, cl=client):
                self._log.info(f"[{hk}] Connected ({event.connection_id})")
                for g in grps:
                    try:
                        cl.join_group(g)
                        self._log.info(f"[{hk}] Joined {g}")
                    except Exception as e:
                        self._log.error(f"[{hk}] Join fail {g}: {e}")

            def _on_msg(event, hk=hub_key):
                self._on_group_message(hk, event)

            def _on_disc(event, hk=hub_key):
                self._log.warning(f"[{hk}] Disconnected: {event.message}")

            client.subscribe(CallbackType.CONNECTED, _on_conn)
            client.subscribe(CallbackType.GROUP_MESSAGE, _on_msg)
            client.subscribe(CallbackType.DISCONNECTED, _on_disc)

            t = threading.Thread(target=client.open, daemon=True, name=f"gexbot-{hub_key}")
            t.start()
            self._clients.append(client)
            self._log.info(f"[{hub_key}] Started with {len(groups)} groups")

    def listen(self) -> None:
        """Blocca finche' running. I client Azure girano in thread propri."""
        while self._running and not self._shutdown_event.is_set():
            self._shutdown_event.wait(1.0)

    def disconnect(self) -> None:
        for c in self._clients:
            try:
                c.close()
            except Exception:
                pass
        self._clients.clear()

    def _build_groups(self, hub_key: str) -> list[str]:
        hub_map = {
            "classic": ("classic", self.hubs_config.get("classic", [])),
            "state_gex": ("state", self.hubs_config.get("state_gex", [])),
            "state_greeks_zero": ("state", self.hubs_config.get("state_greeks_zero", [])),
            "state_greeks_one": ("state", self.hubs_config.get("state_greeks_one", [])),
            "orderflow": ("orderflow", self.hubs_config.get("orderflow", [])),
        }
        package, categories = hub_map.get(hub_key, ("", []))
        if not categories:
            return []
        return [f"{self._prefix}_{tk}_{package}_{cat}" for tk in self.tickers for cat in categories]

    def _on_group_message(self, hub_key: str, event: Any) -> None:
        try:
            any_msg = any_pb2.Any()
            any_msg.ParseFromString(event.data)
            type_url = any_msg.type_url
            category = self._extract_category(event.group)

            if "proto.gex" in type_url or "Gex" in type_url:
                data = self._decompress_gex(any_msg)
                if data:
                    self._ingest_gex(data, hub_key, category)
            elif "proto.option" in type_url or "OptionProfile" in type_url:
                data = self._decompress_greek(any_msg, category)
                if data:
                    self._ingest_greek(data, category)
            elif "proto.orderflow" in type_url or "Orderflow" in type_url:
                data = self._decompress_orderflow(any_msg)
                if data:
                    self._ingest_orderflow(data)
        except Exception as e:
            self._log.error(f"[{hub_key}] Parse error: {e}")

    @staticmethod
    def _extract_category(group_name: str) -> str:
        for pkg in ("classic", "state", "orderflow"):
            sep = f"_{pkg}_"
            if sep in group_name:
                return group_name.split(sep)[-1]
        return ""

    def _ingest_gex(self, data: dict, hub_key: str, category: str) -> None:
        record = {
            "timestamp": data.get("timestamp"), "ticker": data.get("ticker"),
            "spot": data.get("spot"), "zero_gamma": data.get("zero_gamma"),
            "call_wall_vol": data.get("major_pos_vol"), "call_wall_oi": data.get("major_pos_oi"),
            "put_wall_vol": data.get("major_neg_vol"), "put_wall_oi": data.get("major_neg_oi"),
            "net_gex_vol": data.get("sum_gex_vol"), "net_gex_oi": data.get("sum_gex_oi"),
            "delta_rr": data.get("delta_risk_reversal"),
            "hub": "classic" if "classic" in hub_key else "state_gex",
            "aggregation": category,
            "n_strikes": len(data.get("strikes", [])),
            "min_dte": data.get("min_dte"), "sec_min_dte": data.get("sec_min_dte"),
        }
        self.process_message(record)
        self._update_gex_cache(record)

    def _ingest_greek(self, data: dict, category: str) -> None:
        parts = category.split("_")
        greek_type = parts[0] if parts else "unknown"
        dte_type = parts[-1] if len(parts) > 1 else "zero"
        record = {
            "timestamp": data.get("timestamp"), "ticker": data.get("ticker"),
            "spot": data.get("spot"), "greek_type": greek_type, "dte_type": dte_type,
            "major_positive": data.get("major_positive"),
            "major_negative": data.get("major_negative"),
            "major_long_gamma": data.get("major_long_gamma"),
            "major_short_gamma": data.get("major_short_gamma"),
            "hub": "classic", "min_dte": data.get("min_dte"), "sec_min_dte": data.get("sec_min_dte"),
        }
        self.process_message(record)

    def _ingest_orderflow(self, data: dict) -> None:
        self.process_message(data)

    def _update_gex_cache(self, record: dict) -> None:
        try:
            cache = {k: str(v) if isinstance(v, datetime) else v for k, v in record.items()}
            cache["_updated_at"] = datetime.now(timezone.utc).isoformat()
            self._gex_cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._gex_cache_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(cache, indent=2, default=str))
            tmp.replace(self._gex_cache_path)
        except Exception:
            pass
