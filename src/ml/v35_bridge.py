"""
V3.5 Bridge — Integra il motore V35ProductionEngine nel sistema P1UNI.

ARCHITETTURA DATA-FLOW (in ordine di priorità):

  1. data/gexbot_latest.json  — scritto da gexbot_scraper.py ogni 60s
                                (scrittura atomica, nessun lock issue)
  2. DB live (p1uni_live.duckdb) — fallback se il JSON è stale
  3. DB gold (ml_gold.duckdb)   — fallback storico read-only

FEATURE CALCOLO:
  • Feature raw dai dati GEX/Greeks/Orderflow
  • Feature derivate (spot_vs_zero_gamma, wall_spread, etc.)
  • Feature temporali (L1/L2/L3/D1 lags, MA3/STD3/MA6/STD6/MA12/STD12/MOM)
    calcolate dalla history in gex_zero_history del JSON
  • Feature temporali (hour_utc, minutes_since_open, minutes_to_close)
"""

from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

logger = logging.getLogger("p1uni.ml.v35_bridge")

# ── Mapping: (package, category) → {feature_name: column/raw_key} ─────────────
# Defines how each API category maps to the V3.5 feature space.

# Categories whose structured columns provide "zero DTE" GEX walls
_GEX_ZERO_CATS = {"gex_zero", "gex_full"}
# Categories for "one DTE" (next expiry)
_GEX_ONE_CATS  = {"gex_one"}


class V35Bridge:
    """Bridge tra P1UNI e il motore V3.5 certificato.

    Carica l'engine V3.5, legge feature dal gexbot_latest.json (scritto ogni 60s
    da gexbot_scraper.py), produce segnali con i 4 Risk Gates.
    """

    def __init__(self, config: dict[str, Any], db: Any = None) -> None:
        self._base_dir = Path(config.get("_base_dir", "."))
        models_dir = self._base_dir / "ml_models" / "v35_production"

        # Percorso gexbot_latest.json (scritto da gexbot_scraper ogni 60s)
        self.snapshots_json_path = self._base_dir / "data" / "gexbot_latest.json"

        # DB live (DatabaseManager singleton) — fallback
        self.live_db = db

        # DB gold path (read-only, fallback storico)
        self.gold_db_path: str = config.get("database", {}).get(
            "gold_path",
            r"C:\Users\annal\Desktop\ML DATABASE\ml_gold.duckdb",
        )

        # Carica feature names
        feat_path = models_dir / "feature_names_v35_production.json"
        with open(feat_path) as f:
            self.feature_names: list[str] = json.load(f)
        self._feature_set = set(self.feature_names)
        logger.info(f"V3.5 Bridge: {len(self.feature_names)} features loaded")

        # Carica engine V3.5
        from ml_models.v35_production.live_inference_v35 import V35ProductionEngine
        self.engine = V35ProductionEngine(models_dir=models_dir, enable_gates=True)
        self.engine.load()

        # Cache
        self._last_features: dict[str, float] = {}
        self._last_update: float = 0
        self._live_data_age_sec: float = 9999.0

        # BUG#9 fix: history saved here, temporal features computed AFTER derived features
        self._last_json_history: list[dict] = []
        self._last_json_now: datetime = datetime.utcnow()

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────────

    def predict(self, current_time: datetime | None = None) -> dict[str, Any] | None:
        """Genera un segnale usando il motore V3.5."""
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        t0 = time.perf_counter()

        features = self._build_v35_features(current_time)
        if features is None:
            return None

        # Feature temporali
        features["hour_utc"] = float(current_time.hour)
        features["minutes_since_open"] = self._calc_minutes_since_open(current_time)
        # BUG#4 fix: ES overnight close is 22:00 UTC (17:00 ET).
        # For hours after midnight (0-21), add 24h to avoid negative wrap.
        _mins_now = current_time.hour * 60 + current_time.minute
        _close_mins = 22 * 60  # 22:00 UTC
        if _mins_now <= _close_mins:
            _to_close = _close_mins - _mins_now
        else:
            # Past 22:00 UTC — next close is 22:00 next day
            _to_close = (24 * 60 - _mins_now) + _close_mins
        features["minutes_to_close"] = float(max(0, _to_close))
        features["day_of_week"] = float(current_time.weekday())

        # Flags orari
        hour = current_time.hour
        mins = current_time.hour * 60 + current_time.minute
        us_open = 14 * 60 + 30  # 09:30 ET = 14:30 UTC
        features["is_first_hour"]   = 1.0 if us_open <= mins <= us_open + 60 else 0.0
        features["is_last_hour"]    = 1.0 if 20 * 60 <= mins <= 21 * 60 else 0.0
        features["is_power_hour"]   = 1.0 if 20 * 60 <= mins <= 21 * 60 else 0.0
        features["is_hedging_hour"] = 1.0 if hour in (14, 15, 20, 21) else 0.0
        features["is_close_hedging"]= 1.0 if 20 * 60 <= mins else 0.0

        # VIX default conservativo se non disponibile
        if "vix" not in features or features.get("vix", 0) == 0:
            # Stima VIX da condizioni di mercato
            # In regime positivo gamma (net_gex > 0): VIX tipicamente bassa (14-20)
            # In regime negativo gamma (net_gex < 0): VIX tipicamente alta (20-35)
            net_gex = features.get("net_gex_oi", 0)
            delta_rr = features.get("delta_rr", 0)
            # Se delta_rr molto negativo (put skew elevato) → VIX alta
            if delta_rr < -0.5:
                features["vix"] = 25.0  # stressed market
            elif delta_rr < -0.2:
                features["vix"] = 20.0  # elevated risk
            elif net_gex < 0:
                features["vix"] = 22.0  # negative gamma = higher vol
            else:
                features["vix"] = 16.0  # calm positive gamma market

        # Aggiungi feature di interazione derivate di ordine superiore
        self._add_interaction_features(features)

        # Fill missing con 0
        full_features = {name: features.get(name, 0.0) for name in self.feature_names}

        # Predict
        timestamp_str = current_time.isoformat()
        signal = self.engine.predict(full_features, timestamp=timestamp_str)

        elapsed_ms = (time.perf_counter() - t0) * 1000

        direction_map = {"UP": "LONG", "DOWN": "SHORT", "FLAT": "NEUTRAL"}
        result = {
            "signal": direction_map.get(signal.direction, "NEUTRAL"),
            "confidence": signal.confidence,
            "mode": "V35_FULL" if not signal.blocked else "V35_BLOCKED",
            "votes": {
                "seed42": "LONG" if signal.raw_probability > 0.5 else "SHORT",
                "agreement": signal.model_agreement,
            },
            "probabilities": {
                "raw": signal.raw_probability,
                "confidence": signal.confidence,
            },
            "signal_class": signal.signal_class.value,
            "position_size_fraction": signal.position_size,
            "blocked": signal.blocked,
            "block_reason": signal.block_reason,
            "gate_results": [
                {"gate": g.gate_name, "action": g.action, "reason": g.reason}
                for g in signal.gate_results
            ],
            "elapsed_ms": round(elapsed_ms, 1),
            "data_age_sec": round(self._live_data_age_sec, 0),
        }

        if signal.blocked:
            result["signal"] = "NEUTRAL"

        logger.info(
            f"V3.5: {result['signal']} conf={signal.confidence:.3f} "
            f"class={signal.signal_class.value} blocked={signal.blocked} "
            f"age={self._live_data_age_sec:.0f}s [{elapsed_ms:.1f}ms]"
        )

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # FEATURE BUILDING — main pipeline
    # ─────────────────────────────────────────────────────────────────────────

    def _build_v35_features(self, now: datetime) -> dict[str, float] | None:
        """
        Costruisce il vettore feature a 222 dimensioni.

        Ordine di priorità:
          1. gexbot_latest.json (scritto ogni 60s, zero lock issue)
          2. DB live (fallback se JSON stale > 10 min)
          3. DB gold (fallback storico)
          4. Cache recente (< 5 min)
        """
        features: dict[str, float] = {}
        data_age = 9999.0

        # ── 1. PRIMARY: gexbot_latest.json ────────────────────────────────
        try:
            features, data_age = self._read_gexbot_latest_json(now)
            self._live_data_age_sec = data_age
            if features:
                logger.debug(
                    f"[json] {len(features)} features, age={data_age:.0f}s"
                )
        except Exception as e:
            logger.debug(f"gexbot_latest.json read: {e}")

        STALE_THRESHOLD_SEC = 600  # 10 minuti

        # ── 2. FALLBACK: DB live ───────────────────────────────────────────
        if (data_age > STALE_THRESHOLD_SEC or not features) and self.live_db is not None:
            try:
                live_f, live_age = self._query_gex_features(
                    db_executor=self.live_db.execute_read,
                    label="live",
                )
                for k, v in live_f.items():
                    if k not in features:
                        features[k] = v
                if live_age < data_age:
                    data_age = live_age
                    self._live_data_age_sec = data_age
            except Exception as e:
                logger.debug(f"Live DB GEX read: {e}")

        # ── 3. FALLBACK: DB gold ───────────────────────────────────────────
        if data_age > STALE_THRESHOLD_SEC or not features:
            if data_age < 9999:
                logger.warning(
                    f"GEX stale ({data_age:.0f}s) — falling back to gold DB"
                )
            conn = None
            try:
                conn = duckdb.connect(self.gold_db_path, read_only=True)

                def gold_executor(query: str, params: list | None = None):
                    import pandas as pd
                    try:
                        if params:
                            return pd.DataFrame(conn.execute(query, params).fetchdf())
                        return pd.DataFrame(conn.execute(query).fetchdf())
                    except Exception:
                        return pd.DataFrame()

                gold_f, gold_age = self._query_gex_features(
                    db_executor=gold_executor,
                    label="gold",
                )
                for k, v in gold_f.items():
                    if k not in features:
                        features[k] = v
            except Exception as e:
                logger.debug(f"Gold DB read: {e}")
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        # ── 4. Feature derivate ────────────────────────────────────────────
        self._add_derived_features(features)

        # ── 4b. Temporal features (AFTER derived so mr1d/mr1u are populated) ──
        # BUG#9 fix: was called inside _read_gexbot_latest_json BEFORE _add_derived_features
        # causing mr1d/mr1u=0 → spot_in_mr1_pct proxy scale ±2% instead of 0-100%
        if self._last_json_history:
            self._add_temporal_features(features, self._last_json_history, self._last_json_now)

        if features:
            self._last_features = features
            self._last_update = time.monotonic()
            return features

        # ── 5. Cache recente ───────────────────────────────────────────────
        if self._last_features and (time.monotonic() - self._last_update) < 300:
            logger.warning("Using cached features (all DB reads failed)")
            return self._last_features

        logger.warning("No features available from any source")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # PRIMARY SOURCE: gexbot_latest.json
    # ─────────────────────────────────────────────────────────────────────────

    def _read_gexbot_latest_json(
        self, now: datetime
    ) -> tuple[dict[str, float], float]:
        """
        Legge feature GEX dal file gexbot_latest.json scritto da gexbot_scraper.

        Mappings verificati dalla struttura API GexBot:
          classic/gex_zero: spot, zero_gamma, major_pos_oi→call_wall, sum_gex_oi, delta_risk_reversal
          state/delta|gamma|vanna|charm: major_positive/negative/long_gamma/short_gamma
          orderflow: dexoflow, gexoflow, cvroflow, agg_dex, net_dex, zvanna, zcharm, etc.

        Returns:
            (features_dict, data_age_seconds)
        """
        if not self.snapshots_json_path.exists():
            return {}, 9999.0

        data: dict = {}
        try:
            raw_text = self.snapshots_json_path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
        except Exception as e:
            logger.debug(f"JSON parse error: {e}")
            return {}, 9999.0

        features: dict[str, float] = {}
        data_age_sec = 9999.0

        def sf(v) -> float | None:
            """Safe float conversion."""
            if v is None:
                return None
            try:
                f = float(v)
                return f if math.isfinite(f) else None
            except (ValueError, TypeError):
                return None

        def set_f(name: str, value, overwrite: bool = False) -> None:
            if name not in features or overwrite:
                v = sf(value)
                if v is not None:
                    features[name] = v

        # ── Calculate data age ─────────────────────────────────────────────
        # ts_server can be seconds (~1.77e9) or milliseconds (~1.77e12)
        max_ts: float | None = None
        for snap in data.get("snapshots", []):
            ts = snap.get("ts_server")
            if ts and (max_ts is None or ts > max_ts):
                max_ts = float(ts)
        if max_ts:
            now_sec = now.timestamp()
            if max_ts > 1e12:
                # Milliseconds
                data_age_sec = max(0.0, (now_sec * 1000 - max_ts) / 1000.0)
            else:
                # Seconds
                data_age_sec = max(0.0, now_sec - max_ts)

        # ── Extract features from each category snapshot ───────────────────
        for snap in data.get("snapshots", []):
            pkg = snap.get("package", "")
            cat = snap.get("category", "")
            raw = snap.get("raw", {}) or {}

            spot = sf(snap.get("spot")) or sf(raw.get("spot"))

            # EXACT NAME MATCH: any raw key that matches a V3.5 feature name
            for k, v in raw.items():
                if k in self._feature_set and not isinstance(v, (list, dict)):
                    set_f(k, v)

            if spot:
                set_f("spot", spot)

            # ── classic/gex_zero ──────────────────────────────────────────
            # Fields: spot, zero_gamma, major_pos_vol/oi, major_neg_vol/oi,
            #         sum_gex_vol/oi, delta_risk_reversal
            if pkg == "classic" and cat == "gex_zero":
                set_f("zero_gamma",       raw.get("zero_gamma"))
                # Call wall (major positive GEX level) — prefer OI
                cw = sf(raw.get("major_pos_oi")) or sf(raw.get("major_pos_vol"))
                if cw:
                    set_f("call_wall_oi",   cw)
                    set_f("zero_major_call", cw)
                    set_f("call_wall_strike", cw)
                # Put wall (major negative GEX level)
                pw = sf(raw.get("major_neg_oi")) or sf(raw.get("major_neg_vol"))
                if pw:
                    set_f("put_wall_oi",    pw)
                    set_f("zero_major_put",  pw)
                    set_f("put_wall_strike", pw)
                # Net GEX (by OI is more stable)
                ng = sf(raw.get("sum_gex_oi")) or sf(raw.get("sum_gex_vol"))
                if ng:
                    set_f("net_gex_oi",     ng)
                    set_f("net_gamma_zero", ng)
                    set_f("gex_ratio_total", sf(raw.get("sum_gex_vol")))
                # Delta risk reversal
                drr = sf(raw.get("delta_risk_reversal"))
                if drr:
                    set_f("delta_rr", drr)

            # ── classic/gex_one ───────────────────────────────────────────
            elif pkg == "classic" and cat == "gex_one":
                zg1 = sf(snap.get("zero_gamma")) or sf(raw.get("zero_gamma"))
                if zg1: set_f("one_zero_gamma", zg1)
                cw1 = sf(raw.get("major_pos_oi")) or sf(raw.get("major_pos_vol"))
                if cw1:
                    set_f("one_call_wall_oi", cw1)
                    set_f("one_major_call",   cw1)
                pw1 = sf(raw.get("major_neg_oi")) or sf(raw.get("major_neg_vol"))
                if pw1:
                    set_f("one_put_wall_oi",  pw1)
                    set_f("one_major_put",    pw1)
                ng1 = sf(raw.get("sum_gex_oi")) or sf(raw.get("sum_gex_vol"))
                if ng1:
                    set_f("net_gamma_one",  ng1)

            # ── classic/gex_full ──────────────────────────────────────────
            elif pkg == "classic" and cat == "gex_full":
                # Full-range GEX — use as fallback for zero features if not set
                set_f("zero_gamma",  raw.get("zero_gamma"), overwrite=False)
                cw_f = sf(raw.get("major_pos_oi")) or sf(raw.get("major_pos_vol"))
                if cw_f:
                    set_f("call_wall_oi",  cw_f, overwrite=False)
                pw_f = sf(raw.get("major_neg_oi")) or sf(raw.get("major_neg_vol"))
                if pw_f:
                    set_f("put_wall_oi",   pw_f, overwrite=False)
                ng_f = sf(raw.get("sum_gex_oi")) or sf(raw.get("sum_gex_vol"))
                if ng_f:
                    set_f("net_gex_oi",    ng_f, overwrite=False)
                drr_f = sf(raw.get("delta_risk_reversal"))
                if drr_f:
                    set_f("delta_rr",      drr_f, overwrite=False)

            # ── state/delta_zero & state/gamma_zero (same structure) ──────
            elif pkg == "state" and cat in ("delta_zero", "gamma_zero"):
                # These have major_positive/negative/long_gamma/short_gamma
                if cat == "gamma_zero":
                    set_f("zero_major_long_gamma",  raw.get("major_long_gamma"))
                    set_f("zero_major_short_gamma", raw.get("major_short_gamma"))
                    ng = sf(raw.get("major_positive")) - sf(raw.get("major_negative")) if (sf(raw.get("major_positive")) and sf(raw.get("major_negative"))) else None
                    if ng is not None:
                        set_f("net_gamma_zero", ng, overwrite=False)
                elif cat == "delta_zero":
                    # Delta: major_positive/negative are delta-weighted levels
                    mp = sf(raw.get("major_positive"))
                    mn2 = sf(raw.get("major_negative"))
                    ml2 = sf(raw.get("major_long_gamma"))  # delta long level
                    ms2 = sf(raw.get("major_short_gamma")) # delta short level
                    if mp:  set_f("zero_agg_dex", mp, overwrite=False)
                    if mn2: set_f("zero_net_dex",  mn2, overwrite=False)
                    if ml2: set_f("greek_major_long",  ml2, overwrite=False)
                    if ms2: set_f("greek_major_short", ms2, overwrite=False)

            # ── state/delta_one & state/gamma_one ─────────────────────────
            elif pkg == "state" and cat in ("delta_one", "gamma_one"):
                if cat == "gamma_one":
                    set_f("one_major_long_gamma",  raw.get("major_long_gamma"))
                    set_f("one_major_short_gamma", raw.get("major_short_gamma"))
                elif cat == "delta_one":
                    mp1 = sf(raw.get("major_positive"))
                    mn1 = sf(raw.get("major_negative"))
                    if mp1: set_f("one_agg_dex", mp1)
                    if mn1: set_f("one_net_dex",  mn1)

            # ── state/vanna_zero ──────────────────────────────────────────
            elif pkg == "state" and cat == "vanna_zero":
                mp_v = sf(raw.get("major_positive"))
                ml_v = sf(raw.get("major_long_gamma"))
                if mp_v: set_f("vanna_total", mp_v, overwrite=False)
                if ml_v: set_f("vanna_best",  ml_v, overwrite=False)

            # ── state/charm_zero ──────────────────────────────────────────
            elif pkg == "state" and cat == "charm_zero":
                mp_c = sf(raw.get("major_positive"))
                ml_c = sf(raw.get("major_long_gamma"))
                if mp_c: set_f("charm_total", mp_c, overwrite=False)
                if ml_c: set_f("charm_best",  ml_c, overwrite=False)

            # ── orderflow/orderflow ───────────────────────────────────────
            # Fields verified from API: dexoflow, gexoflow, cvroflow,
            #   one_dexoflow, one_gexoflow, one_cvroflow,
            #   agg_dex, one_agg_dex, net_dex, one_net_dex,
            #   net_call_dex, net_put_dex, agg_call_dex, agg_put_dex,
            #   zvanna, zcharm, zcvr, ocvr, zgr, ogr,
            #   z_mlgamma, z_msgamma, o_mlgamma, o_msgamma,
            #   zero_mcall, zero_mput, one_mcall, one_mput
            elif pkg == "orderflow":
                # DEX flows
                set_f("dex_flow",     raw.get("dexoflow"))
                set_f("one_dex_flow", raw.get("one_dexoflow"))
                # GEX flows
                set_f("gex_flow",     raw.get("gexoflow"))
                set_f("one_gex_flow", raw.get("one_gexoflow"))
                # CVR flows
                set_f("cvr_flow",     raw.get("cvroflow"))
                set_f("one_cvr_flow", raw.get("one_cvroflow"))
                # Best flows (raw levels are "best" signals)
                nd = sf(raw.get("net_dex"))
                zg_ = sf(raw.get("zgr"))
                if nd:  set_f("dex_flow_best",  nd)
                if zg_: set_f("gex_flow_best",  zg_)
                set_f("net_dex_total", raw.get("net_dex"))
                # DEX aggregates
                set_f("zero_agg_dex", raw.get("agg_dex"),      overwrite=False)
                set_f("one_agg_dex",  raw.get("one_agg_dex"),  overwrite=False)
                set_f("zero_net_dex", raw.get("net_dex"),       overwrite=False)
                set_f("one_net_dex",  raw.get("one_net_dex"),   overwrite=False)
                # Vanna & charm
                set_f("vanna_total",  raw.get("zvanna"),   overwrite=False)
                set_f("vanna_best",   raw.get("zvanna"),   overwrite=False)
                set_f("charm_total",  raw.get("zcharm"),   overwrite=False)
                set_f("charm_best",   raw.get("zcharm"),   overwrite=False)
                # Call/put pressure = net_call_dex - net_put_dex
                ncd = sf(raw.get("net_call_dex"))
                npd = sf(raw.get("net_put_dex"))
                if ncd is not None and npd is not None:
                    set_f("call_put_pressure", ncd - npd)
                # Greek major levels from orderflow
                set_f("zero_major_long_gamma",  raw.get("z_mlgamma"), overwrite=False)
                set_f("zero_major_short_gamma", raw.get("z_msgamma"), overwrite=False)
                set_f("one_major_long_gamma",   raw.get("o_mlgamma"), overwrite=False)
                set_f("one_major_short_gamma",  raw.get("o_msgamma"), overwrite=False)
                set_f("zero_major_call",  raw.get("zero_mcall"), overwrite=False)
                set_f("zero_major_put",   raw.get("zero_mput"),  overwrite=False)
                set_f("one_major_call",   raw.get("one_mcall"),  overwrite=False)
                set_f("one_major_put",    raw.get("one_mput"),   overwrite=False)
                # Wall OI from orderflow (as fallback)
                set_f("call_wall_oi",     raw.get("zero_mcall"), overwrite=False)
                set_f("put_wall_oi",      raw.get("zero_mput"),  overwrite=False)
                set_f("one_call_wall_oi", raw.get("one_mcall"),  overwrite=False)
                set_f("one_put_wall_oi",  raw.get("one_mput"),   overwrite=False)
                # Greek major net
                zml = sf(raw.get("z_mlgamma"))
                zms = sf(raw.get("z_msgamma"))
                if zml and zms:
                    set_f("greek_major_long",  zml)
                    set_f("greek_major_short", zms)
                    set_f("greek_major_net",   zml - zms)
                # Flow convergence: dex and gex pointing same direction
                df_ = sf(raw.get("dexoflow"))
                gf_ = sf(raw.get("gexoflow"))
                if df_ is not None and gf_ is not None:
                    set_f("flow_convergence",
                          1.0 if (df_ > 0 and gf_ > 0) or (df_ < 0 and gf_ < 0) else 0.0)
                    set_f("flow_divergence",
                          1.0 if (df_ > 0 and gf_ < 0) or (df_ < 0 and gf_ > 0) else 0.0)
                # MM composite proxy
                mm = sf(raw.get("mm_composite"))
                if mm:
                    set_f("mm_composite", mm)
                elif nd is not None and zg_ is not None:
                    # Proxy: normalized combination of DEX and GEX flow
                    mm_proxy = (nd / 10000.0 + gf_ / 100.0) if gf_ is not None else nd / 10000.0
                    set_f("mm_composite", mm_proxy)

        # ── Store history for temporal features (computed AFTER derived features) ──
        # BUG#9 fix: do NOT call _add_temporal_features here — mr1d/mr1u not yet set.
        # Instead save history for _build_v35_features to use after _add_derived_features.
        self._last_json_history = data.get("gex_zero_history", [])
        self._last_json_now = now

        logger.debug(
            f"[json] {len(features)} features from {len(data.get('snapshots', []))} categories, "
            f"age={data_age_sec:.0f}s"
        )
        return features, data_age_sec

    # ─────────────────────────────────────────────────────────────────────────
    # TEMPORAL FEATURES (lags, rolling windows)
    # ─────────────────────────────────────────────────────────────────────────

    def _add_temporal_features(
        self,
        features: dict[str, float],
        history: list[dict],
        now: datetime,
    ) -> None:
        """
        Calcola L1/L2/L3/D1 lags e MA3/STD3/MA6/STD6/MA12/STD12/MOM
        dalle ultime N righe di gex_zero nel JSON history.

        Queste feature sono CRITICHE per il modello V3.5 — senza di loro
        tutti i lag sono 0 e il modello produce previsioni degeneri.
        """
        def sf(v) -> float:
            if v is None: return 0.0
            try:
                f = float(v)
                return f if math.isfinite(f) else 0.0
            except (ValueError, TypeError):
                return 0.0

        # History è ordinata DESC (più recente prima) dal gexbot_scraper
        # La invertiamo per avere ordine cronologico (più vecchio prima)
        hist = list(reversed(history))
        n = len(hist)
        if n < 2:
            return

        # ── Build series arrays ────────────────────────────────────────────
        spots    = np.array([sf(h.get("spot")) or 0.0 for h in hist])
        zg_arr   = np.array([sf(h.get("zero_gamma")) or 0.0 for h in hist])

        # net_gex_oi: from sum_gex_oi in raw, or structured major_pos_oi
        def extract_raw(h, *keys):
            raw = h.get("raw", {}) or {}
            for k in keys:
                v = sf(raw.get(k))
                if v is not None and v != 0.0:
                    return v
            return 0.0

        net_gex_arr = np.array([
            extract_raw(h, "sum_gex_oi", "sum_gex_vol", "net_gex_oi") for h in hist
        ])
        delta_arr = np.array([
            extract_raw(h, "delta_risk_reversal", "delta_rr") for h in hist
        ])
        mm_arr = np.array([
            extract_raw(h, "mm_composite") for h in hist
        ])

        # spot_vs_zero_gamma series
        svzg_arr = np.where(
            zg_arr > 0, (spots - zg_arr) / zg_arr, 0.0
        )

        # move range for spot_in_mr1_pct (use current features as proxy)
        mr1d = features.get("mr1d", 0)
        mr1u = features.get("mr1u", 0)
        mr1_range = mr1u - mr1d
        if mr1_range > 0 and mr1d > 0:
            svmr1_arr = (spots - mr1d) / mr1_range * 100.0
        else:
            # Proxy: use 1% bands around zero gamma
            svmr1_arr = svzg_arr * 100.0

        # call_put_pressure: from raw net_call_dex - net_put_dex, or cw - pw proxy
        def cpp_from_h(h):
            raw = h.get("raw", {}) or {}
            ncd = sf(raw.get("net_call_dex"))
            npd = sf(raw.get("net_put_dex"))
            if ncd is not None and npd is not None:
                return ncd - npd
            # Proxy from call/put wall levels
            cw = sf(raw.get("major_pos_oi")) or sf(raw.get("major_pos_vol")) or 0.0
            pw = sf(raw.get("major_neg_oi")) or sf(raw.get("major_neg_vol")) or 0.0
            return cw - pw if (cw or pw) else 0.0

        cpp_arr = np.array([cpp_from_h(h) for h in hist])

        # greek_major_net: from raw z_mlgamma - z_msgamma or major_long - major_short
        def gmn_from_h(h):
            raw = h.get("raw", {}) or {}
            ml_ = sf(raw.get("z_mlgamma")) or sf(raw.get("major_long_gamma")) or 0.0
            ms_ = sf(raw.get("z_msgamma")) or sf(raw.get("major_short_gamma")) or 0.0
            return ml_ - ms_

        gmn_arr = np.array([gmn_from_h(h) for h in hist])

        # dex_flow_best: net_dex from orderflow (proxy: major_pos_oi from gex_zero history)
        dex_arr = np.array([
            extract_raw(h, "net_dex", "agg_dex") or (sf(h.get("major_pos_oi")) or 0.0)
            for h in hist
        ])
        # gex_flow_best proxy (net_gex_oi)
        gex_arr = net_gex_arr.copy()

        # BUG#13 fix: extract vanna/charm per-snapshot from raw data if available.
        # Without this, STD of vanna/charm lags is always 0 (constant array).
        def vanna_from_h(h):
            raw = h.get("raw", {}) or {}
            v = (sf(raw.get("zvanna")) or sf(raw.get("vanna_total"))
                 or sf(raw.get("z_vanna")))
            return v if v is not None else 0.0

        def charm_from_h(h):
            raw = h.get("raw", {}) or {}
            c = (sf(raw.get("zcharm")) or sf(raw.get("charm_total"))
                 or sf(raw.get("z_charm")))
            return c if c is not None else 0.0

        vanna_hist = np.array([vanna_from_h(h) for h in hist])
        charm_hist = np.array([charm_from_h(h) for h in hist])

        # Fallback to constant array if history is all zeros (API doesn't provide it)
        vanna_cur = features.get("vanna_best", 0.0)
        charm_cur = features.get("charm_best", 0.0)
        vanna_arr = vanna_hist if np.any(vanna_hist != 0) else np.full(n, vanna_cur)
        charm_arr = charm_hist if np.any(charm_hist != 0) else np.full(n, charm_cur)
        # regime_flip: 0 (no regime history)
        flip_arr = np.zeros(n)

        def add_lag_roll(name: str, arr: np.ndarray) -> None:
            """Aggiungi L1/L2/L3/D1 e MA3/STD3/MA6/STD6/MA12/STD12/MOM."""
            cur = arr[-1]
            # Lags
            if n >= 2: features[f"{name}_L1"] = float(arr[-2])
            if n >= 3: features[f"{name}_L2"] = float(arr[-3])
            if n >= 4: features[f"{name}_L3"] = float(arr[-4])
            features[f"{name}_D1"] = float(cur - arr[-2]) if n >= 2 else 0.0
            # Rolling windows
            for w, suf in [(3, "3"), (6, "6"), (12, "12")]:
                window = arr[-w:] if n >= w else arr
                features[f"{name}_MA{suf}"]  = float(np.mean(window))
                features[f"{name}_STD{suf}"] = float(np.std(window)) if len(window) > 1 else 0.0
            # Momentum
            ma12 = features.get(f"{name}_MA12", cur)
            features[f"{name}_MOM"] = float(cur - ma12)

        add_lag_roll("spot",              spots)
        add_lag_roll("net_gex_oi",        net_gex_arr)
        add_lag_roll("delta_rr",          delta_arr)
        add_lag_roll("spot_vs_zero_gamma", svzg_arr)
        add_lag_roll("spot_in_mr1_pct",   svmr1_arr)
        add_lag_roll("dex_flow_best",     dex_arr)
        add_lag_roll("gex_flow_best",     gex_arr)
        add_lag_roll("vanna_best",        vanna_arr)
        add_lag_roll("charm_best",        charm_arr)
        add_lag_roll("net_gamma_zero",    net_gex_arr)
        add_lag_roll("call_put_pressure", cpp_arr)
        add_lag_roll("greek_major_net",   gmn_arr)
        add_lag_roll("mm_composite",      mm_arr)
        add_lag_roll("regime_flip",       flip_arr)

        # Spot acceleration (d²spot/dt²)
        if n >= 3:
            features["spot_accel"] = float(
                (spots[-1] - spots[-2]) - (spots[-2] - spots[-3])
            )

        # Spot z-score (vs last 6 observations)
        if n >= 6:
            w = spots[-6:]
            mu, sigma = float(np.mean(w)), float(np.std(w))
            features["spot_z_score"] = (spots[-1] - mu) / sigma if sigma > 0 else 0.0

        logger.debug(f"[temporal] lags/rolling computed from {n} history points")

    # ─────────────────────────────────────────────────────────────────────────
    # DERIVED & INTERACTION FEATURES
    # ─────────────────────────────────────────────────────────────────────────

    def _add_derived_features(self, features: dict[str, float]) -> None:
        """Feature derivate calcolate dalle raw (spot vs walls, spread, etc.)."""
        spot = features.get("spot", 0)
        zg   = features.get("zero_gamma", 0)
        cw   = features.get("call_wall_oi", 0)
        pw   = features.get("put_wall_oi", 0)

        # ── Regime da net_gex_oi ──────────────────────────────────────────
        if "regime" not in features:
            net_gex = features.get("net_gex_oi", 0)
            if net_gex > 1000:
                features["regime"] = 1.0   # positive gamma regime (market maker hedging)
            elif net_gex < -1000:
                features["regime"] = -1.0  # negative gamma regime (momentum trending)
            else:
                features["regime"] = 0.0

        # ── Move range estimation from GEX + delta_rr ────────────────────
        # mr1d/mr1u = spot ± "1-sigma" daily move implied by options market
        # Proxy: zero_gamma acts as magnetic level; walls are natural boundaries
        if "mr1d" not in features or features["mr1d"] == 0:
            if spot > 0 and cw > 0 and pw > 0:
                # Use call wall / put wall as natural boundaries for MR1
                features["mr1u"] = cw
                features["mr1d"] = pw
            elif spot > 0 and zg > 0:
                # Estimate ±1% intraday range around zero gamma
                features["mr1u"] = zg * 1.01
                features["mr1d"] = zg * 0.99
        if "mr2d" not in features or features["mr2d"] == 0:
            if spot > 0 and cw > 0 and pw > 0:
                # MR2 = extended range beyond MR1
                spread = cw - pw
                features["mr2u"] = cw + spread * 0.5
                features["mr2d"] = pw - spread * 0.5

        mr1d = features.get("mr1d", 0)
        mr1u = features.get("mr1u", 0)
        mr2d = features.get("mr2d", 0)
        mr2u = features.get("mr2u", 0)

        if spot > 0 and zg > 0:
            features["spot_vs_zero_gamma"] = (spot - zg) / zg
            features["spot_above_zg"]      = 1.0 if spot > zg else 0.0
            features["dist_zg_pct"]        = abs(spot - zg) / zg * 100

        if spot > 0 and cw > 0:
            features["spot_vs_call_wall"] = (spot - cw) / cw
            features["dist_call_wall_pct"] = (cw - spot) / spot * 100

        if spot > 0 and pw > 0:
            features["spot_vs_put_wall"]  = (spot - pw) / pw
            features["dist_put_wall_pct"] = (spot - pw) / spot * 100

        if cw > 0 and pw > 0:
            features["wall_spread"] = cw - pw
            spread = cw - pw
            if spread > 0:
                features["wall_skew"] = (cw - spot) / spread if spot > 0 else 0.5

        if mr1d > 0 and mr1u > 0:
            mr1_range = mr1u - mr1d
            if mr1_range > 0 and spot > 0:
                features["spot_in_mr1_pct"] = (spot - mr1d) / mr1_range * 100
                features["mr1_extremity"]   = abs(features["spot_in_mr1_pct"] - 50) / 50
                features["outside_mr1"]     = 1.0 if (spot < mr1d or spot > mr1u) else 0.0
                features["mr1_range_width"] = mr1_range
                features["mr1_break_risk"]  = features["mr1_extremity"]

        if mr2d > 0 and mr2u > 0 and spot > 0:
            mr2_range = mr2u - mr2d
            if mr2_range > 0:
                features["spot_in_mr2_pct"] = (spot - mr2d) / mr2_range * 100
                features["outside_mr2"]     = 1.0 if (spot < mr2d or spot > mr2u) else 0.0

        # Indicatori VIX derivati
        vix = features.get("vix", 0)
        if vix > 0:
            features["vix_normalized"]   = (vix - 18) / 10
            features["vix_break_signal"] = 1.0 if vix > 25 else 0.0
            features["vix_high"]         = 1.0 if vix > 30 else 0.0
            features["vix_low"]          = 1.0 if vix < 15 else 0.0

        # GEX decay
        net_gex = features.get("net_gex_oi", 0)
        mr1_range = features.get("mr1_range_width", 0)
        if mr1_range > 0:
            features["gex_decay_factor"]  = 1.0 / (1.0 + abs(net_gex) / (mr1_range * 1000 + 1))
            features["net_gex_decayed"]   = net_gex * features["gex_decay_factor"]
            features["wall_spread_decayed"] = features.get("wall_spread", 0) * features["gex_decay_factor"]

    def _add_interaction_features(self, features: dict[str, float]) -> None:
        """Feature di interazione di ordine superiore."""
        regime    = features.get("regime", 0)
        mr1_pos   = features.get("spot_in_mr1_pct", 50)
        zg_dist   = features.get("dist_zg_pct", 0)
        vanna     = features.get("vanna_total", 0)
        vix       = features.get("vix", 18)
        charm     = features.get("charm_best", 0)
        decay     = features.get("gex_decay_factor", 1)
        gamma_ext = features.get("mr1_extremity", 0)
        pressure  = features.get("call_put_pressure", 0)
        dex_mom   = features.get("dex_flow_best_MOM", 0)
        net_gamma = features.get("net_gamma_zero", 0)
        conv      = features.get("flow_convergence", 0)
        flip      = features.get("regime_flip", 0)

        features["regime_x_mr1pos"]      = regime * mr1_pos
        features["regime_x_zgdist"]      = regime * zg_dist
        features["vanna_x_vix"]          = vanna * vix
        features["charm_x_decay"]        = charm * decay
        features["gamma_x_extremity"]    = net_gamma * gamma_ext
        features["pressure_x_pos"]       = pressure * mr1_pos
        features["dex_mom_x_gamma"]      = dex_mom * net_gamma
        features["convergence_x_regime"] = conv * regime

        skew = features.get("skew", 0)
        if skew != 0:
            features["skew_high"] = 1.0 if abs(skew) > 1.0 else 0.0

    # ─────────────────────────────────────────────────────────────────────────
    # LEGACY: query da DB gold / DB live
    # ─────────────────────────────────────────────────────────────────────────

    def _query_gex_features(
        self,
        db_executor: Any,
        label: str,
    ) -> tuple[dict[str, float], float]:
        """Interroga gex_summary, greeks_summary, orderflow da un DB legacy."""
        import pandas as pd
        features: dict[str, float] = {}
        data_ts: float | None = None

        # GEX summary
        try:
            gex = db_executor("""
                SELECT zero_gamma, call_wall_oi, put_wall_oi, net_gex_oi,
                       delta_rr, spot, ts_utc
                FROM gex_summary
                WHERE ticker='ES'
                ORDER BY ts_utc DESC LIMIT 1
            """)
            if gex is not None and not gex.empty:
                row = gex.iloc[0]
                if "ts_utc" in row.index and row["ts_utc"] is not None:
                    try:
                        ts_dt = pd.to_datetime(row["ts_utc"], utc=True)
                        import datetime as dt_mod
                        now_utc = dt_mod.datetime.now(dt_mod.timezone.utc)
                        data_ts = (now_utc - ts_dt.to_pydatetime()).total_seconds()
                    except Exception:
                        pass
                for col in gex.columns:
                    if col == "ts_utc":
                        continue
                    val = row.get(col)
                    if val is not None and str(val) not in ("nan", "None", ""):
                        try:
                            features[col] = float(val)
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            logger.debug(f"[{label}] GEX query: {e}")

        # Greeks summary
        try:
            for greek_type in ("delta", "gamma", "vanna", "charm"):
                gr = db_executor(f"""
                    SELECT major_positive, major_negative,
                           major_long_gamma, major_short_gamma
                    FROM greeks_summary
                    WHERE ticker='ES' AND greek_type='{greek_type}'
                    ORDER BY ts_utc DESC LIMIT 1
                """)
                if gr is not None and not gr.empty:
                    row = gr.iloc[0]
                    prefix = f"zero_{greek_type}_" if greek_type != "delta" else ""
                    for col in gr.columns:
                        val = row.get(col)
                        if val is not None and str(val) not in ("nan", "None", ""):
                            try:
                                features[f"{prefix}{col}"] = float(val)
                            except (ValueError, TypeError):
                                pass
        except Exception as e:
            logger.debug(f"[{label}] Greeks query: {e}")

        # Orderflow
        try:
            of = db_executor("""
                SELECT * FROM orderflow
                WHERE ticker='ES'
                ORDER BY ts_utc DESC LIMIT 1
            """)
            if of is not None and not of.empty:
                row = of.iloc[0]
                for col in of.columns:
                    if col in ("ts_utc", "source", "ticker"):
                        continue
                    val = row.get(col)
                    if val is not None and str(val) not in ("nan", "None", ""):
                        try:
                            features[col] = float(val)
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            logger.debug(f"[{label}] Orderflow query: {e}")

        age = data_ts if data_ts is not None else (0.0 if features else 9999.0)
        if features:
            logger.debug(f"[{label}] {len(features)} features, age={age:.0f}s")
        return features, age

    # ─────────────────────────────────────────────────────────────────────────
    # UTILITIES
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _calc_minutes_since_open(now: datetime) -> float:
        """Minuti dall'apertura sessione US (14:30 UTC = 09:30 ET)."""
        now_min = now.hour * 60 + now.minute
        us_open = 14 * 60 + 30
        return max(0.0, float(now_min - us_open))

    def get_status(self) -> dict[str, Any]:
        json_age = 9999.0
        if self.snapshots_json_path.exists():
            try:
                import os
                json_age = time.time() - os.path.getmtime(self.snapshots_json_path)
            except Exception:
                pass
        return {
            "engine": "V3.5_PRODUCTION",
            "features_count": len(self.feature_names),
            "models_loaded": len(self.engine.models),
            "gates_enabled": self.engine.enable_gates,
            "emergency_halt": self.engine.emergency_halt,
            "last_features_count": len(self._last_features),
            "data_source": "gexbot_latest.json",
            "json_file_age_sec": round(json_age, 0),
            "live_data_age_sec": round(self._live_data_age_sec, 0),
        }

    def is_operational(self) -> bool:
        return self.engine._loaded and len(self.engine.models) == 3
