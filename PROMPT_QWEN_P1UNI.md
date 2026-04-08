# PROMPT PER QWEN: COSTRUZIONE REPOSITORY P1UNI

**Data**: 2026-04-08
**Da**: Claude (assistente di Annalisa)
**Per**: Qwen
**Obiettivo**: Costruire il repository GitHub `p1uni` completo, modulare, production-ready

---

## CONTESTO OPERATIVO

Il database `ml_gold.duckdb` (159 GB, 16 tabelle, 1.94B righe) e' certificato SAFE FOR LIVE TRADING.
Il sistema fa autotrading di futures E-mini S&P 500 (ES) usando modelli ML (XGBoost ensemble di 3 modelli) per generare segnali, con NinjaTrader 8 che esegue gli ordini.

**Hardware**: Windows 11, 128GB RAM, RTX 4080 Super, DuckDB configurato con 32 thread / 110GB RAM.

**Architettura target**:
```
GEXBOT WS (GEX levels)  --|
P1-LITE (ranges/spot)   --|--> INGESTOR --> DuckDB --> FEATURE BUILDER --> ML ENSEMBLE --> SIGNAL ENGINE --> NT8 BRIDGE --> NINJATRADER 8
DATABENTO (trades live)  --|
```

---

## FASE 1: STRUTTURA CARTELLE

Crea esattamente questa tree structure:

```
p1uni/
├── .github/workflows/          # CI/CD per test automatici
├── config/
│   ├── settings.yaml           # Configurazione principale (esempio con placeholder)
│   └── settings.example.yaml   # Copia senza secrets
├── data/                       # .gitignore (qui va il DB e i dati raw)
│   ├── quarantine/             # Log dati scartati
│   └── cache/                  # Cache temporanea (gex_cache.json etc)
├── src/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── database.py         # Connessione DuckDB ottimizzata (singleton thread-safe)
│   │   ├── normalizer.py       # Universal Normalizer (ticker/colonne/timestamp)
│   │   └── validator.py        # Gatekeeper: validazione pre-write
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── adapter_ws.py       # WebSocket GexBot (GEX levels live)
│   │   ├── adapter_p1.py       # P1-Lite Scraper (ranges/spot 1Hz)
│   │   ├── adapter_databento.py # Databento trades live
│   │   └── backfiller.py       # Scarica storico se mancano dati
│   ├── ml/
│   │   ├── __init__.py
│   │   ├── feature_builder.py  # Costruisce vettore 17 feature in real-time
│   │   ├── model_ensemble.py   # Gestisce i 3 XGBoost, media ponderata + confidenza
│   │   └── trainer.py          # Script ri-addestramento periodico
│   ├── execution/
│   │   ├── __init__.py
│   │   ├── signal_engine.py    # Logica 5 fasi temporali (Notte/Mattina/Pom/Stop)
│   │   ├── level_validator.py  # Verifica livelli MR/OR prima di trade
│   │   ├── ninja_bridge.py     # TCP bridge verso NinjaTrader 8 (porta 5555)
│   │   └── risk_manager.py     # Stop loss, max daily loss, position sizing
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           # Logging centralizzato (file + console + Telegram)
│       ├── telegram_bot.py     # Invio alert via Telegram
│       └── system_monitor.py   # Controllo salute CPU/RAM/Connessioni
├── tests/
│   ├── __init__.py
│   ├── test_normalizer.py
│   ├── test_validator.py
│   ├── test_feature_builder.py
│   └── test_full_system.py     # Integration test end-to-end
├── notebooks/                  # Analisi esplorative (opzionale)
├── requirements.txt
├── Dockerfile
├── README.md
├── .gitignore
└── main.py                     # Orchestratore unico
```

---

## FASE 2: SPECIFICHE TECNICHE PER OGNI MODULO

### 2.1 `src/core/database.py` — DuckDB Singleton Thread-Safe

```python
# SPECIFICHE:
# - Pattern Singleton: una sola istanza per processo
# - DUE modalita di connessione:
#   1. WRITER (read-write, WAL mode) — usato SOLO dall'Ingestion thread
#   2. READER (read-only) — usato da ML/Execution, riconnette ogni ciclo per dati freschi
# - Thread lock per proteggere il writer
# - Path DB da config/settings.yaml
# - DuckDB config: threads=32, memory_limit='110GB'
# - IMPORTANTE: DuckDB NON supporta writer concorrenti. Un solo processo puo scrivere.
#   La pipeline ML DEVE aprire in read_only=True.

# Metodi richiesti:
# get_writer() -> duckdb.DuckDBPyConnection  (singleton, thread-locked)
# get_reader() -> duckdb.DuckDBPyConnection  (nuova connessione read-only ogni volta)
# execute_write(query, params) -> None
# execute_read(query, params) -> pd.DataFrame
# close() -> None
```

### 2.2 `src/core/normalizer.py` — Universal Normalizer

Implementa ESATTAMENTE queste mappe di normalizzazione (dal DATA_CONTRACT):

**Ticker Aliases**:
```python
TICKER_MAP = {
    'ES': 'ES', 'ES_SPX': 'ES', 'ES.c.0': 'ES', 'ESM6': 'ES', 'ESZ5': 'ES',
    'NQ': 'NQ', 'NQ_NDX': 'NQ', 'NQ.c.0': 'NQ', 'NQM6': 'NQ',
    'SPX': 'SPY', 'SPY': 'SPY', 'SPDR': 'SPY',
    'NDX': 'QQQ', 'QQQ': 'QQQ', 'TQQQ': 'QQQ',
    'VIX': 'VIX', '^VIX': 'VIX', 'VX': 'VIX',
    'IWM': 'IWM', 'RUT': 'IWM',
    'TLT': 'TLT',
    'GLD': 'GLD', 'XAUUSD': 'GLD',
    'UVXY': 'UVXY', 'VXX': 'UVXY',
}
VALID_TICKERS = {'ES', 'NQ', 'SPY', 'QQQ', 'VIX', 'IWM', 'TLT', 'GLD', 'UVXY'}
```

**Column Aliases GEX**:
```python
GEX_COLUMN_MAP = {
    'call_gex': 'call_wall_vol',
    'call_gamma_oi': 'call_wall_oi',
    'put_gex': 'put_wall_vol',
    'put_gamma_oi': 'put_wall_oi',
    'gamma_flip': 'zero_gamma',
    'gamma_level': 'zero_gamma',
    'net_gamma': 'net_gex_oi',
    'net_gex': 'net_gex_oi',
    'total_gex': 'net_gex_vol',
    'timestamp': 'ts_utc', 'time': 'ts_utc', 'datetime': 'ts_utc', 'ts': 'ts_utc',
    'price': 'spot', 'last_price': 'spot', 'underlying': 'spot',
    'symbol': 'ticker', 'asset': 'ticker', 'instrument': 'ticker',
    'num_strikes': 'n_strikes', 'strike_count': 'n_strikes',
}
```

**Timestamp Rules**:
| Fonte | Formato Input | Trasformazione |
|-------|---------------|----------------|
| GexBot Storico | UTC ISO8601 | Nessuna |
| WebSocket Live | UTC ISO8601 ms | Nessuna |
| P1-Lite | Locale (Zurigo) | Converti a UTC (-1h CET / -2h CEST) |
| Databento | Nanosec UNIX epoch | Dividi per 1e9, converti a TIMESTAMP UTC |

**REGOLA FERREA**: Tutti i timestamp nel DB DEVONO essere UTC. Nessuna eccezione.

### 2.3 `src/core/validator.py` — Gatekeeper Pre-Write

Ogni record DEVE superare TUTTI questi controlli prima dell'inserimento:

```python
# 1. ticker IN VALID_TICKERS
# 2. spot > 0 AND spot < 100000
# 3. ts_utc non nel futuro (tolleranza: +5 minuti)
# 4. ts_utc > 2023-01-01
# 5. Non weekend (Sab >01:00 UTC o Dom <22:00 UTC)
# 6. Non duplicato (ticker + ts_utc + aggregation + hub)  -- per gex_summary/greeks
# 7. Nessun campo obbligatorio NULL

# Se un record FALLISCE -> scrivi in tabella data_quarantine:
# CREATE TABLE data_quarantine (
#     raw_data JSON,
#     failure_reason VARCHAR,
#     target_table VARCHAR,
#     source VARCHAR,
#     ticker VARCHAR,
#     ts_record TIMESTAMP,
#     ts_quarantined TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     resolved BOOLEAN DEFAULT FALSE
# );
```

### 2.4 `src/ingestion/adapter_databento.py` — Feed Databento Live

```python
# SPECIFICHE:
# - Usa libreria: databento>=0.20.0
# - API: db.Live(key=api_key)  (NON db.set_api_key() che non esiste!)
# - Schema: "trades" (NON "mbo" per storico, ma MBO e' disponibile in live)
# - Dataset: "GLBX.MDP3"
# - Simbolo: da config, default "ES.c.0" (continuous front-month)
# - Buffer: deque(maxlen=200_000)
# - Flush in DuckDB ogni 300 secondi (mbo_flush_interval_sec da config)
# - Tabella target: trades_live
# - Schema tabella:
#   CREATE TABLE trades_live (
#       ts_event TIMESTAMP,   -- da nanosec /1e9
#       price DOUBLE,         -- da scaled /1e9 se necessario
#       size INTEGER,
#       side VARCHAR,         -- 'B' (buyer) o 'A' (seller)
#       flags INTEGER,
#       ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#   );
#   CREATE INDEX idx_trades_ts ON trades_live(ts_event DESC);
#
# - Retry logic: se connessione persa, retry con backoff esponenziale (1s, 2s, 4s, max 60s)
# - Log ogni flush: "Flushed {N} trades to DuckDB"
```

### 2.5 `src/ingestion/adapter_ws.py` — WebSocket GexBot

```python
# SPECIFICHE:
# - Si connette al WebSocket GexBot per ricevere livelli GEX in tempo reale
# - Dati: gex_summary e greeks_summary
# - Normalizza tramite normalizer.py PRIMA di scrivere
# - Valida tramite validator.py
# - Scrive anche in cache/gex_cache.json (aggiornamento atomico) per la pipeline ML
# - Tabelle target: gex_summary, greeks_summary
# - Source tag: 'WS'
# - gex_summary schema: ts_utc, source, ticker, hub, aggregation, spot, zero_gamma,
#   call_wall_vol, call_wall_oi, put_wall_vol, put_wall_oi, net_gex_vol, net_gex_oi,
#   delta_rr, n_strikes, min_dte, sec_min_dte
# - greeks_summary schema: ts_utc, source, ticker, hub, greek_type, dte_type, spot,
#   major_positive, major_negative, major_long_gamma, major_short_gamma, n_contracts,
#   min_dte, sec_min_dte
```

### 2.6 `src/ingestion/adapter_p1.py` — P1-Lite Scraper

```python
# SPECIFICHE:
# - Frequenza: 1Hz (un tick al secondo)
# - ATTENZIONE TIMESTAMP: P1-Lite usa timezone Zurigo (CET/CEST)
#   DEVI convertire a UTC: -1h in inverno (CET), -2h in estate (CEST)
#   Usa: datetime.astimezone(timezone.utc) o pytz
# - Tabella target: intraday_ranges_stream
# - Schema: ts_utc, session_date, ticker('ES'), source('P1_LITE'),
#   spot, running_high, running_low, running_vwap, volume_cumulative,
#   mr1d, mr1u, or1d, or1u
# - Aggregazione fine giornata: consolida in settlement_ranges
#   (max_10_22, min_10_22, close_22, is_final=TRUE)
# - Source tag: 'P1_LITE'
```

### 2.7 `src/ml/feature_builder.py` — 17 Feature in Real-Time

```python
# Legge da DuckDB (read-only!) gli ultimi 60 minuti di trades e calcola:
#
# | Feature | Calcolo |
# |---------|---------|
# | return_1min | media pct_change ultimi 60 trade |
# | return_5min | media pct_change ultimi 300 trade |
# | vol_1min | std pct_change ultimi 60 trade |
# | vol_5min | std pct_change ultimi 300 trade |
# | ofi_buy | sum(size) dove side='B' |
# | ofi_sell | sum(size) dove side='A' |
# | ofi_net | ofi_buy - ofi_sell |
# | ofi_ratio | ofi_net / (ofi_buy + ofi_sell + 1) |
# | trade_buy_volume | = ofi_buy |
# | trade_sell_volume | = ofi_sell |
# | trade_imbalance | = ofi_ratio |
# | add_buy_count | count trade side='B' |
# | add_sell_count | count trade side='A' |
# | cancel_buy_count | 0 (placeholder, non disponibile da trades) |
# | cancel_sell_count | 0 (placeholder) |
# | gex_proximity | 1/(1+min_dist_to_gex_level) — legge da gex_cache.json |
# | low_vol_regime | 1 se vol_5min < 0.0003, else 0 |
#
# Ritorna: dict con le 17 feature, pronto per il model_ensemble
# Intervallo: ogni 5 minuti (bar_interval_min da config) o ogni tick (configurabile)
```

### 2.8 `src/ml/model_ensemble.py` — Ensemble 3 XGBoost

```python
# SPECIFICHE:
# - Carica 3 modelli XGBClassifier da models_dir (config):
#   1. bounce_prob.joblib     — Probabilita rimbalzo prezzo (peso 50%)
#   2. regime_persistence.joblib — Stabilita regime mercato (peso 30%)
#   3. volatility_30min.joblib — Probabilita alta volatilita (peso 20%, INVERTITO)
# - Carica scaler.joblib (StandardScaler, attualmente passthrough/identita)
#
# Metodo predict(features: dict) -> dict:
#   1. Scala le feature con scaler
#   2. predict_proba() su ognuno dei 3 modelli
#   3. Calcola:
#      overall_conf = bounce * 0.50 + regime * 0.30 + (1 - vol) * 0.20
#   4. Direzione: "LONG" se bounce_prob > 0.55, altrimenti "SHORT"
#   5. Ritorna: {
#        "direction": "LONG" | "SHORT",
#        "confidence": float,  # overall_conf
#        "std_dev": float,     # deviazione standard tra i 3 modelli
#        "bounce_prob": float,
#        "regime_prob": float,
#        "vol_prob": float,
#        "timestamp": datetime.utcnow()
#      }
#
# Soglia minima: min_confidence = 0.60 (da config)
# Se confidence < 0.60 -> NON inviare segnale
```

### 2.9 `src/execution/signal_engine.py` — Cervello Temporale (5 Fasi)

```python
# SPECIFICHE — 5 FASI DI TRADING (orari UTC):
#
# 1. NOTTE EARLY (22:00 - 01:00 UTC)
#    - Solo osservazione, NO trading
#    - Registra livelli di apertura sessione
#    - Aggiorna settlement/range del giorno precedente
#
# 2. NOTTE FULL (01:00 - 09:30 UTC)
#    - Trading consentito ma CONSERVATIVO
#    - min_confidence alzata a 0.70 (vs 0.60 standard)
#    - Max 2 trade per fase
#    - Monitora range notturno (overnight high/low)
#
# 3. MATTINA (09:30 - 12:00 UTC = apertura US)
#    - Trading PIENO
#    - min_confidence standard 0.60
#    - DEVE chiamare level_validator prima di ogni segnale
#    - Verifica MR/OR levels (mid-range/opening-range)
#
# 4. POMERIGGIO (12:00 - 20:00 UTC)
#    - Trading PIENO
#    - Attenzione a 14:00-14:30 UTC (FOMC, dati macro)
#    - Power hour 19:00-20:00: volatilita attesa alta
#
# 5. STOP (20:00 - 22:00 UTC)
#    - NESSUN nuovo trade
#    - Chiudi posizioni aperte entro 21:50 UTC
#    - Calcola P&L giornaliero
#    - Reset contatori per il giorno dopo (reset_hour_utc=22)
#
# Il signal_engine:
# 1. Riceve prediction dal model_ensemble
# 2. Verifica la fase temporale corrente
# 3. Chiama level_validator.check(spot, direction, levels)
# 4. Chiama risk_manager.can_trade()
# 5. Se TUTTO OK -> invia al ninja_bridge
```

### 2.10 `src/execution/level_validator.py` — Verifica Livelli MR/OR

```python
# Legge da settlement_ranges (524+ righe, solo ES):
# - mr1d, mr1u, mr2d, mr2u (mid-ranges)
# - or1d, or1u, or2d, or2u (opening-ranges)
# - vwap, settlement
#
# Regole:
# - LONG consentito solo se spot >= mr1d (supporto mid-range 1 down)
# - SHORT consentito solo se spot <= mr1u (resistenza mid-range 1 up)
# - Se spot dentro OR range (or1d < spot < or1u): cautela, ridurre size
# - Se spot fuori MR2: segnale forte di breakout, autorizzare con alta confidenza
```

### 2.11 `src/execution/ninja_bridge.py` — Bridge TCP verso NinjaTrader 8

```python
# SPECIFICHE:
# - Server TCP su bridge_host:bridge_port (default 127.0.0.1:5555)
# - Riceve segnali dalla signal_engine come JSON
# - Format messaggio:
#   {"action": "LONG"|"SHORT"|"CLOSE", "confidence": 0.72, "timestamp": "..."}
# - Invia al client NT8 connesso (MLAutoStrategy_P1.cs)
# - Aspetta ACK dal client entro 5 secondi
# - Se nessun ACK -> logga WARNING e invia alert Telegram
# - Supporta riconnessione: se NT8 si disconnette, attende riconnessione
# - Heartbeat ogni 30 secondi per verificare che NT8 sia vivo
```

### 2.12 `src/execution/risk_manager.py` — Gestione Rischio

```python
# Doppio livello di risk (Python + C# in NT8 come backup):
#
# Parametri da config:
# - max_daily_loss_pts: 40  (punti ES, 1pt = $50)
# - max_consec_losses: 5
# - min_confidence: 0.60
# - reset_hour_utc: 22
#
# Metodo can_trade() -> bool:
# 1. daily_pnl > -max_daily_loss_pts ?
# 2. consecutive_losses < max_consec_losses ?
# 3. Non siamo in fase STOP ?
# 4. Nessun altro trade aperto ?
#
# Metodo update_trade_result(pnl_pts: float):
# - Aggiorna daily_pnl
# - Aggiorna consecutive_losses (reset a 0 se positivo)
# - Se daily limit raggiunto -> STOP trading per il giorno
#
# Reset giornaliero: alle 22:00 UTC azzera tutto
```

### 2.13 `src/utils/logger.py` — Logging Centralizzato

```python
# - Un logger per modulo (e.g., "p1uni.ingestion.databento")
# - Output: console (INFO+) e file rotante (DEBUG+) in logs/
# - Formato: "[2026-04-08 14:30:00 UTC] [INFO] [ingestion.databento] Flushed 1500 trades"
# - File rotante: 10MB max, 5 file backup
# - Livello configurabile da settings.yaml
```

### 2.14 `src/utils/telegram_bot.py` — Alert Telegram

```python
# - Invia alert via Telegram bot
# - Token e chat_id da config
# - Metodi:
#   send_signal(direction, confidence) — quando si apre un trade
#   send_alert(message, level) — errori, warning, info
#   send_daily_summary(pnl, trades) — fine giornata
# - Rate limit: max 1 messaggio ogni 3 secondi
```

### 2.15 `main.py` — Orchestratore

```python
# Avvia 4 thread principali:
#
# Thread 1: INGESTION LOOP
#   - adapter_databento: riceve trades live, flush ogni 5 min
#   - adapter_ws: riceve GEX levels, aggiorna cache + DB
#   - adapter_p1: riceve ranges 1Hz, scrive in DB
#
# Thread 2: FEATURE BUILDER + ML INFERENCE
#   - Ogni 5 minuti (bar_interval_min):
#     1. feature_builder.build() -> 17 feature
#     2. model_ensemble.predict(features) -> segnale
#     3. Se confidence >= threshold -> passa a signal_engine
#
# Thread 3: SIGNAL ENGINE + EXECUTION
#   - signal_engine.evaluate(prediction)
#   - Se autorizzato -> ninja_bridge.send(signal)
#
# Thread 4: SYSTEM MONITOR + HEARTBEAT
#   - Ogni 60 secondi: check CPU, RAM, connessioni DB
#   - Ogni 5 minuti: heartbeat Telegram "System alive"
#   - Se anomalia: alert immediato
#
# CLI args:
#   python main.py --mode live     # produzione
#   python main.py --mode paper    # simulazione (default)
#   python main.py --mode backtest # replay storico
#
# Graceful shutdown: cattura SIGINT/SIGTERM, chiude tutto ordinatamente
```

---

## FASE 3: CONFIGURAZIONE

### `config/settings.yaml` (esempio):

```yaml
system:
  mode: "paper"  # "paper", "live", "backtest"
  log_level: "INFO"

database:
  path: "C:\\Users\\annal\\Desktop\\ML DATABASE\\ml_gold.duckdb"
  threads: 32
  memory_limit: "110GB"

databento:
  api_key: "${DATABENTO_API_KEY}"  # da env var
  symbol: "ES.c.0"
  dataset: "GLBX.MDP3"
  schema: "trades"
  flush_interval_sec: 300
  buffer_size: 200000

models:
  dir: "ml_models/"
  bounce_weight: 0.50
  regime_weight: 0.30
  volatility_weight: 0.20
  min_confidence: 0.60

execution:
  bridge_host: "127.0.0.1"
  bridge_port: 5555
  bar_interval_min: 5
  feature_window_min: 60

risk:
  max_daily_loss_pts: 40
  max_consec_losses: 5
  reset_hour_utc: 22

sessions:  # orari UTC
  night_early: { start: "22:00", end: "01:00" }
  night_full:  { start: "01:00", end: "09:30", min_confidence: 0.70, max_trades: 2 }
  morning:     { start: "09:30", end: "12:00" }
  afternoon:   { start: "12:00", end: "20:00" }
  stop:        { start: "20:00", end: "22:00" }

telegram:
  token: "${TELEGRAM_BOT_TOKEN}"
  chat_id: "${TELEGRAM_CHAT_ID}"
  heartbeat_interval_min: 5

tickers:
  valid: ["ES", "NQ", "SPY", "QQQ", "VIX", "IWM", "TLT", "GLD", "UVXY"]
  primary: "ES"
```

---

## FASE 4: REQUIREMENTS.TXT

```
databento>=0.20.0
pandas>=2.2.0
duckdb>=1.0.0
xgboost>=2.0.0
scikit-learn>=1.4.0
joblib>=1.3.0
numpy>=1.26.0
pyyaml>=6.0
pytz>=2024.1
psutil>=5.9.0
requests>=2.31.0
python-telegram-bot>=20.0
streamlit>=1.30.0
plotly>=5.18.0
pytest>=8.0.0
```

---

## FASE 5: TEST — `tests/test_full_system.py`

Crea un integration test end-to-end che:

1. **Mock dati in ingresso**: simula un tick da WS (GEX level), P1 (range), Databento (trade)
2. **Verifica normalizzazione**: il normalizer mappa correttamente ticker aliases e colonne
3. **Verifica validazione**: il validator rifiuta record con spot=0 o timestamp futuro
4. **Verifica feature builder**: dato N trades mockati, calcola correttamente le 17 feature
5. **Verifica model ensemble**: con feature mockate, il predict() ritorna confidence e direction
6. **Verifica signal engine**: rispetta la fase temporale corrente, non tradare in STOP
7. **Verifica risk manager**: blocca se daily loss >= 40pt
8. **Verifica ninja bridge**: il segnale arriva al mock TCP server solo se TUTTO ok

---

## FASE 6: .GITIGNORE

```
# Database
*.duckdb
*.duckdb.wal
*.duckdb.tmp/
duckdb_tmp/

# Data
data/
ml_models/
*.joblib
*.csv

# Secrets
config/settings.yaml
live_config.json
.env

# Python
__pycache__/
*.pyc
*.pyo
.pytest_cache/
*.egg-info/

# IDE
.vscode/
.idea/

# OS
Thumbs.db
.DS_Store

# Logs
logs/
*.log
```

---

## FASE 7: README.md — Guida Zero-to-Hero

Scrivi un README con queste sezioni:
1. **Cos'e' P1UNI**: Sistema di autotrading ES futures con ML
2. **Architettura**: diagramma ASCII dei 4 thread
3. **Requisiti**: Python 3.10+, NinjaTrader 8, account Databento Standard
4. **Installazione**:
   - `git clone`, `pip install -r requirements.txt`
   - Copiare `settings.example.yaml` -> `settings.yaml`, inserire API key
   - Copiare `ml_gold.duckdb` in `data/`
   - Copiare modelli `.joblib` in `ml_models/`
5. **Avvio**: `python main.py --mode paper`
6. **NinjaTrader**: copiare `MLAutoStrategy_P1.cs`, compilare, applicare al chart ES
7. **Monitoraggio**: dashboard Streamlit opzionale, alert Telegram
8. **Struttura cartelle**: tree con descrizioni
9. **Contribuire**: coding standards, type hints, pytest

---

## FASE 8: PULIZIA FILE VECCHI

Una volta che p1uni e' completo e testato, questi file nella cartella `ML DATABASE` possono essere ARCHIVIATI (non cancellati subito, spostarli in `_ARCHIVIO_LEGACY/`):

### DA ARCHIVIARE (sostituiti da p1uni):
```
ML DATABASE/
├── databento_live_ingestor.py      -> sostituito da src/ingestion/adapter_databento.py
├── main_live_pipeline.py           -> sostituito da main.py + src/ml/
├── nt8_bridge_daemon.py            -> sostituito da src/execution/ninja_bridge.py
├── universal_normalizer.py         -> sostituito da src/core/normalizer.py
├── unified_ingestor_safe.py        -> sostituito da src/ingestion/
├── unified_ingestor_v2.py          -> sostituito da src/ingestion/
├── NightlyDBIngestor.py            -> sostituito da src/ingestion/backfiller.py
├── monitor_dashboard.py            -> sostituito da src/utils/system_monitor.py
├── MasterWatchdog.py               -> sostituito da main.py Thread 4
├── telegram_notifier.py            -> sostituito da src/utils/telegram_bot.py
├── auto_rollover_futures.py        -> integrato in config (es_symbol/es_next_symbol)
├── ninja_autologin.py              -> non necessario in p1uni
├── ninja_login_diretto.py          -> non necessario
├── nt8_auto_login.py               -> non necessario
├── nt8_auto_login.vbs              -> non necessario
├── NT8AutoLogin.ps1                -> non necessario
├── START_TRADING_SYSTEM.bat        -> sostituito da: python main.py --mode live
├── MasterLauncher.bat              -> sostituito da: python main.py
├── DEPLOY_NOW.ps1                  -> non necessario
├── START_NOW.ps1                   -> non necessario
├── KILL_AND_INSPECT.ps1            -> non necessario
├── Setup_TaskScheduler.ps1         -> non necessario
├── EmergencyStop.bat               -> sostituito da Ctrl+C su main.py (graceful shutdown)
├── RUN_THIS_NOW.py                 -> non necessario
├── run_all_checks.py               -> sostituito da pytest tests/
```

### DA TENERE (non sostituiti):
```
├── ml_gold.duckdb                  -> IL DATABASE (spostare in p1uni/data/)
├── ml_models/                      -> I MODELLI (spostare in p1uni/ml_models/)
├── MLAutoStrategy_P1.cs            -> STRATEGIA NT8 (copiare in p1uni/docs/)
├── MLSignalIndicator.cs            -> INDICATORE NT8 (copiare in p1uni/docs/)
├── training/                       -> RISULTATI TRAINING (tenere come riferimento)
├── backups/                        -> BACKUP DB (tenere finche non confermato p1uni OK)
├── DATA_CONTRACT.md                -> copiare in p1uni/docs/
├── HANDOVER_QWEN.md                -> copiare in p1uni/docs/
├── live_config.json                -> migrato in p1uni/config/settings.yaml
```

### DA CANCELLARE (inutili):
```
├── duckdb_tmp/                     # 23 GB di temp files
├── ml_gold.duckdb.tmp/             # 717 MB temp
├── ml_gold.duckdb.wal.bak.bak2    # 0 bytes
├── ml_gold.duckdb.wal.bak2         # 0 bytes
├── __pycache__/                    # bytecode
├── heuristic-dewdney/              # cartella vuota
├── ml_gold.duckdb?mode=ro          # file fantasma (12KB)
├── trading_data.db                 # SQLite legacy (12KB)
```

---

## ISTRUZIONI OPERATIVE PER QWEN

1. **Lavora per moduli**. Inizia da `core/` (database, normalizer, validator), poi `ingestion/`, poi `ml/`, poi `execution/`, infine `main.py`.
2. **Type hinting rigoroso** (Python 3.10+). Ogni funzione con docstring.
3. **Errori difensivi**: nessun crash deve fermare l'intero sistema. Logga l'errore e continua. Usa try/except in ogni loop, non far propagare eccezioni al thread principale.
4. **Thread safety**: usa `threading.Lock` dove serve, specialmente nel database writer.
5. **Testa ogni modulo** prima di passare al successivo.
6. **Il DB e' su Windows**: i path usano backslash. Usa `pathlib.Path` ovunque.
7. **NON includere API key reali** nei file. Usa env vars o placeholder.

---

## DOMANDE CHE QWEN POTREBBE FARE

**Q: Dove trovo il WebSocket endpoint di GexBot?**
A: L'endpoint esatto lo fornira' Annalisa. Per ora crea l'adapter con un placeholder URL configurabile in settings.yaml.

**Q: I modelli .joblib sono gia' addestrati?**
A: Si, ci sono 3 modelli in `ml_models/`. Sono XGBClassifier addestrati su feature MBO. Potrebbero aver bisogno di ri-addestramento su feature trades, ma per ora usali cosi.

**Q: Come gestisco il rollover ES trimestrale?**
A: Il simbolo `ES.c.0` e' il continuous front-month di Databento e gestisce il rollover automaticamente. Il campo `es_next_symbol` in config serve solo per logging.

**Q: Databento supporta MBO in live?**
A: SI! Il piano Standard include MBO in live streaming. Solo lo storico MBO e' limitato a 1 mese. Per ora usiamo schema `trades` per semplicita', ma l'adapter puo essere esteso a MBO live in futuro.

---

**INIZIA ORA. Crea la struttura cartelle, poi scrivi modulo per modulo partendo da core/database.py.**
