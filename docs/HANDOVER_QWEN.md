# HANDOVER TECNICO COMPLETO — Sistema Autotrading ES Futures

Data: 2026-04-06
Autore: Claude (assistente di Annalisa)
Destinatario: Qwen (AI assistant)

---

## 1. PANORAMICA DEL SISTEMA

Il sistema e' un autotrading di futures E-mini S&P 500 (ES) che usa modelli ML (XGBoost) per generare segnali di ingresso e un bridge TCP per comunicarli a NinjaTrader 8 che esegue gli ordini.

Architettura a 4 componenti:

```
DATABENTO LIVE (trades)  -->  INGESTOR (Python)  -->  DuckDB
                                                         |
                                                    PIPELINE ML (Python, read-only)
                                                         |
                                                    TCP BRIDGE (Python, porta 5555)
                                                         |
                                                    NINJATRADER 8 (C#, strategia MLAutoStrategy_P1)
```

## 2. I TRE PROGETTI

### 2.1 ML DATABASE (progetto principale)
Percorso: `C:\Users\annal\Desktop\ML DATABASE`
Repo git: master branch

File operativi (quelli che contano):
- `databento_live_ingestor.py` — riceve trade in tempo reale da Databento, scrive in DuckDB
- `main_live_pipeline.py` — legge DuckDB (read-only), calcola feature, inferenza ML, invia segnale al bridge
- `nt8_bridge_daemon.py` — server TCP che riceve segnali dalla pipeline e li inoltra a NT8
- `MLAutoStrategy_P1.cs` — strategia NinjaTrader che riceve segnali via TCP e esegue ordini
- `live_config.json` — configurazione centralizzata (API key, simbolo, porte, soglie rischio)
- `monitor_dashboard.py` — dashboard Streamlit per monitorare il sistema (opzionale)
- `auto_rollover_futures.py` — gestione rollover contratto ES trimestrale
- `ml_models/` — cartella con i 4 modelli ML

File di test:
- `test_mbo_stream_validation.py` — validatore stream Databento (ora usa schema trades)
- `test_live_connection.py` — test pre-live di tutti i componenti

Cartelle archivio (NON toccare):
- `_ARCHIVIO_LEGACY/`, `_ARCHIVIO_PRE_P1Q/`, `_ARCHIVIO_PUNTO0/` — vecchie versioni
- `training/` — risultati di training e backtest

### 2.2 WebSocket (progetto GEXBot)
Percorso: da determinare (Annalisa deve indicarlo)
Stato: esiste un progetto separato che si connette al WebSocket di GEXBot per ricevere dati Gamma Exposure (GEX) in tempo reale.
NON ancora integrato nel sistema principale.

Obiettivo futuro: creare `gex_ingestor.py` che:
1. Si connette al WebSocket GEXBot
2. Riceve livelli GEX: Hedge Wall, Put Wall, Zero Gamma, Call/Put OI
3. Scrive in `gex_cache.json` (aggiornamento atomico)
4. La pipeline ML legge gex_cache.json come feature aggiuntiva

### 2.3 P1 (progetto NinjaTrader)
Percorso: da determinare
Contiene la strategia NinjaTrader originale. Il file chiave e' gia' copiato in ML DATABASE come `MLAutoStrategy_P1.cs`.

---

## 3. STATO ATTUALE E PROBLEMI RISOLTI

### Problemi risolti oggi (2026-04-06):

1. **Schema MBO non autorizzato** — Il piano Databento Standard NON include lo schema `mbo` (order-by-order). Tutto il codice e' stato convertito a schema `trades`:
   - `databento_live_ingestor.py`: schema="trades", tabella `trades_live`
   - `main_live_pipeline.py`: query su `trades_live`, feature da trade (non order book)
   - `test_mbo_stream_validation.py`: schema="trades"
   - `test_live_connection.py`: tabella `trades_live`
   - `monitor_dashboard.py`: tabella `trades_live`

2. **DuckDB lock concorrente** — L'ingestor e la pipeline aprivano entrambi il DB in read-write, causando lock (PID conflict). Risolto:
   - Ingestor: unico writer (read-write, WAL mode)
   - Pipeline: `duckdb.connect(path, read_only=True)` — riconnette ad ogni ciclo per leggere dati freschi

3. **API Databento** — `db.set_api_key()` non esiste. Corretto in `db.Live(key=api_key)`.

4. **Dataset mancante** — Aggiunto `dataset="GLBX.MDP3"` nelle subscribe.

5. **Cartella ml_models mancante** — Creata con 4 modelli:
   - `bounce_prob.joblib` (XGBClassifier, da calibration/)
   - `regime_persistence.joblib` (XGBClassifier, da calibration_mbo/)
   - `volatility_30min.joblib` (XGBClassifier, da calibration/)
   - `scaler.joblib` (StandardScaler identita' — placeholder, non scala nulla)

### Problemi APERTI:

1. **Scaler non addestrato** — Lo scaler.joblib e' un passthrough (identita'). I modelli XGBoost funzionano comunque senza scaling, ma per prestazioni ottimali servirebbe riaddestrare lo scaler sulle feature reali.

2. **Feature mismatch potenziale** — I modelli sono stati addestrati su feature MBO (con order book). Ora le feature vengono da trades-only. I nomi delle colonne matchano (ofi_buy, ofi_sell, vol_1min, etc.) ma i VALORI statistici sono diversi. Idealmente i modelli andrebbero riaddestrati su dati trades.

3. **GEXBot non integrato** — Il WebSocket GEXBot esiste come progetto separato ma non e' collegato alla pipeline. I livelli GEX sono hardcoded in `live_config.json` come `gex_threshold_levels: [5500, 5520, 5540, 5560, 5580]`.

4. **Mode paper** — Il sistema e' in modalita' paper (`"mode": "paper"` in config). Non si scambia denaro reale finche' non si cambia a `"mode": "live"`.

---

## 4. CONFIGURAZIONE (live_config.json)

```json
{
  "databento_api_key": "db-53HKVrspUA65WmHVQCVR3NyiBaKEQ",
  "es_symbol": "ES.c.0",
  "es_next_symbol": "ESM6",
  "duckdb_path": "C:\\Users\\annal\\Desktop\\ML DATABASE\\ml_gold.duckdb",
  "models_dir": "C:\\Users\\annal\\Desktop\\ML DATABASE\\ml_models",
  "bridge_host": "127.0.0.1",
  "bridge_port": 5555,
  "mode": "paper",
  "max_daily_loss_pts": 40,
  "max_consec_losses": 5,
  "min_confidence": 0.60,
  "reset_hour_utc": 22,
  "bar_interval_min": 5,
  "mbo_flush_interval_sec": 300,
  "feature_window_min": 60,
  "gex_threshold_levels": [5500, 5520, 5540, 5560, 5580],
  "dashboard_port": 8501
}
```

Note: `ES.c.0` = continuous front-month contract. Rollover gestito da `auto_rollover_futures.py`.

---

## 5. MODELLI ML

Tutti e 3 sono XGBClassifier con metodo `predict_proba()`.

| Modello | File | Scopo | Peso nel segnale |
|---------|------|-------|-------------------|
| Bounce | bounce_prob.joblib | Probabilita' di rimbalzo prezzo | 50% |
| Regime | regime_persistence.joblib | Stabilita' del regime di mercato | 30% |
| Volatility | volatility_30min.joblib | Probabilita' alta volatilita' 30min | 20% (invertito) |

Formula segnale: `overall_conf = bounce*0.50 + regime*0.30 + (1-vol)*0.20`
Se `overall_conf >= 0.60` (min_confidence) e risk check OK -> segnale inviato.
Direzione: LONG se bounce_prob > 0.55, altrimenti SHORT.

---

## 6. FEATURE ENGINEERING (da schema trades)

Le feature calcolate dalla pipeline (trades-only, NO order book):

| Feature | Calcolo | Note |
|---------|---------|------|
| return_1min | media pct_change ultimi 60 trade | momentum breve |
| return_5min | media pct_change ultimi 300 trade | momentum medio |
| vol_1min | std pct_change ultimi 60 trade | volatilita' breve |
| vol_5min | std pct_change ultimi 300 trade | volatilita' media |
| ofi_buy | sum(size) dove side='B' | volume buyer aggressor |
| ofi_sell | sum(size) dove side='A' | volume seller aggressor |
| ofi_net | ofi_buy - ofi_sell | imbalance netto |
| ofi_ratio | ofi_net / (ofi_buy + ofi_sell + 1) | imbalance normalizzato |
| trade_buy_volume | = ofi_buy (in schema trades) | proxy |
| trade_sell_volume | = ofi_sell (in schema trades) | proxy |
| trade_imbalance | = ofi_ratio | proxy |
| add_buy_count | count trade side='B' | proxy order book |
| add_sell_count | count trade side='A' | proxy order book |
| cancel_buy_count | 0 (non disponibile) | placeholder |
| cancel_sell_count | 0 (non disponibile) | placeholder |
| gex_proximity | 1/(1+min_dist_to_gex_level) | da config |
| low_vol_regime | 1 se vol_5min < 0.0003, else 0 | binario |

---

## 7. FLUSSO DATI IN TEMPO REALE

```
1. Databento invia trade ES in tempo reale (schema "trades", dataset "GLBX.MDP3")
2. Ingestor bufferizza in deque(200k) e flush ogni 300 sec in DuckDB (tabella trades_live)
3. Pipeline ogni 5 min:
   a. Riconnette DuckDB in read-only
   b. Legge ultimi 60 min di trade
   c. Calcola 17 feature
   d. Scala con scaler (attualmente passthrough)
   e. predict_proba su 3 modelli
   f. Calcola overall_conf
   g. Risk check (daily loss < 40pt, consec losses < 5)
   h. Se OK: invia JSON al bridge TCP su porta 5555
4. Bridge riceve segnale e lo inoltra a tutti i client NT8 connessi
5. NT8 riceve JSON, esegue ordine, invia ACK
```

---

## 8. RISK MANAGEMENT

Doppio livello:
- Python (pipeline): max 40 pt daily loss, max 5 consecutive losses, min confidence 0.60
- C# (NT8): stesso, come backup locale. Reset giornaliero a 22:00 UTC.

---

## 9. SEQUENZA DI AVVIO

PowerShell, 3 terminali separati, in ordine:

```
# Terminale 1 - Bridge
cd "C:\Users\annal\Desktop\ML DATABASE"
python nt8_bridge_daemon.py

# Terminale 2 - Ingestor
cd "C:\Users\annal\Desktop\ML DATABASE"
python databento_live_ingestor.py

# Terminale 3 - Pipeline
cd "C:\Users\annal\Desktop\ML DATABASE"
python main_live_pipeline.py
```

NinjaTrader 8:
1. Copiare MLAutoStrategy_P1.cs in Documents\NinjaTrader 8\bin\Custom\Strategies\
2. NinjaScript Editor -> Compile (F5)
3. Grafico ES (1min o 5min) -> Strategies -> MLAutoStrategy_P1 -> OK

---

## 10. DIPENDENZE PYTHON

```
databento
duckdb
pandas
numpy
joblib
xgboost
scikit-learn
streamlit (opzionale, per dashboard)
```

---

## 11. STRUTTURA TABELLA DuckDB (trades_live)

```sql
CREATE TABLE trades_live (
    ts_event TIMESTAMP,
    price DOUBLE,
    size INTEGER,
    side VARCHAR,       -- 'B' = buyer aggressor, 'A' = seller aggressor
    flags INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_trades_ts ON trades_live(ts_event DESC);
```

---

## 12. COSA SERVE FARE (PROSSIMI STEP)

1. **Integrare GEXBot WebSocket** — creare gex_ingestor.py che si connette al WS di GEXBot, riceve i livelli GEX e li cachea in gex_cache.json. La pipeline li legge come feature aggiuntive.

2. **Riaddestramento modelli** — I modelli attuali sono stati trainati su feature MBO. Con schema trades le feature sono diverse (meno granulari). Serve riaddestrare i 3 XGBClassifier su dati trades storici.

3. **Scaler reale** — Addestrare un vero StandardScaler sulle feature trades e salvarlo come scaler.joblib.

4. **Validazione paper 3+ giorni** — Far girare il sistema in paper mode e verificare che i segnali siano sensati prima di passare a live.

5. **Script avvio unificato** — Creare un .bat o .ps1 che lancia tutti e 3 i processi Python in sequenza.

---

## 13. NOTE PER QWEN

- Il codice e' su Windows 10/11, paths con backslash
- Python 3.10+ richiesto
- NinjaTrader 8 deve girare sulla stessa macchina (localhost:5555)
- Il DB DuckDB (`ml_gold.duckdb`) puo' avere file .wal, .wal.bak — sono normali (WAL mode)
- Se DuckDB da' lock error: chiudere TUTTI i processi Python prima, poi cancellare i file .wal e riavviare
- L'API key Databento e' nel config JSON — NON condividerla pubblicamente
- I livelli GEX in config sono statici e probabilmente obsoleti — vanno aggiornati o automatizzati via GEXBot
