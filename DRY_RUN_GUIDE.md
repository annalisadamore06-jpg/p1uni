# DRY RUN GUIDE — Primo Avvio Sicuro di P1UNI

## Prerequisiti

1. Python 3.10+ installato
2. Dipendenze installate:
   ```
   cd C:\Users\annal\Desktop\P1UNI
   pip install -r requirements.txt
   ```
3. File `config/settings.yaml` compilato (almeno i path DB)
4. Modelli ML nella cartella `ml_models/` (copia da `ML DATABASE\ml_models\`)

## Step 1: Verifica Struttura

```powershell
cd C:\Users\annal\Desktop\P1UNI
python -c "import src; print('Import OK')"
```

Se stampa "Import OK", la struttura e' corretta.

## Step 2: Lancia i Test

```powershell
cd C:\Users\annal\Desktop\P1UNI
python -m pytest tests/ -v --tb=short
```

Cosa aspettarsi: tutti i test devono passare (PASSED).
Se alcuni falliscono per moduli mancanti (zmq, websocket), e' normale —
quei test richiedono le dipendenze opzionali.

## Step 3: Dry Run (Paper Mode)

```powershell
cd C:\Users\annal\Desktop\P1UNI
python main.py --mode paper --config config/settings.yaml
```

### Cosa aspettarsi nei log

**Avvio (primi 10 secondi):**
```
[INFO] [p1uni.main] P1UNI initializing in PAPER mode...
[INFO] [p1uni.main] Singletons initialized: DB, Telegram, SessionManager
[INFO] [p1uni.ml.ensemble] Model bounce loaded in 15.2ms
[INFO] [p1uni.ml.ensemble] Model regime loaded in 12.1ms
[INFO] [p1uni.ml.ensemble] Model volatility loaded in 11.8ms
[INFO] [p1uni.ml.ensemble] MLEnsemble initialized: mode=FULL
[INFO] [p1uni.main] All components initialized
[INFO] [p1uni.main] P1UNI STARTING — Mode: PAPER
[INFO] [p1uni.main] Thread started: adapter_db
[INFO] [p1uni.main] Thread started: monitor
[INFO] [p1uni.main] Thread started: signal-engine
[INFO] [p1uni.main] All 4 threads started. System running.
```

**Se i modelli non sono trovati:**
```
[WARNING] Model bounce MISSING: ml_models/bounce_prob.joblib
[WARNING] Model regime MISSING: ml_models/regime_persistence.joblib
[ERROR] ML Ensemble NOT operational!
```
Questo e' normale se non hai ancora copiato i .joblib. Il sistema gira comunque
ma non genera segnali.

**Se Databento API key non e' configurata:**
```
[WARNING] Databento adapter DISABLED (no api_key)
```
Normale per il dry run. Il sistema gira senza feed live.

**Ogni 5 minuti (signal engine):**
```
[INFO] Decision: SKIP | signal=NONE conf=0.000 phase=halt step=1/6 | Trading not allowed
```
oppure (se in orario di trading):
```
[INFO] Decision: SKIP | signal=NONE conf=0.000 phase=morning_eu step=2/6 | Feature builder returned None
```

**Ogni 60 secondi (monitor):**
```
[DEBUG] Health: CPU=5% RAM=42% Disk=150GB free
```

### Come verificare che funziona

1. **Log file**: Controlla `logs/p1uni.log` — deve crescere continuamente
2. **Fase corretta**: Il log deve mostrare la fase giusta per l'orario corrente UTC
3. **Nessun crash**: Il sistema deve girare senza eccezioni per almeno 30 minuti
4. **Nessun ordine**: In paper mode senza feed, non deve apparire "[PAPER] Order"

## Step 4: Test Graceful Shutdown

Mentre il sistema gira, premi `Ctrl+C`.

Cosa aspettarsi:
```
[INFO] Ctrl+C received
[INFO] SHUTTING DOWN P1UNI
[INFO] [1/6] Blocking new signals...
[INFO] [2/6] Paper mode — skip flatten
[INFO] [3/6] Stopping adapters...
[INFO] [4/6] Stopping monitor...
[INFO] [5/6] Closing bridge and DB...
[INFO] [6/6] Sending shutdown notification...
[INFO] Shutdown complete.
```

Il processo deve chiudersi in meno di 5 secondi.

## Step 5: Dry Run con Feed Reale (Dopo aver inserito le API key)

Quando hai le API key configurate in settings.yaml:

```powershell
python main.py --mode paper --config config/settings.yaml
```

Ora vedrai anche:
```
[INFO] Connecting to Databento: GLBX.MDP3/ES.c.0/trades
[INFO] Databento connected and subscribed
[INFO] Flushed 1500 trades to DuckDB
[INFO] Decision: EXECUTE | signal=LONG conf=0.723 phase=morning_eu step=6/6
[INFO] [PAPER] Order FILLED: LONG x1 @ 5420.50
```

## Durata Consigliata

| Fase | Durata | Cosa verificare |
|------|--------|-----------------|
| Dry run senza feed | 30 min | Nessun crash, fasi corrette |
| Dry run con Databento | 2 ore | Dati entrano, feature calcolate |
| Paper trading completo | 48 ore | Segnali sensati, PnL paper realistico |
| Live (size minima) | 1 settimana | 1 contratto, monitoraggio costante |

## Troubleshooting

| Errore | Causa | Soluzione |
|--------|-------|-----------|
| `ModuleNotFoundError: No module named 'src'` | Python path | Lancia da `C:\Users\annal\Desktop\P1UNI\` |
| `DuckDB: database is locked` | Altro processo usa il DB | Chiudi tutti i Python/Jupyter |
| `Model MISSING` | File .joblib non trovato | Copia da `ML DATABASE\ml_models\` |
| `ConnectionError: Databento` | API key errata o rete | Verifica key in settings.yaml |
