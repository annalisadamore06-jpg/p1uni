# CHECKLIST GO-LIVE — 9 Aprile 2026

## Ora Accensione: 08:00 locali (06:00 UTC)

---

## PRE-AVVIO (5 minuti)

- [ ] PC acceso, connesso a Internet
- [ ] Nessun altro processo Python aperto che usa il DB
  ```
  powershell: Get-Process python
  ```
- [ ] NinjaTrader 8 aperto e loggato (o sara' avviato dall'orchestrator)

## AVVIO

```powershell
cd C:\Users\annal\Desktop\P1UNI
python main.py --mode paper --config config/settings.yaml
```

## PRIMI 5 MINUTI — Cosa Controllare

1. **Log startup**: cerca queste righe (tutte devono apparire)
   ```
   P1UNI STARTING — Mode: PAPER
   V3.5 PRODUCTION engine loaded
   Databento connected and subscribed
   All X threads started. System running.
   ```

2. **Databento connesso?**
   ```
   Wrote XXX records to trades_live
   ```
   Se vedi "Failed to resolve symbol" -> il mercato potrebbe essere chiuso

3. **V3.5 attivo?**
   ```
   V3.5 PRODUCTION engine loaded: 3 models, 222 features
   ```
   Se vedi "V3.5 Bridge init failed" -> controlla il path al DB gold

4. **VIX < 35?** (check manuale su TradingView o broker)

5. **Nessun errore critico nei primi 5 min**

## DURANTE LA GIORNATA

- **Signal Engine**: ogni 5 minuti vedrai:
  ```
  Decision: SKIP | signal=NEUTRAL conf=0.xxx phase=morning_eu
  ```
  oppure (se c'e' un segnale forte):
  ```
  Decision: EXECUTE | signal=LONG conf=0.750 phase=afternoon_us
  [PAPER] Order FILLED: LONG x1 @ 5420.50
  ```

- **Gate Attivi**: devi vedere blocchi nelle ore morte
  ```
  V3.5 GATE BLOCKED: Dead hour 04 UTC
  ```

- **Telegram**: dovresti ricevere heartbeat ogni 5 min

## PROCEDURA DI EMERGENZA (Kill Switch)

```
# Metodo 1: Ctrl+C nel terminale
# Metodo 2: da un altro terminale
powershell: Get-Process python | Stop-Process -Force
```

## Telegram Kill Switch
Manda `/flatten` al bot @P1TERMINAL_bot

## METRICHE DA GUARDARE A FINE GIORNATA

- Quanti trade in paper mode?
- Quanti segnali generati vs bloccati dai gate?
- Il P&L paper e' realistico?
- Ci sono stati crash o disconnessioni?

## CRITERI PER PASSARE A LIVE

- [ ] 3 giorni paper senza crash
- [ ] Gate attivi correttamente (log verificati)
- [ ] P&L paper coerente con backtest (non troppo diverso)
- [ ] Telegram alert funzionanti
- [ ] NinjaTrader riceve segnali (in paper)
