# P1 Orchestrator

**Umbrella supervisor** per il sistema di trading automatico.
Responsabilità: garantire che ESATTAMENTE UN'ISTANZA di ogni componente richiesto sia viva,
nel giusto ordine di dipendenze, con i file di output freschi.

Non sostituisce i watchdog interni di ciascun componente. Li completa dall'alto.

---

## Avvio rapido

```bash
# Audit read-only (consigliato per il primo check)
C:\Users\annal\Desktop\P1UNI\orchestrator\audit_once.bat

# Loop con fix attivo (kills dup + lancia missing, ciclo 60s)
C:\Users\annal\Desktop\P1UNI\orchestrator\run_orchestrator.bat

# Auto-start al boot/login
powershell -ExecutionPolicy Bypass -File install_task.ps1
```

---

## Componenti monitorati

| # | Nome | Categoria | Singolo? | Depends on | Health file | Max age |
|---|---|---|---|---|---|---|
| 1 | TWS | TWS | ✓ | — | (port 7496 check) | — |
| 2 | p1lite_collector | DATA | ✓ | TWS | `p1-lite/data/state.json` | 120s |
| 3 | p1lite_dashboard | UI | ✓ | — | — | — |
| 4 | gexbot_ws_collector | DATA | ✓ | — | — | — |
| 5 | nt8_bridge_scraper | BRIDGE | ✓ | gexbot_ws_collector | `WEBSOCKET DATABASE/nt8_live_enhanced.json` | 900s |
| 6 | p1uni_main | EXEC | ✓ | TWS | — | — |
| 7 | p1uni_gexbot_scraper | DATA | ✓ | — | `P1UNI/data/gexbot_latest.json` | 180s |
| 8 | NinjaTrader | EXEC | ✓ (external) | — | — | — |

**External** = non si auto-lancia, richiede apertura manuale (NT8 ha UI login).

---

## Ordine di dipendenze (boot sequence)

```
┌─────────┐
│   TWS   │  (via IBC — C:\IBC\IBC (TWS).lnk, config C:\Users\annal\Documents\IBC\config.ini)
└────┬────┘
     │ port 7496
     ├──────────────────┐
     ▼                  ▼
┌──────────────┐   ┌──────────────┐
│ p1lite       │   │ p1uni_main   │──► signal + execution
│ collector    │   └──────────────┘
└──────────────┘            ▲
                            │ (indirect via TCP 5555 to NT8)
┌──────────────┐            │
│ gexbot_ws_   │            │
│ collector    │            │
└──────┬───────┘            │
       ▼                    │
┌──────────────┐            │
│ nt8_bridge_  │            │
│ from_scraper │            │
└──────────────┘            │
                            ▼
                     ┌────────────┐
                     │ NinjaTrader │
                     └────────────┘

Parallel (non bloccanti):
  - p1uni_gexbot_scraper  (REST polling, alimenta P1UNI ML)
  - p1lite_dashboard      (UI only, serve per visualizzazione)
```

---

## Come funziona l'orchestrator

### Audit
1. Itera tutti i processi del sistema (`psutil`).
2. Per ogni componente filtra per `match_name` (es. `python.exe`) + `match_cmd` (substring nell'args).
3. Se il componente ha `health_file`, confronta mtime con `max_age_sec`.
4. Se il componente ha `port_check`, verifica socket listening su `127.0.0.1:port`.
5. Produce report ASCII (default) o JSON (`--json`).

### Fix (`--fix`)
1. **Dedup**: se `singleton=True` e ci sono più PID, mantiene il **più vecchio** (più stabile) e termina gli altri.
2. **Launch missing**: in ordine topologico delle dipendenze; se una dep è DOWN o STALE, rimanda il figlio.
3. **External**: non lancia, solo avvisa.

### Loop (`--loop`)
Cicla ogni `--interval` (default 60s). Log ruotato giornaliero in `logs/orchestrator_YYYY-MM-DD.log`.

---

## Stato attuale del sistema (23/04/2026 14:00 CEST)

```
[TWS]
  [OK  ] TWS                      pids=[28828]       ← running from 22/04 23:39 via IBC

[DATA]
  [DOWN] p1lite_collector                            ← collector_lite.py crashed 21/04 04:41
  [OK  ] gexbot_ws_collector      pids=[6276]
  [DUP ] p1uni_gexbot_scraper     pids=[5428, 28800] ← 2 istanze duplicate

[BRIDGE]
  [OK  ] nt8_bridge_scraper       pids=[24292] age=46s

[EXEC]
  [DUP ] p1uni_main               pids=[13220, 31416, 31740]  ← 3 istanze duplicate!
  [OK  ] NinjaTrader              pids=[22712]

[UI]
  [OK  ] p1lite_dashboard         pids=[3720]
```

### Interventi consigliati

1. **Run `orchestrator.py --fix` una volta** per deduplicare `p1uni_main` (3x) e `p1uni_gexbot_scraper` (2x)
2. **Verificare perché p1-lite collector è morto** — controllare `logs/collector_lite.log` + `logs/watchdog.log`
3. **NON attivare `--loop --fix` finché non è stato validato a mano** il comportamento.

### Gotcha verificate

- `range_levels.csv` continua ad aggiornarsi anche con `collector_lite.py` morto — lo scrive un altro processo (probabilmente `app.py` dashboard ha il suo IB client).
- `state.json` fermo al 17/04 conferma che il master collector è giù.
- TWS **non** richiede reboot — è già attivo e connesso; le connessioni API (porta 52463→client) sono visibili via `netstat`.

---

## File in questa directory

| File | Scopo |
|---|---|
| `components.py` | Registry di tutti i componenti + metadati |
| `orchestrator.py` | Logica audit/fix/loop |
| `audit_once.bat` | Audit read-only (doppio click) |
| `run_orchestrator.bat` | Loop con fix, usato da Task Scheduler |
| `install_task.ps1` | Registra `P1_Orchestrator` nel Task Scheduler |
| `logs/` | Log giornalieri dell'orchestrator |

---

## Task Scheduler — state pre-orchestrator

Tasks già presenti (non rimuovere):

| Task | Trigger | Action |
|---|---|---|
| P1UNI_BootSequence | daily 08:00 | `pythonw P1UNI\scripts\boot_sequence.py --mode paper` |
| P1UNI_Main | AtLogOn | `P1UNI\scripts\start_p1uni.bat` |
| P1UNI_MorningCheck | daily 07:55 | `pythonw P1UNI\scripts\morning_check.py` |
| P1UNI_NightlyHarvest | daily 23:00 | `pythonw P1UNI\scripts\nightly_data_harvest.py --days 1` |
| TradingSystem_AutoStart | Mon-Fri 00:00 | `ML DATABASE\MasterLauncher.bat` |
| TradingSystem_OnBoot | AtStartup | `ML DATABASE\MasterLauncher.bat` |

L'orchestrator **non sostituisce** questi task: fa da umbrella. Se i task lanciano correttamente, l'orchestrator vede i processi alive e li lascia in pace. Se uno crasha, l'orchestrator lo rilancia (con `--fix`).

---

## Debug

```bash
# JSON output for scripting
python orchestrator.py --json

# Verifica match di un singolo componente
python -c "from components import BY_NAME; import json; print(json.dumps(BY_NAME['p1uni_main'], indent=2, default=str))"

# Tail log
powershell Get-Content logs\orchestrator_$(Get-Date -Format yyyy-MM-dd).log -Tail 50 -Wait
```

---

## Filosofia: cosa NON fa

- ❌ Non riavvia processi "sani" (dedup solo)
- ❌ Non manda allarmi Telegram (lasciato ai watchdog dei singoli componenti)
- ❌ Non gestisce finestre di mercato / blackout (lasciato a `watchdog.py` di p1-lite e al signal engine di P1UNI)
- ❌ Non monitora P&L o stato trade
- ❌ Non tocca TWS login (lo fa IBC)
