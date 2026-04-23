# P1UNI — BOOT_PLAN

**Scopo:** P1UNI è il SINGOLO punto di controllo del sistema di trading automatico.
Nessun componente parte da fuori P1UNI. L'orchestrator (`orchestrator/`) è l'unico
supervisore attivo; i vecchi launcher multipli vengono deprecati.

**Last updated:** 2026-04-23 (post audit conflitti)
**Owner:** Annalisa (bulenox $150K, MES, V40+Virgin ensemble)

---

## 0. Audit FASE 1 — Stato osservato (23/04/2026 14:30 CEST)

### 0.1 Processi Python attivi (9)

| PID | Script | Parent | Start | Problema |
|---|---|---|---|---|
| 6276 | `WEBSOCKET/collector.py --no-rth` | 27600 | 20/04 17:25 | OK — singleton |
| 3720 | `p1-lite/app.py` (dashboard) | 17572 | 21/04 07:31 | OK — singleton |
| 24292 | `WEBSOCKET/nt8_bridge_from_scraper.py` | 31684 | 22/04 04:33 | OK — singleton |
| 21416 | `WEBSOCKET/ninja_bridge_enhanced.py` | 20056 | 22/04 04:49 | ⚠️ Parallelo a `nt8_bridge_from_scraper` — scopi sovrapposti |
| 31416 | `P1UNI/main.py --mode paper` (no --config) | 4012 | 22/04 04:54 | 🔴 **DUP 1/3** (manual/old start) |
| 31740 | `P1UNI/main.py --mode paper --config config/settings.yaml` | 32112 | 22/04 08:00 | 🔴 **DUP 2/3** (P1UNI_BootSequence ieri) |
| 13220 | `P1UNI/main.py --mode paper --config config/settings.yaml` | 31644 | **23/04 08:00** | 🔴 **DUP 3/3** (P1UNI_BootSequence oggi) |
| 5428 | `P1UNI/scripts\gexbot_scraper.py` | **13220** (child di main) | 23/04 08:00 | 🔴 **DUP** — figlio del main oggi |
| 28800 | `P1UNI/scripts\gexbot_scraper.py` | **31740** (child di altro main) | 23/04 09:32 | 🔴 **DUP** — figlio del main di ieri |

### 0.2 Java / TWS

| PID | Processo | CommandLine | Stato |
|---|---|---|---|
| 28828 | java.exe (TWS via IBC) | `ibcalpha.ibc.IbcTws ... C:\Users\annal\Documents\IBC\config.ini` | ✅ OK — porta 7496 LISTENING |

### 0.3 NinjaTrader

| PID | Processo | Sessione | Stato |
|---|---|---|---|
| 24544 | NinjaTrader.exe | Services (session 0) | ⚠️ Running **ma** porta 5555 NON LISTENING → MLAutoStrategy_P1 non attiva o non connessa |

### 0.4 Porte

| Porta | Ruolo | Stato |
|---|---|---|
| **7496** | TWS API | ✅ LISTENING (PID 28828) |
| **5555** | NT8 TCP bridge (signals da P1UNI → NT8) | 🔴 **NON LISTENING** — MLAutoStrategy_P1 inattiva |
| 4001/4002 | IB Gateway | — non usato |
| 8501 | Streamlit dashboard (ML DB) | — non usato |
| 8054/8051/8055 | Fortress dashboards | — non usato |

### 0.5 Task Scheduler (trading-related)

| Task | Stato | Trigger | Azione | Problema |
|---|---|---|---|---|
| **P1UNI_Main** | ✅ Enabled | AtLogOn | `P1UNI\scripts\start_p1uni.bat` | Parte a login — fires main.py in watchdog loop |
| **P1UNI_BootSequence** | ✅ Enabled | Daily 08:00 | `pythonw P1UNI\scripts\boot_sequence.py --mode paper` | **🔴 CONFLITTO** — spawna un NUOVO main.py detached ogni giorno senza killare il precedente → causa dei 3 duplicati |
| **P1UNI_MorningCheck** | ✅ Enabled | Daily 07:55 | `pythonw P1UNI\scripts\morning_check.py` | OK — solo pre-flight check |
| **P1UNI_NightlyHarvest** | ✅ Enabled | Daily 23:00 | `pythonw P1UNI\scripts\nightly_data_harvest.py --days 1` | OK — job idempotente di data collection |
| **TradingSystem_AutoStart** | ✅ Enabled | Mon–Fri 00:00 | `ML DATABASE\MasterLauncher.bat` | **🔴 ROTTO** — referenzia path **inesistenti** (p1-lite\main.py, gexbot\main.py, databento-bridge\main.py) + fa `taskkill /F /IM NinjaTrader.exe` al boot! |
| **TradingSystem_OnBoot** | ✅ Enabled | AtStartup | `ML DATABASE\MasterLauncher.bat` | **🔴 ROTTO** — stesso file broken |
| P1UNI_DAILY_BOOT | ❌ Disabled | Daily 07:30 | `full_boot.py --mode paper` | Dead task — superseded |
| P1UNI_Monitor_13min | ❌ Disabled | Every 13min | `monitor_status.py` | Dead task |
| Timon-Watchdog-5min | ❌ Disabled | — | run-watchdog-hidden.vbs | Not trading-related |
| Timon-Watchdog-Startup | ❌ Disabled | AtLogOn | — | Not trading-related |
| TimonWatchdog (×2) | ❌ Disabled | — | — | Not trading-related |

### 0.6 Launcher scripts duplicati/conflittuali

| File | Ruolo | Problema |
|---|---|---|
| `P1UNI\scripts\start_p1uni.bat` | Watchdog loop main.py | OK se usato da P1UNI_Main |
| `P1UNI\scripts\boot_sequence.py` | Bootstrap 4 step (NT8+API+main) | 🔴 Spawna main detached senza dedup → duplica ogni giorno |
| `P1UNI\scripts\run_automation.bat` | Alt launcher via system_orchestrator | Non usato da nessun task — dead code |
| `P1UNI\scripts\install_task_scheduler.bat` | Installa 4 task | Già eseguito — i task esistono |
| `ML DATABASE\MasterLauncher.bat` | Legacy master | 🔴 Path inesistenti, killa NT8 al boot |
| `WEBSOCKET\start_live_system.bat` | Kill+restart scraper+bridge | 🔴 Killa processi di P1UNI senza sapere che main.py li gestisce |
| `WEBSOCKET\START_MONDAY.bat` | Kill zombies, restart collector | Ridondante con l'orchestrator |
| `WEBSOCKET\start_bridge.bat` / `start_trading_day.bat` / `start_ingest*.bat` | Vari launcher WS | Legacy, non invocati |
| `p1-lite\start_p1lite.bat` | Watchdog p1-lite (watchdog.py) | Non in Task Scheduler — non si autoavvia |

### 0.7 Bug critici confermati

1. **3 `main.py` di P1UNI contemporanei** → rischio di ordini duplicati verso NT8, race su DuckDB, Kelly sizing inflazionato.
2. **2 `gexbot_scraper.py`** (figli di 2 main diversi) → API quota raddoppiata, rate-limit GEXBot.
3. **NT8 porta 5555 non listening** → MLAutoStrategy_P1 v2.2 non sta ricevendo segnali; anche se P1UNI decidesse di fare un trade, NT8 non esegue.
4. **MasterLauncher.bat referenzia path morti** (p1-lite\main.py, gexbot\, databento-bridge\) — ogni boot Mon-Fri 00:00 il task fallisce silenziosamente.
5. **MasterLauncher fa `taskkill /F NinjaTrader.exe` al boot** — può killare una NT8 in uso.
6. **`state.json` di p1-lite fermo al 17/04** — `collector_lite.py` morto; `range_levels.csv` aggiornato da `app.py` (p1-lite dashboard) tramite IB client separato.
7. **`ninja_bridge_enhanced.py` parallelo a `nt8_bridge_from_scraper.py`** — stesso output (`nt8_live_enhanced.json`), un writer dovrebbe essere sufficiente.
8. **Nessuna deduplicazione al boot** in nessuno dei launcher esistenti.

---

## A. Boot sequence (accensione PC)

### A.1 Ordine canonico

```
t=0s    Windows avvia
t≈5s    Task Scheduler fires P1_Orchestrator (AtStartup trigger)
        └─ run_orchestrator.bat
           └─ python orchestrator.py --loop --fix --interval 60

t=0..60s  Orchestrator cycle #1:
          [1] TWS check
              ├─ se DOWN → launch IBC (TWS).lnk   [15s wait per login]
              └─ verifica porta 7496 LISTENING
          [2] NinjaTrader check
              ├─ se DOWN e external=True → WARN (launch manuale)
              └─ verifica PID NinjaTrader.exe
          [3] gexbot_ws_collector check
              └─ se DOWN → launch WEBSOCKET\collector.py --no-rth
          [4] p1uni_gexbot_scraper check
              └─ se DOWN → launch P1UNI\scripts\gexbot_scraper.py
          [5] p1lite_collector check        (depends_on TWS)
              └─ se DOWN → launch p1-lite\collector_lite.py
          [6] p1lite_dashboard check
              └─ se DOWN → launch p1-lite\app.py
          [7] nt8_bridge_scraper check      (depends_on gexbot_ws_collector)
              └─ se DOWN → launch WEBSOCKET\nt8_bridge_from_scraper.py
          [8] p1uni_main check              (depends_on TWS)
              └─ se DOWN → launch P1UNI\main.py --mode paper
          [9] morning_check.py              (solo prima volta di giornata)
              └─ scrive MORNING_READINESS_REPORT.md

t=60s+   Orchestrator entra in loop steady:
          - audit ogni 60s
          - dedup immediato se singleton violato
          - rilancio componenti morti
          - log in orchestrator/logs/orchestrator_YYYY-MM-DD.log
```

### A.2 Tempi di attesa tra i componenti

| Passo | Wait | Perché |
|---|---|---|
| TWS launch → login ready | **15s** | IBC autologin richiede TWS splash + API init |
| collector.py spawn → DB open | **3s** | `duckdb.connect()` richiede handshake |
| p1uni_main spawn → threads up | **5s** | 8 thread avviati in main.py |
| nt8_bridge spawn → first JSON write | **60s** | Primo ciclo di polling scraper DB |
| Ciclo orchestrator completo | **60s** | Default `--interval` |

### A.3 Verifica readiness di ogni componente

| Componente | Check di vitalità |
|---|---|
| TWS | `netstat -an` → `0.0.0.0:7496 LISTENING` + java process con `IbcTws` in cmdline |
| NT8 | `NinjaTrader.exe` PID + porta 5555 LISTENING (**richiede MLAutoStrategy_P1 loaded**) |
| p1uni_main | PID unico `main.py --mode paper --config config/settings.yaml` + log fresco |
| gexbot_scraper | `P1UNI/data/gexbot_latest.json` mtime < 180s |
| gexbot_ws_collector | Process vivo (file gexbot_snapshot.json solo RTH) |
| nt8_bridge_scraper | `WEBSOCKET/nt8_live_enhanced.json` mtime < 900s |
| p1lite_collector | `p1-lite/data/state.json` mtime < 120s |
| p1lite_dashboard | `app.py` PID |

---

## B. Componenti (inventario completo + ownership)

| # | Componente | Dove vive | Singleton? | External? | Dipendenze | Ownership |
|---|---|---|---|---|---|---|
| 1 | **TWS/IBC** | `C:\Jts\tws.exe` via `C:\IBC\IBC (TWS).lnk` | ✓ | via IBC auto-login | — | Annalisa (credenziali) |
| 2 | **NinjaTrader** | `C:\Program Files (x86)\NinjaTrader 8\bin64\NinjaTrader.exe` | ✓ | ✓ **manual** | — | Annalisa (login + strategia) |
| 3 | **MLAutoStrategy_P1 v2.2** | NT8 internal, source `ML DATABASE\MLAutoStrategy_P1.cs` (commit f5cb45b) | ✓ | ✓ loaded inside NT8 | NT8, porta 5555 | P1UNI (produce signal) |
| 4 | **p1uni_main** (`main.py`) | `P1UNI\main.py --mode paper` | ✓ | — | TWS, p1uni_gexbot_scraper, (nt8:5555) | P1UNI |
| 5 | **p1uni_gexbot_scraper** | `P1UNI\scripts\gexbot_scraper.py` | ✓ | — | GEXBot REST API | P1UNI |
| 6 | **p1lite_collector** | `p1-lite\collector_lite.py` | ✓ | — | TWS (port 7496, clientId=77) | p1-lite |
| 7 | **p1lite_dashboard** (app.py) | `p1-lite\app.py` | ✓ | — | state.json (locale), IB opz. | p1-lite |
| 8 | **gexbot_ws_collector** | `WEBSOCKET DATABASE\collector.py --no-rth` | ✓ | — | GEXBot WebSocket | WEBSOCKET DB |
| 9 | **nt8_bridge_scraper** | `WEBSOCKET DATABASE\nt8_bridge_from_scraper.py` | ✓ | — | gexbot_ws_collector DB | WEBSOCKET DB |
| 10 | **Databento adapter** | thread interno a p1uni_main | ✓ | — | DATABENTO_API_KEY env | P1UNI |
| 11 | **nightly_harvest** | `P1UNI\scripts\nightly_data_harvest.py` | job una volta/giorno | — | DATABENTO_API_KEY | Task Scheduler |

### Componenti DEPRECATI / da rimuovere

| Componente | Perché rimuoverlo |
|---|---|
| `WEBSOCKET\ninja_bridge_enhanced.py` | Parallelo a `nt8_bridge_from_scraper.py` — stesso output, no ownership chiaro |
| `ML DATABASE\MasterLauncher.bat` | Path inesistenti, killa NT8 al boot, referenzia repo morti |
| `ML DATABASE\MasterWatchdog.py` | Stesso runtime di `MasterLauncher.bat` |
| `ML DATABASE\databento_live_ingestor.py` / `main_live_pipeline.py` | Funzioni ora interne a `p1uni_main` |
| `WEBSOCKET\start_live_system.bat` / `start_trading_day.bat` / `START_MONDAY.bat` | Legacy duplicati |
| `p1-lite\watchdog.py` | Rimpiazzato dall'orchestrator globale |

---

## C. Monitoraggio continuo

### C.1 Health check loop (orchestrator)

Ogni 60s l'orchestrator:
1. **Audit** di tutti gli 11 componenti (da `components.py`).
2. Per ognuno: **ALIVE / DOWN / STALE / DUP**.
3. Se `singleton=true` e `len(pids)>1` → termina tutti tranne il PID più vecchio.
4. Se `DOWN` e le dipendenze sono `ALIVE` → rilancia.
5. Se `STALE` (health_file troppo vecchio) → NON uccide subito: segnala e rilancia solo se processo DOWN; altrimenti è responsabilità del watchdog interno al componente.

### C.2 Soglie di staleness (già calibrate in `components.py`)

| Componente | max_age_sec | Perché |
|---|---|---|
| p1lite_collector | 120 | state.json scritto ogni 1s |
| gexbot_scraper P1UNI | 180 | REST poll 60s |
| nt8_bridge_scraper | 900 | poll 60s ma tolerance 15min per gaps RTH |
| gexbot_ws_collector | — (no health_file) | File fresco solo durante RTH |

### C.3 Alerting

- **Orchestrator stesso**: log-only (no Telegram)
- **P1UNI main**: già manda Telegram via `telegram_bot_token` su crash/trade
- **Nightly harvest**: Telegram summary on completion

### C.4 Auto-restart rules

| Evento | Azione |
|---|---|
| Componente DOWN con deps OK | Launch immediato |
| Singleton violato | Keep oldest PID, kill others |
| Orchestrator stesso crasha | `run_orchestrator.bat` ha `goto loop` — riparte in 10s |
| Windows reboot | Task Scheduler `P1_Orchestrator` AtStartup rilancia tutto |

---

## D. Sessioni di trading (timeline day)

Tutti gli orari in **CEST** (Europe/Zurich-Rome), auto-adattato con DST.

### D.1 Timeline giornaliera (Lun–Ven)

```
00:00  midnight reset — risk counters, trailing DD
02:15  TWS daily reset (IBC handles) — breve disconnessione gestita da collector
07:55  P1UNI_MorningCheck fires → pre-flight report (MORNING_READINESS_REPORT.md)
08:00  Orchestrator garantisce tutti i componenti UP (era il compito di BootSequence)
09:00  MR (midnight range) snap — p1-lite scrive snapshots_YYYY-MM-DD.csv
09:30  Trading EU session può operare su MR levels
15:25  RTH pre-open — collector.py (WebSocket) inizia a ricevere GEXBot data
15:30  🇺🇸 US market open → OR (open range) snap — p1-lite scrive OR snap
15:30  P1UNI signal engine attivo: V40+Virgin ensemble + R5 hedging
22:00  US RTH close — signal engine va flat (posizioni chiuse), WS collector staccato
22:05  Sacred window post-close (10s) — orchestrator skip update
22:55  Hard stop trading (Annalisa bulenox rule)
23:00  P1UNI_NightlyHarvest fires → Databento historical harvest
```

### D.2 Pre-market (00:00–15:30 CEST) — cosa deve essere attivo

| Componente | Attivo? | Note |
|---|---|---|
| TWS | ✅ | Serve sempre (p1-lite usa IB per ES/SPX/VIX anche di notte) |
| NT8 | ✅ | Caricato con MLAutoStrategy_P1 (standby mode) |
| p1uni_main | ✅ | Signal engine in "await session=RTH" |
| p1lite_collector | ✅ | Calcola MR (midnight range) per 09:00 snap |
| gexbot_scraper P1UNI | ⚠️ | GEXBot API restituisce dati **stale di ieri** fino alle 15:30 — comportamento normale |
| gexbot_ws_collector | ✅ | Running ma stream vuoto fino a 15:25 |
| nt8_bridge_scraper | ✅ | Passa level placeholder a NT8 |

### D.3 RTH (15:30–22:00 CEST) — piena operatività

Tutti gli 11 componenti UP, health check stringente (max_age 60–120s), signal engine autorizzato a mandare ordini.

### D.4 After-hours (22:00–23:55)

| Componente | Azione |
|---|---|
| p1uni_main | Flat all positions, signal engine idle |
| gexbot_ws_collector | Può disconnettersi (normale) |
| p1lite_collector | Continua in "night mode" (cycle 1s) |
| nightly_harvest | 23:00 fires |

### D.5 Notte profonda (00:00–07:55)

Solo p1lite_collector + TWS attivi per calcolo MR. Tutto il resto sleep.
**Orchestrator continua a girare** — assicura che niente muoia silenziosamente.

### D.6 Weekend

- Sabato/Domenica: mercato chiuso.
- p1-lite `watchdog.py` (legacy) andava in idle — l'orchestrator invece continua a monitorare ma **non** lancia nuovi processi (regola aggiuntiva da implementare).
- TWS può essere chiuso (Annalisa manuale) — l'orchestrator segnala DOWN ma non rilancia (da aggiungere: weekend-aware skip).

---

## E. Task Scheduler finale — STATO TARGET

### E.1 Task DA MANTENERE (3)

| Task | Trigger | Azione |
|---|---|---|
| **P1_Orchestrator** | AtLogOn + AtStartup | `P1UNI\orchestrator\run_orchestrator.bat` |
| **P1UNI_MorningCheck** | Daily 07:55 | `pythonw P1UNI\scripts\morning_check.py` |
| **P1UNI_NightlyHarvest** | Daily 23:00 | `pythonw P1UNI\scripts\nightly_data_harvest.py --days 1` |

### E.2 Task DA ELIMINARE

| Task | Perché |
|---|---|
| **P1UNI_Main** | L'orchestrator è l'entry point. Il launcher interno (`start_p1uni.bat`) viene comunque riutilizzato dall'orchestrator per launch p1uni_main, ma NON va messo in Task Scheduler come entry separato. |
| **P1UNI_BootSequence** | `boot_sequence.py` spawna main.py detached senza dedup — è la causa dei 3 duplicati. Rimpiazzato dall'orchestrator. |
| **TradingSystem_AutoStart** | Referenzia `MasterLauncher.bat` con path inesistenti. Pura fonte di errori e `taskkill` di NT8 indesiderati. |
| **TradingSystem_OnBoot** | Stesso file broken. |
| **P1UNI_DAILY_BOOT** | Disabled, dead task. |
| **P1UNI_Monitor_13min** | Disabled, dead task. |
| **Timon-Watchdog-5min / Timon-Watchdog-Startup / TimonWatchdog (×2)** | Non trading-related, disabled, inquinano la lista. |

### E.3 Script di migrazione Task Scheduler

```powershell
# Crea P1_Orchestrator
powershell -ExecutionPolicy Bypass -File C:\Users\annal\Desktop\P1UNI\orchestrator\install_task.ps1

# Rimuove i task obsoleti
$dead = @(
  "P1UNI_Main",
  "P1UNI_BootSequence",
  "TradingSystem_AutoStart",
  "TradingSystem_OnBoot",
  "P1UNI_DAILY_BOOT",
  "P1UNI_Monitor_13min",
  "Timon-Watchdog-5min",
  "Timon-Watchdog-Startup",
  "TimonWatchdog"
)
foreach ($t in $dead) {
  schtasks /delete /tn $t /f 2>$null
}
```

---

## F. Eliminazione conflitti

### F.1 File da rimuovere / disabilitare fisicamente

| File | Azione | Perché |
|---|---|---|
| `ML DATABASE\MasterLauncher.bat` | Rename `.bat.DISABLED` | Path inesistenti + taskkill NT8 |
| `ML DATABASE\MasterWatchdog.py` | Rename `.py.DISABLED` | Rimpiazzato da orchestrator |
| `WEBSOCKET\start_live_system.bat` | Rename `.bat.DISABLED` | Kill processi di P1UNI |
| `WEBSOCKET\start_trading_day.bat` | Rename `.bat.DISABLED` | Legacy duplicato |
| `WEBSOCKET\START_MONDAY.bat` | Rename `.bat.DISABLED` | Legacy duplicato |
| `WEBSOCKET\ninja_bridge_enhanced.py` | Spegnere process (kill PID 21416), poi rename `.py.DISABLED` | Output sovrapposto a `nt8_bridge_from_scraper.py` |
| `P1UNI\scripts\boot_sequence.py` | Rename `.py.DISABLED` | Causa duplicazione main.py |
| `P1UNI\scripts\run_automation.bat` | Rename `.bat.DISABLED` | Dead code |
| `P1UNI\scripts\start_p1uni.bat` | **MANTENERE** | Usato dall'orchestrator come launch_cmd di p1uni_main |
| `p1-lite\watchdog.py` | Rename `.py.DISABLED` (dopo verifica che app.py scrive range_levels.csv indipendentemente) | Rimpiazzato da orchestrator |
| `p1-lite\start_p1lite.bat` | Rename `.bat.DISABLED` | Invoca watchdog.py (deprecato) |

### F.2 Processi attivi da KILLARE ADESSO (una volta)

```bash
# DUP — tieni il più vecchio (31416 per p1uni_main), kill gli altri
taskkill /PID 31740 /F    # P1UNI main duplicato 2/3
taskkill /PID 13220 /F    # P1UNI main duplicato 3/3
taskkill /PID 5428 /F     # gexbot_scraper figlio di 13220
taskkill /PID 28800 /F    # gexbot_scraper figlio di 31740

# ambiguo — valutare se spegnere
taskkill /PID 21416 /F    # ninja_bridge_enhanced.py (se confermato deprecato)
```

Equivalente via orchestrator: `python orchestrator.py --fix` (dedup keep-oldest).

### F.3 Fix MLAutoStrategy_P1 (porta 5555)

1. Apri NinjaTrader 8
2. Control Center → **Strategies**
3. Carica `MLAutoStrategy_P1` su chart ES (MES) 1-min
4. Parameters → `ListenPort=5555`, `ListenAddress=127.0.0.1`
5. **Enable** — verifica che `netstat -an | findstr :5555` mostri LISTENING

Senza questo step il Phase 4 edge (trailing 4pt) e qualsiasi altro segnale non raggiungono NT8.

### F.4 Config da centralizzare (single source of truth)

`P1UNI\config\settings.yaml` è l'unico file di verità per:
- mode (paper/live)
- TWS host/port/clientId
- GEXBot API key (via .env)
- Telegram bot token (via .env)
- Signal engine thresholds
- Hedging signals (R5 Phase-4 defaults)

I config dispersi in altri repo (`ML DATABASE\live_config.json`, `WEBSOCKET\gexbot_config.json`) diventano **read-only references** — ognuno dei processi WSDB li usa, ma i valori di verità sono in `settings.yaml`.

---

## Checklist di cutover (ordine operativo)

- [ ] 1. **Commit orchestrator** (fatto — commit `de88d97`, path `P1UNI/orchestrator/`)
- [ ] 2. **Commit BOOT_PLAN.md** (questo file)
- [ ] 3. Run `python orchestrator.py` (audit read-only) per baseline
- [ ] 4. Kill duplicati via `python orchestrator.py --fix` (prima volta a mano, osservare log)
- [ ] 5. Rimuovere Task Scheduler obsoleti (script in E.3)
- [ ] 6. Installare `P1_Orchestrator` (script in E.3)
- [ ] 7. Rinominare file deprecati (F.1) — NON cancellare, solo `.DISABLED`
- [ ] 8. Caricare `MLAutoStrategy_P1` su NT8 (F.3) — verificare porta 5555
- [ ] 9. Lasciare orchestrator in loop 24h — review logs il giorno dopo
- [ ] 10. Se stabile: aggiungere weekend-aware skip all'orchestrator (D.6)

---

## Invarianti che il piano garantisce

1. **One process, one truth**: un solo `main.py`, un solo `gexbot_scraper.py`, un solo `collector_lite.py`, un solo NT8.
2. **P1UNI è il cervello**: nessun launcher fuori da `P1UNI\orchestrator\` gestisce dipendenze o fa start/stop.
3. **Dead code marcato**: tutti i file legacy `.DISABLED` (reversibile — rename per reintegrare).
4. **Task Scheduler minimal**: 3 task, zero conflitti, audit in ≤ 5s.
5. **Failure mode is graceful**: se l'orchestrator crasha il `.bat` lo rilancia; se Windows reboot il Task Scheduler lo rilancia.
6. **Upgrade path is safe**: modifiche a `components.py` sono l'unico punto di contatto per aggiungere/togliere componenti — nessun `.bat` da toccare.

---

## Rischi residui (onesto)

- **NT8 non è auto-launchable come processo Windows affidabile** → `external=True` nell'orchestrator, resta responsabilità di Annalisa.
- **Weekend behavior non ancora implementato** → TODO: aggiungere `is_market_open_today()` check in `orchestrator.py`.
- **IBC login può fallire** (TWS demand re-auth, captcha, 2FA) → orchestrator segnala DOWN ma non risolve.
- **collector_lite.py race con app.py** su client ID 77 → se entrambi tentano connessione, IB rifiuta la seconda. Da verificare dopo reintroduzione di collector_lite.
- **Settings.yaml gitignored** → ogni clone del repo deve ripopolarlo manualmente (documentato in HANDOVER.md).
