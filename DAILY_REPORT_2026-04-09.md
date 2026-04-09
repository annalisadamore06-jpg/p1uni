# P1UNI — Daily Report 2026-04-09

**Session**: Giovedi 9 aprile 2026
**Time range**: 16:20 - 17:00 UTC (~40 minuti effettivi)
**Mode**: Paper trading

---

## STATO FINALE

Sistema fermato alle ~17:00 UTC su richiesta utente. Nessun trade eseguito (paper mode, nessuna posizione aperta).

## DATI ACQUISITI

### Live streaming (Databento)
- **Tabella**: `trades_live` in `data/p1uni_live.duckdb`
- **Totale**: 35,818 trade ES
- **Finestra temporale**: 16:20 - 16:26 UTC (~6 minuti)
- **Side distribution**: bilanciato B/A (fix del bug `side` ufficialmente convalidato)
- **Prezzo ES**: ~6817-6820 area

### Storico (Databento Historical)
- **Tabella**: `historical_trades` in `data/p1uni_history.duckdb`
- **Totale**: **1,009,743 trade ES**
- **Giorni coperti**: 3 giorni completi (6, 8, 9 aprile 2026)
- **Prezzo range**: 6586.50 - 6847.25
- **Side distribution**: 499,460 A / 510,281 B / 2 N (perfettamente bilanciato 49.5%/50.5%)
- **Giorni mancanti**: 7, 3, 2 aprile (timeout durante harvest, recuperabili)
- **Pre-1 aprile**: 401 auth fail (limite retention key)

## PROBLEMI RILEVATI DURANTE LA SESSIONE

### 1. Databento Live key confusion
- Key OLD `REDACTED_KEY...`: funzionava per Live (~35K trade), poi improvvisamente **Authentication failed**
- Key NEW `REDACTED_KEY...`: funzionava per Historical (~1M trade), poi improvvisamente **Authentication failed**
- Entrambe bloccate al momento del fermo sistema
- **Probabile causa**: rate limit Databento dopo molti retry/reconnect durante i debug
- **Azione richiesta**: verificare sul portale Databento lo stato delle key e l'utilizzo del piano

### 2. GexBot WS — Connesso ma 0 messages
- Negotiate API `api.gexbot.com`: OK (prefix=`blue`)
- Join gruppi: 24 gruppi joined (ES + NQ su tutti gli hub: classic, state_greeks_zero, state_greeks_one, orderflow)
- **MA**: zero messaggi ricevuti
- Errore Azure: `WinError 10060` (connection timeout su `ws.gex.bot:443`)
- **Possibili cause**:
  - Firewall Windows blocca connessioni WebSocket persistenti verso `ws.gex.bot`
  - I gruppi joined potrebbero non essere quelli attivi (pubblicazione server differente)
  - Ticker `ES_SPX` vs `ES` (da verificare col provider)
- **Azione richiesta**: test connessione manuale a `wss://ws.gex.bot:443` con tool esterno (wscat, Postman) per isolare il problema

### 3. Signal Engine — Bassa confidence (atteso)
- Fasi: correttamente `morning_eu` -> `afternoon_us`
- 5 Decision calls, tutte `SKIP`: confidence 0.148 < min 0.60
- **Causa**: il V3.5 Bridge legge 222 feature dal DB gold (`ml_gold.duckdb` in ML DATABASE), ma quel DB non viene aggiornato in real-time senza GexBot funzionante. Le feature GEX sono stale -> confidence bassa.
- **Comportamento corretto**: il modello rifiuta di tradare senza dati freschi. Safety gate funziona.

## COSA FUNZIONA (garantito)

| Componente | Stato |
|-----------|-------|
| Sistema base Python + thread | OK |
| DuckDB write/read | OK |
| Databento Live adapter (side fix) | OK (quando auth funziona) |
| Databento Historical | OK (quando auth funziona) |
| V3.5 Engine (3 modelli + 4 Gate) | OK, caricato, testato |
| Session Manager (5 fasi + transizioni) | OK |
| NinjaBridge (paper mode) | OK |
| Telegram notifications | OK (il bot ti ha mandato messaggi) |
| Boot sequence script | Scritto, da testare |
| Morning readiness check | Scritto, da testare |
| Scheduled task domani 07:45 | ATTIVO |

## COSA NON FUNZIONA (da risolvere)

| Componente | Problema | Priorita |
|-----------|----------|----------|
| Databento API (entrambe key) | Authentication failed | CRITICA — bloccante |
| GexBot WS messages | Connect OK, 0 messaggi | ALTA — blocca le feature GEX |
| P1-Lite adapter | DISABLED (mai configurato) | BASSA |
| TWS/IBC auto-start | Mai testato | BASSA |

## SCHEDULED TASK PER DOMANI

**Task ID**: `p1uni-morning-boot`
**Ora**: Venerdi 10 aprile 2026, 07:45 locali
**Azione**: Lancia `morning_check.py`, se OK lancia `main.py --mode paper`, notifica Telegram

**Attenzione**: il task scheduler funziona solo se Claude Code e' in esecuzione a quell'ora. Se non lo e', serve un vero cron di Windows Task Scheduler (procedura in `scripts/TASK_SCHEDULER_SETUP.md`).

## AZIONI RICHIESTE DALL'UTENTE PRIMA DI DOMANI

1. **Verifica Databento portal**: https://databento.com/portal/keys
   - Stato delle 2 key (Live + Historical)
   - Usage/limiti del piano
   - Eventuali alert di sessioni aperte / throttling
2. **Verifica GexBot**: contattare support o provare `wscat -c "wss://ws.gex.bot:443/client/hubs/orderflow?access_token=..."` per isolare il problema
3. **Controllare Windows Firewall**: aggiungere eccezioni per `python.exe` in entrata/uscita
4. **Se possibile**: testare se altri computer/reti hanno lo stesso problema (per escludere firewall locale)

## DATI A DISPOSIZIONE PER ANALISI OFFLINE

Mentre aspetti che le connessioni si sblocchino, **abbiamo 1+ milione di trade ES puliti**.
Possibili analisi offline (non richiedono Internet):
- Feature engineering VPIN, Kyle's Lambda, Order Flow Imbalance
- Backtest V3.5 sui 3 giorni (6-8-9 aprile)
- Side balance analysis (verificare se il bug e' davvero risolto su tutti i giorni)
- Gap analysis, volume profile, microstructure
- Fine-tuning V3.5 sui dati freschi (se la GPU e' disponibile)

## COMMIT GITHUB

Tutti i fix, gli script e i dati (senza DB) sono pushati su:
`https://github.com/annalisadamore06-jpg/p1uni` — branch `main`
