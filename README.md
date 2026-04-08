# P1UNI - Sistema Unificato Autotrading ES Futures

Sistema di autotrading per E-mini S&P 500 (ES) futures basato su ML (XGBoost ensemble) con esecuzione su NinjaTrader 8.

## Architettura

```
GEXBOT WS (GEX levels)  --|                                    +--> TELEGRAM ALERTS
P1-LITE (ranges/spot)   --|--> NORMALIZER --> VALIDATOR --> DB -+--> FEATURE BUILDER --> ML ENSEMBLE
DATABENTO (trades live)  --|                                    |         |
                                                                |    SIGNAL ENGINE <-- LEVEL VALIDATOR
                                                                |         |                    |
                                                                |    RISK MANAGER         SETTLEMENT
                                                                |         |               RANGES
                                                                |    NINJA BRIDGE (TCP:5555)
                                                                |         |
                                                                +--> NINJATRADER 8 (C#)
```

**4 Thread principali:**
1. **Ingestion**: Databento trades + GexBot WS + P1-Lite ranges
2. **ML Inference**: Feature building + ensemble predict ogni 5 min
3. **Signal Engine + Bridge**: Validazione segnali + invio a NT8
4. **Monitor**: Salute sistema + heartbeat Telegram

## Requisiti

- Python 3.10+
- NinjaTrader 8 (sulla stessa macchina)
- Account Databento Standard ($179/mese CME)
- DuckDB (incluso via pip)
- Windows 10/11 (per NinjaTrader)

## Installazione

```bash
git clone https://github.com/YOUR_USER/p1uni.git
cd p1uni
pip install -r requirements.txt
```

## Configurazione

```bash
# 1. Copia config esempio
cp config/settings.example.yaml config/settings.yaml

# 2. Modifica settings.yaml: inserisci API key Databento, token Telegram, path DB

# 3. Copia database
# Metti ml_gold.duckdb nella cartella data/

# 4. Copia modelli ML
# Metti i file .joblib in ml_models/
```

## Avvio

```bash
# Paper mode (simulazione, default)
python main.py --mode paper

# Live mode (produzione, ATTENZIONE: ordini reali!)
python main.py --mode live
```

## NinjaTrader 8

1. Copia `docs/MLAutoStrategy_P1.cs` in `Documents\NinjaTrader 8\bin\Custom\Strategies\`
2. NinjaScript Editor -> Compile (F5)
3. Grafico ES (5min) -> Strategies -> MLAutoStrategy_P1 -> OK
4. La strategia si connette automaticamente a `localhost:5555`

## Struttura Progetto

```
p1uni/
├── config/settings.yaml     # Configurazione (non committata)
├── src/
│   ├── core/                # DB, normalizer, validator
│   ├── ingestion/           # Adapter per 3 fonti dati
│   ├── ml/                  # Feature builder + ensemble
│   ├── execution/           # Signal engine + bridge + risk
│   └── utils/               # Logger, Telegram, monitor
├── tests/                   # pytest
├── main.py                  # Orchestratore
└── PROMPT_QWEN_P1UNI.md    # Specifiche tecniche complete
```

## Testing

```bash
pytest tests/ -v
```

## 5 Fasi di Trading (UTC)

| Fase | Orario | Trading | Note |
|------|--------|---------|------|
| Notte Early | 22:00-01:00 | NO | Solo osservazione |
| Notte Full | 01:00-09:30 | Conservativo | Confidence 0.70, max 2 trade |
| Mattina | 09:30-12:00 | Pieno | Apertura US |
| Pomeriggio | 12:00-20:00 | Pieno | Attenzione FOMC 14:00-14:30 |
| Stop | 20:00-22:00 | NO | Chiudi posizioni |

## Risk Management

- Max daily loss: 40 punti ($2,000)
- Max consecutive losses: 5
- Min confidence: 0.60 (0.70 di notte)
- Doppio livello: Python + C# (NT8 backup)
- Reset giornaliero: 22:00 UTC
