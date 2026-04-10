# MORNING READINESS REPORT
**Generated**: 2026-04-10 05:45:58 UTC

## System Checks

| Component | Status | Details |
|-----------|--------|---------|
| NinjaTrader | OK | PID 43848 |
| Databento API | OK | API key valid |
| GexBot API | OK | Prefix: blue |
| V3.5 Models | OK | 3/3 models present |
| Live DB | FAIL | IO Error: Cannot open file "\\?\C:\Users\annal\Desktop\P1UNI\data\p1uni_live.duckdb": Impossibile accedere al file. Il file è utilizzato da un altro processo.

File is already open in 
C:\Program Files\Python313\python.exe (PID 40292) |
| CPU | OK | 5% |
| RAM | OK | 22% |
| Disk | OK | 1137 GB free |

## Verdict: ISSUES DETECTED

## Launch Command
```
cd C:\Users\annal\Desktop\P1UNI
python main.py --mode paper --config config/settings.yaml
```