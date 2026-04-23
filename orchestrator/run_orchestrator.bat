@echo off
title P1 Orchestrator
cd /d "C:\Users\annal\Desktop\P1UNI\orchestrator"

:loop
"C:\Program Files\Python313\python.exe" orchestrator.py --loop --fix --interval 60
echo.
echo Orchestrator exited at %time% — restarting in 10s...
timeout /t 10 /nobreak >nul
goto loop
