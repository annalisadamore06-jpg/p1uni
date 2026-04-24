@echo off
title P1UNI Watchdog
cd /d "C:\Users\annal\Desktop\P1UNI\orchestrator"

:loop
"C:\Program Files\Python313\python.exe" watchdog.py --loop --interval 60
echo.
echo Watchdog exited at %time% - restarting in 10s...
timeout /t 10 /nobreak >nul
goto loop
