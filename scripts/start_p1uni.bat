@echo off
:: P1UNI Launcher con Auto-Restart (Watchdog)
:: Uso: doppio click o da Task Scheduler
:: Se il processo crasha, attende 30 secondi e riavvia

title P1UNI Trading System
cd /d "C:\Users\annal\Desktop\P1UNI"

echo ============================================
echo   P1UNI Trading System - Watchdog Launcher
echo   %date% %time%
echo ============================================

:loop
echo.
echo [%date% %time%] Avvio P1UNI...
python main.py --mode paper --config config/settings.yaml

echo.
echo [%date% %time%] P1UNI terminato (exit code: %ERRORLEVEL%)

:: Se exit code 0 = shutdown volontario (Ctrl+C), esci
if %ERRORLEVEL% EQU 0 (
    echo Shutdown volontario. Esco dal watchdog.
    pause
    exit /b 0
)

:: Altrimenti: crash, riavvia dopo 30 secondi
echo CRASH RILEVATO! Riavvio tra 30 secondi...
echo Premi Ctrl+C per annullare il riavvio.
timeout /t 30

goto loop
