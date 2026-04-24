@echo off
:: P1UNI Launcher con Auto-Restart (Watchdog)
:: Uso: doppio click o da Task Scheduler
:: Se il processo crasha, attende 30 secondi e riavvia
::
:: Mode precedence (alta -> bassa):
::   1. --mode CLI (se qualcuno lo aggiunge a mano)
::   2. P1UNI_MODE env var (default "paper")
::   3. settings.yaml system.mode
::
:: Per passare a LIVE:  set P1UNI_MODE=live  &&  start_p1uni.bat

title P1UNI Trading System
cd /d "C:\Users\annal\Desktop\P1UNI"

if "%P1UNI_MODE%"=="" set P1UNI_MODE=paper

echo ============================================
echo   P1UNI Trading System - Watchdog Launcher
echo   %date% %time%
echo   Mode: %P1UNI_MODE%
echo ============================================

:loop
echo.
echo [%date% %time%] Avvio P1UNI (mode=%P1UNI_MODE%)...
python main.py --mode %P1UNI_MODE% --config config/settings.yaml

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
