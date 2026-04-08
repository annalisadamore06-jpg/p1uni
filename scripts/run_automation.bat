@echo off
:: ============================================
::  P1UNI MASTER LAUNCHER — Avvia TUTTO
:: ============================================
:: Uso: doppio click, da Task Scheduler, o da startup
:: Sequenza: Orchestrator (gestisce TWS, NT8, Chrome, main.py)
::
:: L'orchestrator fa tutto da solo:
::   1. Controlla se TWS/IBC e' attivo, se no lo avvia
::   2. Controlla se NT8 e' attivo, se no lo avvia + auto-login
::   3. Controlla se Chrome/P1-Lite e' attivo, se no lo apre
::   4. Avvia main.py (il trading system)
::   5. Monitora tutto ogni 30 secondi e riavvia se necessario

title P1UNI Master Automation
cd /d "C:\Users\annal\Desktop\P1UNI"

echo ============================================
echo   P1UNI MASTER AUTOMATION
echo   %date% %time%
echo ============================================
echo.

:: Verifica Python
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERRORE] Python non trovato nel PATH!
    echo Installa Python 3.10+ e aggiungi al PATH.
    pause
    exit /b 1
)

:: Verifica dipendenze base
python -c "import psutil, yaml" 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [INFO] Installazione dipendenze...
    pip install -r requirements.txt
)

echo.
echo [INFO] Avvio System Orchestrator...
echo [INFO] L'orchestrator gestira' automaticamente TWS, NT8, Chrome e P1UNI.
echo [INFO] Premi Ctrl+C per fermare tutto.
echo.

:loop
python src/automation/system_orchestrator.py

echo.
echo [%date% %time%] Orchestrator terminato (exit code: %ERRORLEVEL%)

if %ERRORLEVEL% EQU 0 (
    echo Shutdown volontario.
    pause
    exit /b 0
)

echo Riavvio orchestrator tra 10 secondi...
timeout /t 10

goto loop
