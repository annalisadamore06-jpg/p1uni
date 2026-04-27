@echo off
REM ============================================================
REM start_nt8_with_login.bat
REM Lancia NinjaTrader 8 + auto-login via PowerShell wrapper.
REM Chiamato dal watchdog (P1UNI/orchestrator/watchdog.py).
REM Idempotente: skip se NinjaTrader.exe gia in esecuzione.
REM ============================================================

set "NT8_EXE=C:\NinjaTrader 8\bin\NinjaTrader.exe"
set "AUTOLOGIN_PS1=C:\Users\annal\Desktop\P1UNI\orchestrator\nt8_autologin.ps1"

REM Skip se NT8 gia in esecuzione
tasklist /FI "IMAGENAME eq NinjaTrader.exe" 2>nul | find /I "NinjaTrader.exe" >nul
if %errorlevel%==0 (
    echo NinjaTrader gia in esecuzione - skip launch
    exit /b 0
)

REM Verifica NT8 path
if not exist "%NT8_EXE%" (
    echo ERRORE: NinjaTrader.exe non trovato in %NT8_EXE%
    exit /b 1
)

REM Lancia NT8 detached
start "" "%NT8_EXE%"

REM Attende avvio finestra login
timeout /t 3 /nobreak >nul

REM Lancia auto-login in background non bloccante
if exist "%AUTOLOGIN_PS1%" (
    start "" /B powershell -NoProfile -ExecutionPolicy Bypass -File "%AUTOLOGIN_PS1%"
) else (
    echo WARN: nt8_autologin.ps1 non trovato - login manuale richiesto
)

exit /b 0
