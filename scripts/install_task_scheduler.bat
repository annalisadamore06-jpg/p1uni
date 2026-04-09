@echo off
:: Registra il task P1UNI_DAILY_BOOT in Windows Task Scheduler
:: Esegue master_orchestrator.py ogni giorno alle 07:30
:: Uso: tasto destro -> "Esegui come amministratore"

setlocal

set TASK_NAME=P1UNI_DAILY_BOOT
set PYTHON_EXE=C:\Program Files\Python313\python.exe
set SCRIPT_PATH=C:\Users\annal\Desktop\P1UNI\src\automation\master_orchestrator.py
set WORK_DIR=C:\Users\annal\Desktop\P1UNI

echo ============================================
echo   P1UNI Task Scheduler Installer
echo ============================================
echo.
echo Task name: %TASK_NAME%
echo Schedule:  Daily 07:30
echo Script:    %SCRIPT_PATH%
echo Working:   %WORK_DIR%
echo.

:: Verifica Python
if not exist "%PYTHON_EXE%" (
    echo [ERRORE] Python non trovato: %PYTHON_EXE%
    pause
    exit /b 1
)

:: Verifica script
if not exist "%SCRIPT_PATH%" (
    echo [ERRORE] Script non trovato: %SCRIPT_PATH%
    pause
    exit /b 1
)

:: Elimina task esistente (se presente)
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo Rimozione task esistente...
    schtasks /delete /tn "%TASK_NAME%" /f
)

:: Crea il nuovo task
echo Creazione task...
schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%PYTHON_EXE%\" \"%SCRIPT_PATH%\" --boot-only" ^
    /sc daily ^
    /st 07:30 ^
    /rl highest ^
    /f

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERRORE] Creazione task fallita. Esegui come Amministratore.
    pause
    exit /b 1
)

echo.
echo [OK] Task "%TASK_NAME%" creato con successo
echo.
echo Comandi utili:
echo   Verifica:  schtasks /query /tn "%TASK_NAME%"
echo   Run ora:   schtasks /run /tn "%TASK_NAME%"
echo   Elimina:   schtasks /delete /tn "%TASK_NAME%" /f
echo.
pause
endlocal
