<#
.SYNOPSIS
    Registra automaticamente tutti i task Windows necessari per l'autonomia
    di P1UNI. Esegue Register-ScheduledTask con privilegi elevati.

.DESCRIPTION
    Task registrati:

    1. P1UNI_Main            - Trading system (main.py --mode paper).
                               Trigger: AtLogOn + AtStartup (delay 1m).
                               Restart: ogni 1m, max 3 volte se crash.

    2. P1UNI_MorningCheck    - scripts/morning_check.py (pre-flight).
                               Trigger: Daily 07:55 local.

    3. P1UNI_BootSequence    - scripts/boot_sequence.py (NT8 login + APIs).
                               Trigger: Daily 08:00 local.
                               Fallback se P1UNI_Main non e' attivo.

    4. P1UNI_NightlyHarvest  - scripts/nightly_data_harvest.py.
                               Trigger: Daily 23:00 local.
                               Belt-and-suspenders: main.py gia' lo spawna
                               dopo le 22:00 UTC, ma se main.py e' down
                               questo lo esegue comunque.

.PARAMETER Mode
    paper (default) o live. Passato a main.py --mode.

.PARAMETER ProjectRoot
    Root directory del progetto P1UNI. Default: C:\Users\annal\Desktop\P1UNI

.PARAMETER PythonExe
    Path eseguibile Python. Default: C:\Program Files\Python313\pythonw.exe
    (pythonw.exe = nessuna console visibile, ideale per autostart).

.PARAMETER Uninstall
    Rimuove tutti i task P1UNI_* senza reinstallarli.

.EXAMPLE
    # Installa tutti i task in paper mode (dopo aver aperto PowerShell come admin):
    .\install_task_scheduler.ps1

.EXAMPLE
    # Installa in modalita live:
    .\install_task_scheduler.ps1 -Mode live

.EXAMPLE
    # Rimuove tutti i task:
    .\install_task_scheduler.ps1 -Uninstall

.NOTES
    DEVE essere lanciato da una sessione PowerShell con privilegi di
    amministratore (Register-ScheduledTask richiede SYSTEM/Admin).
#>

[CmdletBinding()]
param(
    [ValidateSet("paper", "live")]
    [string]$Mode = "paper",

    [string]$ProjectRoot = "C:\Users\annal\Desktop\P1UNI",

    [string]$PythonExe = "C:\Program Files\Python313\pythonw.exe",

    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Pre-flight
# ------------------------------------------------------------

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p  = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-IsAdmin)) {
    Write-Error "Questo script deve essere eseguito come Amministratore. Chiudi e riapri PowerShell con 'Esegui come amministratore'."
    exit 1
}

$TASK_PREFIX = "P1UNI_"
$TASK_NAMES  = @(
    "P1UNI_Main",
    "P1UNI_MorningCheck",
    "P1UNI_BootSequence",
    "P1UNI_NightlyHarvest"
)

# ------------------------------------------------------------
# Uninstall path
# ------------------------------------------------------------

function Remove-P1UniTasks {
    Write-Host "Rimozione task P1UNI_*..."
    foreach ($name in $TASK_NAMES) {
        $existing = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
        if ($null -ne $existing) {
            Unregister-ScheduledTask -TaskName $name -Confirm:$false
            Write-Host "  Rimosso: $name"
        } else {
            Write-Host "  Non presente: $name"
        }
    }
    Write-Host "Uninstall completato."
}

if ($Uninstall) {
    Remove-P1UniTasks
    exit 0
}

# ------------------------------------------------------------
# Validazione paths
# ------------------------------------------------------------

if (-not (Test-Path $PythonExe)) {
    Write-Error "Python non trovato: $PythonExe"
    exit 1
}

if (-not (Test-Path $ProjectRoot)) {
    Write-Error "ProjectRoot non trovata: $ProjectRoot"
    exit 1
}

$mainPy            = Join-Path $ProjectRoot "main.py"
$morningCheckPy    = Join-Path $ProjectRoot "scripts\morning_check.py"
$bootSequencePy    = Join-Path $ProjectRoot "scripts\boot_sequence.py"
$nightlyHarvestPy  = Join-Path $ProjectRoot "scripts\nightly_data_harvest.py"

foreach ($p in @($mainPy, $morningCheckPy, $bootSequencePy, $nightlyHarvestPy)) {
    if (-not (Test-Path $p)) {
        Write-Error "Script mancante: $p"
        exit 1
    }
}

Write-Host "============================================"
Write-Host "  P1UNI Task Scheduler Installer"
Write-Host "============================================"
Write-Host "Mode:         $Mode"
Write-Host "ProjectRoot:  $ProjectRoot"
Write-Host "Python:       $PythonExe"
Write-Host ""

# ------------------------------------------------------------
# Helper: registra un task (rimpiazza l'eventuale esistente)
# ------------------------------------------------------------

function Register-P1UniTask {
    param(
        [string]$Name,
        [string]$Description,
        [string]$Arguments,
        [Microsoft.Management.Infrastructure.CimInstance[]]$Triggers,
        [Microsoft.Management.Infrastructure.CimInstance]$Settings = $null
    )

    # Rimuovi il task esistente se presente
    $existing = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
    if ($null -ne $existing) {
        Unregister-ScheduledTask -TaskName $Name -Confirm:$false
        Write-Host "  Task esistente rimosso: $Name"
    }

    $action = New-ScheduledTaskAction `
        -Execute $PythonExe `
        -Argument $Arguments `
        -WorkingDirectory $ProjectRoot

    $principal = New-ScheduledTaskPrincipal `
        -UserId "$env:USERDOMAIN\$env:USERNAME" `
        -LogonType Interactive `
        -RunLevel Highest

    if ($null -eq $Settings) {
        $Settings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -ExecutionTimeLimit ([TimeSpan]::Zero)
    }

    Register-ScheduledTask `
        -TaskName $Name `
        -Description $Description `
        -Action $action `
        -Trigger $Triggers `
        -Principal $principal `
        -Settings $Settings | Out-Null

    Write-Host "  Registrato: $Name"
}

# ------------------------------------------------------------
# 1. P1UNI_Main  (trading system, AtLogOn + AtStartup)
# ------------------------------------------------------------

$mainArgs = '"' + $mainPy + '" --mode ' + $Mode + ' --config "' + (Join-Path $ProjectRoot "config\settings.yaml") + '"'
$mainTriggers = @(
    (New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"),
    (New-ScheduledTaskTrigger -AtStartup)
)
# Delay 1 min after trigger + restart on failure (max 3, every 1 min)
$mainSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -RestartCount 3 `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

# Delay di 1 minuto su ogni trigger (per dare tempo alla rete)
foreach ($t in $mainTriggers) {
    $t.Delay = "PT1M"
}

Register-P1UniTask `
    -Name "P1UNI_Main" `
    -Description "P1UNI trading system main.py (mode=$Mode)" `
    -Arguments $mainArgs `
    -Triggers $mainTriggers `
    -Settings $mainSettings

# ------------------------------------------------------------
# 2. P1UNI_MorningCheck  (daily 07:55)
# ------------------------------------------------------------

$morningArgs = '"' + $morningCheckPy + '"'
$morningTrigger = New-ScheduledTaskTrigger -Daily -At "07:55"
Register-P1UniTask `
    -Name "P1UNI_MorningCheck" `
    -Description "Pre-flight morning readiness check (GexBot/Databento/DB/models)" `
    -Arguments $morningArgs `
    -Triggers @($morningTrigger)

# ------------------------------------------------------------
# 3. P1UNI_BootSequence  (daily 08:00)
# ------------------------------------------------------------

$bootArgs = '"' + $bootSequencePy + '" --mode ' + $Mode
$bootTrigger = New-ScheduledTaskTrigger -Daily -At "08:00"
Register-P1UniTask `
    -Name "P1UNI_BootSequence" `
    -Description "Daily boot sequence: NT8 auto-login + API checks + launch P1UNI" `
    -Arguments $bootArgs `
    -Triggers @($bootTrigger)

# ------------------------------------------------------------
# 4. P1UNI_NightlyHarvest  (daily 23:00)
# ------------------------------------------------------------

$harvestArgs = '"' + $nightlyHarvestPy + '" --days 1'
$harvestTrigger = New-ScheduledTaskTrigger -Daily -At "23:00"
Register-P1UniTask `
    -Name "P1UNI_NightlyHarvest" `
    -Description "Nightly Databento data harvest (fallback, main.py gia' lo spawna)" `
    -Arguments $harvestArgs `
    -Triggers @($harvestTrigger)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

Write-Host ""
Write-Host "============================================"
Write-Host "  Registrazione completata"
Write-Host "============================================"
Write-Host ""
Write-Host "Task registrati:"
foreach ($n in $TASK_NAMES) {
    $t = Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue
    if ($null -ne $t) {
        Write-Host "  [OK] $n  (state=$($t.State))"
    } else {
        Write-Host "  [MISSING] $n"
    }
}
Write-Host ""
Write-Host "Comandi utili:"
Write-Host "  Get-ScheduledTask P1UNI_*"
Write-Host "  Start-ScheduledTask -TaskName P1UNI_Main"
Write-Host "  Stop-ScheduledTask  -TaskName P1UNI_Main"
Write-Host "  .\install_task_scheduler.ps1 -Uninstall"
Write-Host ""
