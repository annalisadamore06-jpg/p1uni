# Register orchestrator in Windows Task Scheduler.
# Runs at logon AND at system startup (whichever comes first) and restarts on failure.
# Requires admin.

$ErrorActionPreference = "Stop"
$TaskName = "P1_Orchestrator"
$Bat = "C:\Users\annal\Desktop\P1UNI\orchestrator\run_orchestrator.bat"

if (!(Test-Path $Bat)) {
    throw "missing $Bat"
}

# Remove existing
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed old task"
}

$Action  = New-ScheduledTaskAction -Execute $Bat -WorkingDirectory (Split-Path $Bat)
$T1      = New-ScheduledTaskTrigger -AtLogOn
$T2      = New-ScheduledTaskTrigger -AtStartup
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
            -StartWhenAvailable -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) `
            -ExecutionTimeLimit (New-TimeSpan -Days 30)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger @($T1,$T2) `
    -Settings $Settings -Principal $Principal `
    -Description "P1 umbrella orchestrator: dedup + relaunch of TWS/collectors/bridges/main"

Write-Host "Registered $TaskName — starts at logon + boot."
Write-Host "To run now:  Start-ScheduledTask -TaskName $TaskName"
Write-Host "To stop:     Unregister-ScheduledTask -TaskName $TaskName -Confirm:`$false"
