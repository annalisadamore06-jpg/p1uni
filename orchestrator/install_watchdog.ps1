# Register P1UNI unified watchdog in Windows Task Scheduler.
# Fires at logon AND at system startup. Restarts on failure.
# Replaces the old P1_Orchestrator task.

$ErrorActionPreference = "Stop"
$TaskName = "P1UNI_Watchdog"
$Bat      = "C:\Users\annal\Desktop\P1UNI\orchestrator\watchdog.bat"

if (!(Test-Path $Bat)) {
    throw "missing $Bat"
}

# Remove old entries (both legacy and current)
foreach ($old in @("P1_Orchestrator", $TaskName)) {
    if (Get-ScheduledTask -TaskName $old -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $old -Confirm:$false
        Write-Host "Removed old task: $old"
    }
}

$Action    = New-ScheduledTaskAction -Execute $Bat -WorkingDirectory (Split-Path $Bat)
$T1        = New-ScheduledTaskTrigger -AtLogOn
$T2        = New-ScheduledTaskTrigger -AtStartup
$Settings  = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
             -StartWhenAvailable -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) `
             -ExecutionTimeLimit (New-TimeSpan -Days 30)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger @($T1,$T2) `
    -Settings $Settings -Principal $Principal `
    -Description "P1UNI unified watchdog: 8-service supervisor with dedup, auto-restart, operating-hours awareness"

Write-Host "Registered $TaskName - starts at logon + boot."
Write-Host "To run now:  Start-ScheduledTask -TaskName $TaskName"
Write-Host ("To stop:     Unregister-ScheduledTask -TaskName " + $TaskName + " -Confirm:" + '$false')
