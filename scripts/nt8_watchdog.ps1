# nt8_watchdog.ps1 — Watchdog per NinjaTrader 8
# Controlla se NT8 e' attivo, se no lo riavvia.
# Schedulare con: schtasks /create /tn "NT8_Watchdog" /tr "powershell -ExecutionPolicy Bypass -File C:\Users\annal\Desktop\P1UNI\scripts\nt8_watchdog.ps1" /sc minute /mo 1 /ru SYSTEM

$logFile = "C:\Users\annal\Desktop\P1UNI\logs\nt8_watchdog.log"
$nt8Exe  = "C:\NinjaTrader 8\bin\NinjaTrader.exe"
$ts      = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

function Write-Log($msg) {
    "$ts | $msg" | Out-File -Append -FilePath $logFile -Encoding UTF8
}

try {
    $proc = Get-Process -Name "NinjaTrader" -ErrorAction SilentlyContinue

    if ($proc) {
        # NT8 attivo — niente da fare
        Write-Log "OK | NinjaTrader running (PID $($proc.Id))"
    }
    else {
        Write-Log "ALERT | NinjaTrader NOT running — starting..."

        if (Test-Path $nt8Exe) {
            Start-Process -FilePath $nt8Exe
            Write-Log "ACTION | Started NinjaTrader from $nt8Exe"
        }
        else {
            Write-Log "ERROR | NinjaTrader exe not found at $nt8Exe"
        }
    }
}
catch {
    Write-Log "ERROR | Exception: $_"
}
