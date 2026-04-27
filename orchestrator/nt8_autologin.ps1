# ============================================================
# nt8_autologin.ps1 — auto-login NinjaTrader 8
# Legge credenziali da P1UNI/.env (NT8_USER, NT8_PASSWORD).
# Chiamato in background da start_nt8_with_login.bat.
# Non fatale: exit 0 se la finestra di login non appare entro
# MaxWaitSeconds (NT8 potrebbe essere gia' loggato).
# ============================================================

param(
    [int]$MaxWaitSeconds = 120,
    [int]$PostTypeDelay  = 500
)

$ErrorActionPreference = 'Continue'

$EnvPath = 'C:\Users\annal\Desktop\P1UNI\.env'
if (-not (Test-Path $EnvPath)) {
    Write-Host ("[ERRORE] .env non trovato: " + $EnvPath) -ForegroundColor Red
    exit 1
}

function Read-EnvVar {
    param([string]$file, [string]$key)
    foreach ($line in Get-Content $file -Encoding UTF8) {
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith('#') -or -not ($trimmed -match '=')) { continue }
        $eq = $trimmed.IndexOf('=')
        $k = $trimmed.Substring(0, $eq).Trim()
        if ($k -eq $key) {
            $v = $trimmed.Substring($eq + 1).Trim()
            $v = $v.Trim('"').Trim("'")
            return $v
        }
    }
    return $null
}

$nt8User = Read-EnvVar $EnvPath 'NT8_USER'
$nt8Pass = Read-EnvVar $EnvPath 'NT8_PASSWORD'

if ([string]::IsNullOrWhiteSpace($nt8User) -or [string]::IsNullOrWhiteSpace($nt8Pass)) {
    Write-Host '[ERRORE] NT8_USER o NT8_PASSWORD mancanti in .env' -ForegroundColor Red
    exit 1
}

Write-Host '=== NT8 AUTO-LOGIN ===' -ForegroundColor Cyan
Write-Host ('  User: ' + $nt8User) -ForegroundColor White
Write-Host ('  Attesa finestra login (max ' + $MaxWaitSeconds + 's)...') -ForegroundColor Gray

Add-Type -AssemblyName System.Windows.Forms

Add-Type @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public class Win32 {
    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")]
    public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
    [DllImport("user32.dll", CharSet = CharSet.Auto)]
    public static extern int GetWindowText(IntPtr hWnd, StringBuilder lpString, int nMaxCount);
    public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);
    [DllImport("user32.dll")]
    public static extern bool EnumWindows(EnumWindowsProc lpEnumFunc, IntPtr lParam);
    [DllImport("user32.dll")]
    public static extern bool IsWindowVisible(IntPtr hWnd);
}
"@

function Find-NT8LoginWindow {
    $script:foundHwnd = $null
    $callback = [Win32+EnumWindowsProc]{
        param($hWnd, $lParam)
        if ([Win32]::IsWindowVisible($hWnd)) {
            $sb = New-Object System.Text.StringBuilder 256
            [Win32]::GetWindowText($hWnd, $sb, 256) | Out-Null
            $title = $sb.ToString()
            if ($title -match 'NinjaTrader' -and ($title -match 'Login|Log In|Sign In|Accedi' -or $title -eq 'NinjaTrader 8')) {
                $script:foundHwnd = $hWnd
            }
        }
        return $true
    }
    [Win32]::EnumWindows($callback, [IntPtr]::Zero) | Out-Null
    return $script:foundHwnd
}

$startTime = Get-Date
$loginHwnd = $null

while ($true) {
    $elapsed = ((Get-Date) - $startTime).TotalSeconds
    if ($elapsed -gt $MaxWaitSeconds) {
        Write-Host ('[TIMEOUT] Finestra login non trovata dopo ' + $MaxWaitSeconds + 's (NT8 forse gia loggato)') -ForegroundColor Yellow
        exit 0
    }
    $loginHwnd = Find-NT8LoginWindow
    if ($loginHwnd -and $loginHwnd -ne [IntPtr]::Zero) {
        Write-Host '  Finestra login trovata!' -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 2
}

Start-Sleep -Milliseconds 1000
[Win32]::ShowWindow($loginHwnd, 9) | Out-Null
[Win32]::SetForegroundWindow($loginHwnd) | Out-Null
Start-Sleep -Milliseconds 500

[System.Windows.Forms.SendKeys]::SendWait('^a')
Start-Sleep -Milliseconds 200
[System.Windows.Forms.SendKeys]::SendWait($nt8User)
Start-Sleep -Milliseconds $PostTypeDelay

[System.Windows.Forms.SendKeys]::SendWait('{TAB}')
Start-Sleep -Milliseconds 300

[System.Windows.Forms.SendKeys]::SendWait('^a')
Start-Sleep -Milliseconds 200
[System.Windows.Forms.SendKeys]::SendWait($nt8Pass)
Start-Sleep -Milliseconds $PostTypeDelay

[System.Windows.Forms.SendKeys]::SendWait('{ENTER}')

Write-Host '  Login inviato' -ForegroundColor Green
exit 0
