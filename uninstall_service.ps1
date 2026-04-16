# ── LangNetmon Agent — NSSM service uninstaller ──────────────────────────
# Stops and removes the LangNetmonAgent service. Leaves logs and exe in place.
#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"

$InstallDir = "C:\ProgramData\LangNetmon"
$NssmExe    = Join-Path $InstallDir "nssm.exe"
$SvcName    = "LangNetmonAgent"

if (-not (Test-Path $NssmExe)) {
    Write-Host "[LangNetmon] NSSM not found at $NssmExe — falling back to sc.exe"
    & sc.exe stop   $SvcName 2>$null | Out-Null
    & sc.exe delete $SvcName          | Out-Null
    Write-Host "[LangNetmon] Service removed."
    exit 0
}

Write-Host "[LangNetmon] Stopping service '$SvcName'..."
& $NssmExe stop   $SvcName 2>$null | Out-Null

Write-Host "[LangNetmon] Removing service..."
& $NssmExe remove $SvcName confirm | Out-Null

Write-Host "[LangNetmon] Uninstall complete. (exe and logs kept at $InstallDir)"
