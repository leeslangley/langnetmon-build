# ── LangNetmon Agent — NSSM service installer ────────────────────────────
# Installs NetMonAgent.exe as a Windows service (headless, --service mode).
# Must be run as Administrator.
#
# Behaviour:
#   - Downloads NSSM 2.24 if missing (to C:\ProgramData\LangNetmon\nssm.exe)
#   - Installs service "LangNetmonAgent" pointing at C:\ProgramData\LangNetmon\NetMonAgent.exe
#   - Auto-start, delayed-auto-start, restart on failure after 10s
#   - Logs stdout/stderr to C:\ProgramData\LangNetmon\logs\
#   - Starts the service
#
# If NetMonAgent.exe isn't already at the install path, copy it from the
# current directory (so admins can just drop the exe next to this script).

#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"

$InstallDir = "C:\ProgramData\LangNetmon"
$LogDir     = Join-Path $InstallDir "logs"
$AgentExe   = Join-Path $InstallDir "NetMonAgent.exe"
$NssmExe    = Join-Path $InstallDir "nssm.exe"
$SvcName    = "LangNetmonAgent"
$NssmUrl    = "https://nssm.cc/release/nssm-2.24.zip"

Write-Host "[LangNetmon] Preparing install dir: $InstallDir"
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir     -Force | Out-Null

# ── Stage the agent exe ───────────────────────────────────────────────────
if (-not (Test-Path $AgentExe)) {
    $LocalExe = Join-Path (Get-Location) "NetMonAgent.exe"
    if (Test-Path $LocalExe) {
        Write-Host "[LangNetmon] Copying NetMonAgent.exe -> $AgentExe"
        Copy-Item $LocalExe $AgentExe -Force
    } else {
        throw "NetMonAgent.exe not found at $AgentExe or in current directory. Build it first, then re-run."
    }
}

# ── Fetch NSSM if not present ─────────────────────────────────────────────
if (-not (Test-Path $NssmExe)) {
    Write-Host "[LangNetmon] NSSM not found — downloading..."
    $Zip = Join-Path $env:TEMP "nssm-2.24.zip"
    Invoke-WebRequest -Uri $NssmUrl -OutFile $Zip -UseBasicParsing
    $ExtractDir = Join-Path $env:TEMP "nssm-extract"
    if (Test-Path $ExtractDir) { Remove-Item $ExtractDir -Recurse -Force }
    Expand-Archive -Path $Zip -DestinationPath $ExtractDir -Force

    $Arch = if ([Environment]::Is64BitOperatingSystem) { "win64" } else { "win32" }
    $Src = Get-ChildItem -Path $ExtractDir -Recurse -Filter "nssm.exe" |
           Where-Object { $_.FullName -match "\\$Arch\\" } |
           Select-Object -First 1

    if (-not $Src) { throw "Failed to locate nssm.exe in downloaded archive" }
    Copy-Item $Src.FullName $NssmExe -Force
    Write-Host "[LangNetmon] NSSM installed at $NssmExe"
}

# ── Remove any existing service first ─────────────────────────────────────
$existing = & $NssmExe status $SvcName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[LangNetmon] Existing service found — stopping and removing."
    & $NssmExe stop    $SvcName 2>$null | Out-Null
    & $NssmExe remove  $SvcName confirm | Out-Null
}

# ── Install + configure ───────────────────────────────────────────────────
Write-Host "[LangNetmon] Installing service '$SvcName'..."
& $NssmExe install $SvcName $AgentExe "--service" | Out-Null

& $NssmExe set $SvcName DisplayName          "LangNetmon Agent"         | Out-Null
& $NssmExe set $SvcName Description           "Langley Inc network monitoring agent (LangNetmon v2.0.0)" | Out-Null
& $NssmExe set $SvcName Start                 SERVICE_AUTO_START        | Out-Null
& $NssmExe set $SvcName DelayedAutoStart      1                          | Out-Null
& $NssmExe set $SvcName AppStdout             (Join-Path $LogDir "agent.out.log") | Out-Null
& $NssmExe set $SvcName AppStderr             (Join-Path $LogDir "agent.err.log") | Out-Null
& $NssmExe set $SvcName AppRotateFiles        1                          | Out-Null
& $NssmExe set $SvcName AppRotateBytes        10485760                   | Out-Null

# Restart on failure: retry after 10s, up to 3 times, reset counter after 24h
& $NssmExe set $SvcName AppExit Default       Restart                    | Out-Null
& $NssmExe set $SvcName AppRestartDelay       10000                      | Out-Null
& $NssmExe set $SvcName AppThrottle           5000                       | Out-Null

Write-Host "[LangNetmon] Starting service..."
& $NssmExe start $SvcName

Start-Sleep -Seconds 3
& $NssmExe status $SvcName
Write-Host "[LangNetmon] Install complete. Logs: $LogDir"
