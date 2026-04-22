$ErrorActionPreference = "Stop"

$repoUrl = if ($env:SYNCREMNAWAVE_REPO) { $env:SYNCREMNAWAVE_REPO } else { "https://github.com/LaRsonOFFai/SyncRemnawave.git" }
$installRoot = Join-Path $env:LOCALAPPDATA "SyncRemnawave"
$venvDir = Join-Path $installRoot "venv"
$binDir = Join-Path $env:LOCALAPPDATA "Microsoft\\WindowsApps"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python is required"
}

New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
python -m venv $venvDir
& (Join-Path $venvDir "Scripts\\python.exe") -m pip install --upgrade pip
& (Join-Path $venvDir "Scripts\\python.exe") -m pip install --upgrade --force-reinstall --no-cache-dir "git+$repoUrl"

$shimPath = Join-Path $binDir "sync-remnawave.cmd"
$shim = "@echo off`r`n""$venvDir\\Scripts\\sync-remnawave.exe"" %*`r`n"
Set-Content -Path $shimPath -Value $shim -Encoding ASCII

Write-Host ""
Write-Host "SyncRemnawave installed."
Write-Host "Starting setup wizard..."
& (Join-Path $venvDir "Scripts\\sync-remnawave.exe") init
