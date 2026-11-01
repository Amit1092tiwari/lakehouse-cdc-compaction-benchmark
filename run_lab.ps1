# ==============================================================================
# 1-Command Benchmark Runner for Lakehouse CDC Compaction Lab (Windows PowerShell)
# ==============================================================================
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Write-Host "=== Initializing Lakehouse CDC Small-File Benchmark ===" -ForegroundColor Cyan

if (Get-Command py -ErrorAction SilentlyContinue) {
    py "$scriptDir\benchmark_cdc.py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    python "$scriptDir\benchmark_cdc.py"
} else {
    Write-Error "Python runtime not found in PATH."
}
