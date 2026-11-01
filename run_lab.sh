#!/usr/bin/env bash
# ==============================================================================
# 1-Command Benchmark Runner for Lakehouse CDC Compaction Lab
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Initializing Lakehouse CDC Small-File Benchmark ==="
if command -v python3 &>/dev/null; then
    python3 "${SCRIPT_DIR}/benchmark_cdc.py"
elif command -v python &>/dev/null; then
    python "${SCRIPT_DIR}/benchmark_cdc.py"
elif command -v py &>/dev/null; then
    py "${SCRIPT_DIR}/benchmark_cdc.py"
else
    echo "Error: Python runtime required." >&2
    exit 1
fi
