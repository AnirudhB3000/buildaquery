#!/usr/bin/env bash
set -euo pipefail

path="${1:-static/test-duckdb}"
mkdir -p "$path"
echo "DuckDB test directory prepared at $path"
