#!/usr/bin/env bash
set -euo pipefail

path="${1:-static/test-sqlite/db.sqlite}"
mkdir -p "$(dirname "$path")"

if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$path" "VACUUM;"
else
  if [ ! -f "$path" ]; then
    touch "$path"
  fi
fi

echo "SQLite DB created at $path"
