param(
    [string]$Path = "static/test-sqlite/db.sqlite"
)

$dir = Split-Path $Path
if ($dir) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

if (Get-Command sqlite3 -ErrorAction SilentlyContinue) {
    sqlite3 $Path "VACUUM;"
} else {
    if (-not (Test-Path $Path)) {
        New-Item -ItemType File -Force -Path $Path | Out-Null
    }
}

Write-Host "SQLite DB created at $Path"
