param(
    [string]$Path = "static/test-duckdb"
)

New-Item -ItemType Directory -Force -Path $Path | Out-Null
Write-Host "DuckDB test directory prepared at $Path"
