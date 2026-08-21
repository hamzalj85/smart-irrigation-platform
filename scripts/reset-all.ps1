# Full reset of the stack. Destroys every piece of local data.
$ErrorActionPreference = "Stop"
docker compose down -v
docker compose up -d
Write-Host "Stack rebuilt. Wait for the 4 '[listener] query started' lines." -ForegroundColor Green
