# Remise a zero totale de la stack. Detruit toutes les donnees locales.
$ErrorActionPreference = "Stop"
docker compose down -v
docker compose up -d
Write-Host "Stack reconstruite. Attends les 4 '[listener] query started'." -ForegroundColor Green