# Remet à zéro l'état du pipeline de streaming.
# Checkpoint et _spark_metadata doivent être effacés ENSEMBLE : ils décrivent
# le même état transactionnel.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

docker compose stop spark-streaming
docker compose rm -f spark-streaming          # libère le volume
docker volume rm smart-irrigation-platform_spark_checkpoints 2>$null
Remove-Item -Recurse -Force (Join-Path $root "data") -ErrorAction SilentlyContinue

Write-Host "État de streaming réinitialisé." -ForegroundColor Green