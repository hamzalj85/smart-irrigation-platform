# Remet TOUT le pipeline à zéro : topics Kafka, état de streaming, données,
# et journaux des conteneurs. À n'utiliser qu'en développement.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot

Write-Host "1/4  Arret de la stack..." -ForegroundColor Cyan
docker compose down | Out-Null

Write-Host "2/4  Suppression des topics Kafka..." -ForegroundColor Cyan
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:19092 --delete --topic irrigation.telemetry 2>$null
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:19092 --delete --topic irrigation.telemetry.dlq 2>$null
Start-Sleep -Seconds 3
docker compose up -d --force-recreate kafka-init | Out-Null
Start-Sleep -Seconds 5

Write-Host "3/4  Suppression du checkpoint et des donnees..." -ForegroundColor Cyan
docker compose exec -T minio-init sh -c 'mc rm --recursive --force local/$MINIO_BUCKET' 2>$null
docker volume rm smart-irrigation-platform_spark_checkpoints 2>&1 | Out-Null
if (docker volume ls -q -f name=smart-irrigation-platform_spark_checkpoints) {
    throw "Volume de checkpoint toujours present : arrete la stack avant de relancer."
}
Remove-Item -Recurse -Force (Join-Path $root "data") -ErrorAction SilentlyContinue

Write-Host "4/4  Redemarrage..." -ForegroundColor Cyan
docker compose up -d bridge spark-streaming | Out-Null

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:19092 --list
Write-Host "`nPret. Attends '[listener] query started' avant de lancer le simulateur." -ForegroundColor Green