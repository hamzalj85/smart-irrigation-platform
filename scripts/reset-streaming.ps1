# Resets the state of the streaming pipeline.
# The checkpoint and _spark_metadata must be cleared TOGETHER: they describe
# one and the same transactional state.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

docker compose stop spark-streaming
docker compose rm -f spark-streaming          # releases the volume
docker volume rm smart-irrigation-platform_spark_checkpoints 2>$null
Remove-Item -Recurse -Force (Join-Path $root "data") -ErrorAction SilentlyContinue

Write-Host "Streaming state reset." -ForegroundColor Green
