# Charge .env dans la session PowerShell courante.
# Usage (note le point initial, obligatoire) :  . .\scripts\load-env.ps1
Get-Content (Join-Path (Split-Path -Parent $PSScriptRoot) ".env") | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$') {
        $value = $Matches[2].Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($Matches[1], $value, "Process")
    }
}
Write-Host "Variables de .env chargees dans la session." -ForegroundColor Green
$env:DBT_PROFILES_DIR = Join-Path (Split-Path -Parent $PSScriptRoot) "transforms\dbt"