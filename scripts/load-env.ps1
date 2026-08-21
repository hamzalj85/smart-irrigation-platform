# Loads .env into the current PowerShell session.
# Usage (note the leading dot, it is required):  . .\scripts\load-env.ps1
Get-Content (Join-Path (Split-Path -Parent $PSScriptRoot) ".env") | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$') {
        $value = $Matches[2].Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($Matches[1], $value, "Process")
    }
}
Write-Host "Variables from .env loaded into the session." -ForegroundColor Green
$env:DBT_PROFILES_DIR = Join-Path (Split-Path -Parent $PSScriptRoot) "transforms\dbt"
