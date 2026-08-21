# Regenerates infra/mosquitto/passwd from the values in .env.
# .env is the single source of truth; this script is idempotent.
$ErrorActionPreference = "Stop"

$root     = Split-Path -Parent $PSScriptRoot
$envFile  = Join-Path $root ".env"
$passwd   = Join-Path $root "infra\mosquitto\passwd"

if (-not (Test-Path $envFile)) { throw ".env not found. Run: Copy-Item .env.example .env" }

# Read .env into a hash table
$vars = @{}
Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$') {
        $vars[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'")
    }
}

$accounts = @(
    @{ User = $vars['MOSQUITTO_ESP32_USER'];  Pass = $vars['MOSQUITTO_ESP32_PASSWORD'] },
    @{ User = $vars['MOSQUITTO_BRIDGE_USER']; Pass = $vars['MOSQUITTO_BRIDGE_PASSWORD'] }
)

foreach ($a in $accounts) {
    if (-not $a.User) {
        throw "Missing user in .env (MOSQUITTO_*_USER)."
    }
    if (-not $a.Pass -or $a.Pass -eq 'changeme') {
        throw @"
The password of account '$($a.User)' is still 'changeme' in .env.

Generate one and paste it into .env before running this again:
  -join ((48..57)+(65..90)+(97..122) | Get-Random -Count 24 | % {[char]`$_})
"@
    }
}

Remove-Item $passwd -Force -ErrorAction SilentlyContinue
New-Item -ItemType File $passwd -Force | Out-Null

$mount = "$($root -replace '\\', '/')/infra/mosquitto:/mosquitto/config"

# -c creates the file (overwriting it); the second call appends.
docker run --rm -v $mount eclipse-mosquitto:2.0 `
    mosquitto_passwd -b -c /mosquitto/config/passwd $accounts[0].User $accounts[0].Pass
docker run --rm -v $mount eclipse-mosquitto:2.0 `
    mosquitto_passwd -b /mosquitto/config/passwd $accounts[1].User $accounts[1].Pass

Write-Host "passwd regenerated for: $($accounts.User -join ', ')" -ForegroundColor Green
