# =============================================================================
# start_evidence.ps1 — Levanta el servidor Evidence.dev en http://localhost:3333
# =============================================================================
# Uso:
#   .\scripts\start_evidence.ps1
#
# Requiere haber corrido antes (una sola vez):
#   cd dashboards; npm install
# =============================================================================

$dashboards = Join-Path $PSScriptRoot "..\dashboards"
Set-Location $dashboards
# Workaround: EVIDENCE_DISABLE_INCLUDE evita el crash de vite-plugin-svelte:optimize-svelte
# en Node.js 22 con Evidence v40 (bug conocido)
$env:EVIDENCE_DISABLE_INCLUDE = "true"
Write-Host "Iniciando Evidence.dev en http://localhost:3333 ..." -ForegroundColor Cyan
node .\node_modules\@evidence-dev\evidence\cli.js dev --port 3333
