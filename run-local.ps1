param(
    [string]$Database = 'runtime/research.sqlite3',
    [ValidateRange(1, 65535)]
    [int]$Port = 8766
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = $PSScriptRoot
$venvPython = Join-Path $projectRoot '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    throw '尚未准备项目环境。请先在项目目录运行 .\setup-local.ps1。'
}

Push-Location $projectRoot
try {
    & $venvPython -m dlt.app --db $Database --port $Port
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
