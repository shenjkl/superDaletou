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

$venvVersion = & $venvPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
if ($null -eq $venvVersion) { $venvVersion = '不可用' } else { $venvVersion = $venvVersion.Trim() }
if ($LASTEXITCODE -ne 0 -or $venvVersion -ne '3.12') {
    throw "项目 .venv 必须使用 Python 3.12（检测到 $venvVersion）。请先自行备份并移走或删除 .venv，再运行 .\setup-local.ps1。"
}

Push-Location $projectRoot
try {
    & $venvPython -m dlt.app --db $Database --port $Port
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
