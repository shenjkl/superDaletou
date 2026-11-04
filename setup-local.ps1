Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = $PSScriptRoot
$pythonEnvironment = Join-Path $projectRoot '.venv'
$venvPython = Join-Path $pythonEnvironment 'Scripts\python.exe'
$requirements = Join-Path $projectRoot 'requirements.txt'

Push-Location $projectRoot
try {
    if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
        throw '未找到 Windows Python Launcher（py）。请先安装 Python 3.12，并启用 Python Launcher。'
    }

    & py -3.12 -c "import sys; print(sys.version.split()[0])"
    if ($LASTEXITCODE -ne 0) {
        throw '未找到可用的 Python 3.12。请安装 Python 3.12 后重新运行本脚本。'
    }

    if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
        & py -3.12 -m venv $pythonEnvironment
        if ($LASTEXITCODE -ne 0) {
            throw '创建项目虚拟环境失败。'
        }
    }

    & $venvPython -m pip install -r $requirements
    if ($LASTEXITCODE -ne 0) {
        throw '安装项目依赖失败。请检查网络和 Python 包索引配置后重试。'
    }

    Write-Host '本机运行环境已准备完成。接下来运行 .\run-local.ps1 启动服务。' -ForegroundColor Green
}
finally {
    Pop-Location
}
