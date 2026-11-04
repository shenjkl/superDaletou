Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = $PSScriptRoot
$pythonEnvironment = Join-Path $projectRoot '.venv'
$venvPython = Join-Path $pythonEnvironment 'Scripts\python.exe'
$requirements = Join-Path $projectRoot 'requirements.txt'

Push-Location $projectRoot
try {
    $basePython = $null
    $basePythonPrefix = @()
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3.12 -c "import sys; print(sys.version.split()[0])" *> $null
        if ($LASTEXITCODE -eq 0) {
            $basePython = 'py'
            $basePythonPrefix = @('-3.12')
        }
    }

    if (-not $basePython) {
        $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
        if ($pythonCommand) {
            $version = & $pythonCommand.Source -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
            if ($LASTEXITCODE -eq 0 -and $version.Trim() -eq '3.12') {
                $basePython = $pythonCommand.Source
            }
        }
    }

    if (-not $basePython) {
        throw '未找到 Python 3.12。请安装 Python 3.12；脚本会优先使用 py -3.12，也可使用 PATH 中的 python 命令。'
    }

    if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
        $venvArguments = @($basePythonPrefix) + @('-m', 'venv', $pythonEnvironment)
        & $basePython @venvArguments
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
