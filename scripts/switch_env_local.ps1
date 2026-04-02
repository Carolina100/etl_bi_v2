param()

$projectRoot = Join-Path $PSScriptRoot ".."
$projectRoot = [System.IO.Path]::GetFullPath($projectRoot)
$sourceFile = Join-Path $projectRoot ".env.local"
$targetFile = Join-Path $projectRoot ".env"

if (-not (Test-Path $sourceFile)) {
    throw "Arquivo nao encontrado: $sourceFile"
}

Copy-Item -LiteralPath $sourceFile -Destination $targetFile -Force
Write-Host ".env atualizado a partir de .env.local"
