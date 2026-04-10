# ============================================================================
# SCRIPT LOCAL PARA CARREGAR O .env NO POWERSHELL
#
# USO:
#   1. Abra o PowerShell na raiz do projeto
#   2. Rode:
#        . .\scripts\load_local_env.ps1
#   3. Depois execute os comandos Python ou dbt na mesma sessao
#
# IMPORTANTE:
# - Este script e apenas para ambiente LOCAL
# - Ele existe porque o dbt nao le o arquivo .env automaticamente
# - Este script existe principalmente para dbt e ferramentas locais
#
# EM PRODUCAO:
# - NAO use este script
# - NAO dependa de arquivo .env
# - Use variaveis de ambiente injetadas por Kubernetes / Airflow / secret manager
# - Quando o projeto estiver rodando orquestrado, este script pode ser ignorado
# ============================================================================

$envFile = Join-Path $PSScriptRoot "..\.env"
$envFile = [System.IO.Path]::GetFullPath($envFile)

if (-not (Test-Path $envFile)) {
    Write-Error "Arquivo .env nao encontrado em: $envFile"
    return
}

Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [System.Environment]::SetEnvironmentVariable($name, $value, 'Process')
        Write-Host "Variavel carregada: $name"
    }
}

$dbtProfilesDir = Join-Path $PSScriptRoot "..\dbt\solix_dbt"
$dbtProfilesDir = [System.IO.Path]::GetFullPath($dbtProfilesDir)
[System.Environment]::SetEnvironmentVariable("DBT_PROFILES_DIR", $dbtProfilesDir, 'Process')

Write-Host ""
Write-Host "Variaveis do .env carregadas na sessao atual do PowerShell."
Write-Host "DBT_PROFILES_DIR configurado para: $dbtProfilesDir"
Write-Host "Agora voce pode rodar dbt ou comandos locais nesta mesma janela."
