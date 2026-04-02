param()

# ============================================================================
# EXECUCAO LOCAL DA CAMADA DW
#
# ESTA ETAPA REPRESENTA:
# Snowflake DS -> dbt local -> Snowflake DW
#
# EM PRODUCAO:
# - este script tende a virar uma task no Airflow
# - a execucao do dbt pode acontecer em container ou runner dedicado
# ============================================================================

$projectRoot = Join-Path $PSScriptRoot ".."
$projectRoot = [System.IO.Path]::GetFullPath($projectRoot)
$dbtProjectDir = Join-Path $projectRoot "dbt\solix_dbt"

. (Join-Path $PSScriptRoot "load_local_env.ps1")

Push-Location $dbtProjectDir
try {
    Write-Host ""
    Write-Host "Executando dbt para SX_ESTADO_D..."
    dbt build --select stg_ds__sx_estado_d dim_sx_estado_d

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na execucao da camada DW."
    }
}
finally {
    Pop-Location
}

Write-Host "Camada DW executada com sucesso."
