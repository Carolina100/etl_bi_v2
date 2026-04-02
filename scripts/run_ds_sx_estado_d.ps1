param(
    [Parameter(Mandatory = $true)]
    [int]$IdCliente,

    [Parameter(Mandatory = $true)]
    [string]$DataInicio,

    [Parameter(Mandatory = $true)]
    [string]$DataFim
)

# ============================================================================
# EXECUCAO LOCAL DA CAMADA DS
#
# ESTA ETAPA REPRESENTA:
# Oracle -> Python local -> Snowflake DS
#
# EM PRODUCAO:
# - este script tende a virar uma task no Airflow
# - a execucao deve acontecer em container / Kubernetes
# ============================================================================

$projectRoot = Join-Path $PSScriptRoot ".."
$projectRoot = [System.IO.Path]::GetFullPath($projectRoot)

. (Join-Path $PSScriptRoot "load_local_env.ps1")

[System.Environment]::SetEnvironmentVariable("PYTHONPATH", $projectRoot, 'Process')

Write-Host ""
Write-Host "Executando pipeline DS SX_ESTADO_D..."

python (Join-Path $projectRoot "src\pipelines\load_sx_estado_d.py") `
    --id_cliente $IdCliente `
    --data_inicio $DataInicio `
    --data_fim $DataFim

if ($LASTEXITCODE -ne 0) {
    throw "Falha na execucao da camada DS."
}

Write-Host "Camada DS executada com sucesso."
