param(
    [Parameter(Mandatory = $true)]
    [int]$IdCliente,

    [Parameter(Mandatory = $true)]
    [string]$DataInicio,

    [Parameter(Mandatory = $true)]
    [string]$DataFim
)

# ============================================================================
# EXECUCAO LOCAL DO FLUXO COMPLETO
#
# ESTA ETAPA REPRESENTA O DESENHO MAIS PROXIMO DE PRODUCAO:
# Oracle -> Python -> Snowflake DS -> dbt -> Snowflake DW
#
# EM PRODUCAO:
# - este fluxo deve ser orquestrado
# - Airflow pode chamar primeiro a task DS e depois a task DW
# - a dependencia entre etapas deve ser sequencial
# ============================================================================

Write-Host "Iniciando fluxo completo SX_ESTADO_D..."

& (Join-Path $PSScriptRoot "run_ds_sx_estado_d.ps1") `
    -IdCliente $IdCliente `
    -DataInicio $DataInicio `
    -DataFim $DataFim

& (Join-Path $PSScriptRoot "run_dw_sx_estado_d.ps1")

Write-Host "Fluxo completo finalizado com sucesso."
