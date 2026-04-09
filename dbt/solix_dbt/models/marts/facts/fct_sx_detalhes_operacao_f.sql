{#
  MODELO FATO DW

  O QUE ESTE MODELO FAZ:
  - le o staging da fato no DS
  - resolve surrogate keys das dimensoes disponiveis no DW
  - mantem uma linha por grão composto
  - processa incrementalmente pelo BI_UPDATED_AT
  - propaga FG_STATUS como estado mutável do mesmo grão

  REGRA DE NEGOCIO:
  - A = registro vigente
  - I = o mesmo grão retornou da fonte como desativado
  - FG_STATUS nao faz parte da chave, porque ele atualiza a mesma linha

  TRATAMENTO DE ORFAO:
  - quando a dimensao nao for encontrada no DW, a fato recebe SK = -1
  - isso preserva a linha da fato e evita perda de dado por ausencia de cadastro
#}

{% set MODEL_ALIAS = 'SX_DETALHES_OPERACAO_F' %}
{% set STAGING_MODEL_NAME = 'stg_ds__sx_detalhes_operacao_f' %}
{% set NATURAL_KEY_COLUMNS = [
    'ID_CLIENTE',
    'DT_DIA_FECHAMENTO',
    'DATA',
    'HORA',
    'SK_EQUIPAMENTO',
    'SK_OPERACAO',
    'SK_FAZENDA',
    'SK_ESTADO',
    'SK_ORDEM_SERVICO',
    'SK_FRENTE',
    'SK_PROCESSO',
    'CD_MISSAO'
] %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key='SK_DETALHE_OPERACAO',
    on_schema_change='sync_all_columns',
    tags=['dw', 'fact', 'sx_detalhes_operacao_f']
) }}

with staged_source as (
    select
        ID_CLIENTE,
        DT_DIA_FECHAMENTO,
        DATA,
        HORA,
        CD_EQUIPAMENTO,
        CD_GRUPO_EQUIPAMENTO,
        CD_OPERACAO,
        CD_OPERACAO_CB,
        CD_ORDEM_SERVICO,
        CD_UNIDADE,
        CD_REGIONAL,
        CD_CORPORATIVO,
        CD_FAZENDA,
        CD_ZONA,
        CD_TALHAO,
        CD_MISSAO,
        CD_ESTADO,
        VL_TEMPO_SEGUNDOS,
        VL_VELOCIDADE_MEDIA,
        VL_AREA_OPERACIONAL_M2,
        FG_STATUS,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT
    from {{ ref(STAGING_MODEL_NAME) }}
    {% if is_incremental() %}
    where BI_UPDATED_AT >= (
        select timestampadd(minute, -30, coalesce(max(BI_UPDATED_AT), '1900-01-01'::timestamp_ntz))
        from {{ this }}
    )
    {% endif %}
),

dim_equipamento as (
    select
        ID_CLIENTE,
        CD_EQUIPAMENTO,
        SK_EQUIPAMENTO
    from {{ ref('dim_sx_equipamento_d') }}
),

dim_operacao as (
    select
        ID_CLIENTE,
        CD_OPERACAO,
        SK_OPERACAO
    from {{ ref('dim_sx_operacao_d') }}
),

dim_fazenda as (
    select
        ID_CLIENTE,
        CD_FAZENDA,
        CD_ZONA,
        CD_TALHAO,
        SK_FAZENDA
    from {{ ref('dim_sx_fazenda_d') }}
),

dim_estado as (
    select
        ID_CLIENTE,
        CD_ESTADO,
        SK_ESTADO
    from {{ ref('dim_sx_estado_d') }}
),

dim_ordem_servico as (
    select
        ID_CLIENTE,
        CD_ORDEM_SERVICO,
        SK_ORDEM_SERVICO
    from {{ ref('dim_sx_ordem_servico_d') }}
),

dim_frente as (
    select
        ID_CLIENTE,
        CD_CORPORATIVO,
        CD_REGIONAL,
        CD_UNIDADE,
        CD_FRENTE, --- CD_GRUPO_EQUIPAMENTO é a CD_FRENTE na dimensão
        SK_FRENTE
    from {{ ref('dim_sx_frente_d') }}
),

dim_processo as (
    select
        ID_CLIENTE,
        CD_OPERACAO,
        SK_OPERACAO
    from {{ ref('dim_sx_operacao_d') }}
),

fact_rows as (
    select
        staged_source.ID_CLIENTE,
        staged_source.DT_DIA_FECHAMENTO,
        staged_source.DATA,
        staged_source.HORA,
        coalesce(dim_equipamento.SK_EQUIPAMENTO, -1) as SK_EQUIPAMENTO,
        coalesce(dim_operacao.SK_OPERACAO, -1) as SK_OPERACAO,
        coalesce(dim_fazenda.SK_FAZENDA, -1) as SK_FAZENDA,
        coalesce(dim_estado.SK_ESTADO, -1) as SK_ESTADO,
        coalesce(dim_ordem_servico.SK_ORDEM_SERVICO, -1) as SK_ORDEM_SERVICO,
        coalesce(dim_frente.SK_FRENTE, -1) as SK_FRENTE,
        coalesce(dim_processo.SK_OPERACAO, -1) as SK_PROCESSO,
        staged_source.CD_MISSAO,
        staged_source.VL_TEMPO_SEGUNDOS,
        staged_source.VL_VELOCIDADE_MEDIA,
        staged_source.VL_AREA_OPERACIONAL_M2,
        staged_source.FG_STATUS,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
    left join dim_equipamento
        on staged_source.ID_CLIENTE = dim_equipamento.ID_CLIENTE
       and staged_source.CD_EQUIPAMENTO = dim_equipamento.CD_EQUIPAMENTO
    left join dim_operacao
        on staged_source.ID_CLIENTE = dim_operacao.ID_CLIENTE
       and staged_source.CD_OPERACAO = dim_operacao.CD_OPERACAO
    left join dim_fazenda
        on staged_source.ID_CLIENTE = dim_fazenda.ID_CLIENTE
       and staged_source.CD_FAZENDA = dim_fazenda.CD_FAZENDA
       and staged_source.CD_ZONA = dim_fazenda.CD_ZONA
       and staged_source.CD_TALHAO = dim_fazenda.CD_TALHAO
    left join dim_estado
        on staged_source.ID_CLIENTE = dim_estado.ID_CLIENTE
       and staged_source.CD_ESTADO = dim_estado.CD_ESTADO
    left join dim_ordem_servico
        on staged_source.ID_CLIENTE = dim_ordem_servico.ID_CLIENTE
       and staged_source.CD_ORDEM_SERVICO = dim_ordem_servico.CD_ORDEM_SERVICO
    left join dim_frente
        on staged_source.ID_CLIENTE = dim_frente.ID_CLIENTE
        and staged_source.CD_CORPORATIVO = dim_frente.CD_CORPORATIVO
        and staged_source.CD_REGIONAL = dim_frente.CD_REGIONAL
        and staged_source.CD_UNIDADE = dim_frente.CD_UNIDADE
        and staged_source.CD_GRUPO_EQUIPAMENTO = dim_frente.CD_FRENTE
    left join dim_processo
        on staged_source.ID_CLIENTE = dim_processo.ID_CLIENTE
        and staged_source.CD_OPERACAO_CB = dim_processo.CD_OPERACAO
)

select
    {{ dbt_utils.generate_surrogate_key(NATURAL_KEY_COLUMNS) }} as SK_DETALHE_OPERACAO,
    ID_CLIENTE,
    DT_DIA_FECHAMENTO,
    DATA,
    HORA,
    SK_EQUIPAMENTO,
    SK_OPERACAO,
    SK_FAZENDA,
    SK_ESTADO,
    SK_ORDEM_SERVICO,
    SK_FRENTE,
    SK_PROCESSO,
    CD_MISSAO,
    VL_TEMPO_SEGUNDOS,
    VL_VELOCIDADE_MEDIA,
    VL_AREA_OPERACIONAL_M2,
    FG_STATUS,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from fact_rows
