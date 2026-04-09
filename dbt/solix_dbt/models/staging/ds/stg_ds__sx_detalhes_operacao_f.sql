{#
  MODELO STAGING DA FATO SX_DETALHES_OPERACAO_F

  O QUE ESTE MODELO FAZ:
  - le a tabela DS da fato
  - padroniza tipos
  - mantem apenas a linha mais recente por grão composto
#}

{% set SOURCE_TABLE_NAME = 'sx_detalhes_operacao_f' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_detalhes_operacao_f', 'fact']
) }}

with source_data as (
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
    from {{ source('ds', SOURCE_TABLE_NAME) }}
),

typed_data as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(DT_DIA_FECHAMENTO as date) as DT_DIA_FECHAMENTO,
        cast(DATA as date) as DATA,
        cast(HORA as number(38, 0)) as HORA,
        cast(CD_EQUIPAMENTO as number(38, 0)) as CD_EQUIPAMENTO,
        cast(CD_GRUPO_EQUIPAMENTO as number(38, 0)) as CD_GRUPO_EQUIPAMENTO,
        cast(CD_OPERACAO as number(38, 0)) as CD_OPERACAO,
        cast(CD_OPERACAO_CB as number(38, 0)) as CD_OPERACAO_CB,
        cast(CD_ORDEM_SERVICO as number(38, 0)) as CD_ORDEM_SERVICO,
        cast(CD_UNIDADE as number(38, 0)) as CD_UNIDADE,
        cast(CD_REGIONAL as number(38, 0)) as CD_REGIONAL,
        cast(CD_CORPORATIVO as number(38, 0)) as CD_CORPORATIVO,
        cast(CD_FAZENDA as number(38, 0)) as CD_FAZENDA,
        cast(CD_ZONA as number(38, 0)) as CD_ZONA,
        cast(CD_TALHAO as number(38, 0)) as CD_TALHAO,
        cast(CD_MISSAO as number(38, 0)) as CD_MISSAO,
        cast(CD_ESTADO as varchar) as CD_ESTADO,
        cast(VL_TEMPO_SEGUNDOS as number(38, 6)) as VL_TEMPO_SEGUNDOS,
        cast(VL_VELOCIDADE_MEDIA as number(38, 6)) as VL_VELOCIDADE_MEDIA,
        cast(VL_AREA_OPERACIONAL_M2 as number(38, 6)) as VL_AREA_OPERACIONAL_M2,
        cast(FG_STATUS as varchar) as FG_STATUS,
        cast(ETL_BATCH_ID as varchar) as ETL_BATCH_ID,
        cast(BI_CREATED_AT as timestamp_ntz) as BI_CREATED_AT,
        cast(BI_UPDATED_AT as timestamp_ntz) as BI_UPDATED_AT
    from source_data
),

deduplicated as (
    select
        *
    from typed_data
    qualify row_number() over (
        partition by
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
            CD_ESTADO
        order by BI_UPDATED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

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
from deduplicated
