{#
  MODELO STAGING

  AO REUTILIZAR PARA OUTRA TABELA, NORMALMENTE TROQUE:
  1. SOURCE_TABLE_NAME
  2. os casts/colunas do select final
  3. a regra de deduplicacao, se a chave natural mudar

  OBJETIVO:
  - padronizar tipos
  - manter apenas a linha mais recente por chave natural
  - entregar um modelo limpo para a camada DW
#}

{% set SOURCE_TABLE_NAME = 'sx_ordem_servico_d' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_ordem_servico_d']
) }}

with source_data as (
    select
        ID_CLIENTE,
        CD_ORDEM_SERVICO,
        CD_ORD_STATUS,
        DT_CRIADO_EM,
        DT_ABERTURA,
        DT_ENCERRAMENTO,
        DT_INICIO_EXEC,
        DT_TERMINO_EXEC,
        DT_INICIO_PLAN_EXEC,
        DT_TERMINO_PLAN_EXEC,
        DESC_OS,
        DESC_ORD_STATUS,
        VL_LATITUDE,
        VL_LONGITUDE,
        FG_ORIGEM,
        FG_STATUS,
        VL_ORDEM_SERVICO,
        TICKET_NUMBER,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT
    from {{ source('ds', SOURCE_TABLE_NAME) }}
),

typed_data as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_ORDEM_SERVICO as number(38, 0)) as CD_ORDEM_SERVICO,
        cast(CD_ORD_STATUS as number(38, 0)) as CD_ORD_STATUS,
        cast(DT_CRIADO_EM as timestamp_ntz) as DT_CRIADO_EM,
        cast(DT_ABERTURA as timestamp_ntz) as DT_ABERTURA,
        cast(DT_ENCERRAMENTO as timestamp_ntz) as DT_ENCERRAMENTO,
        cast(DT_INICIO_EXEC as timestamp_ntz) as DT_INICIO_EXEC,
        cast(DT_TERMINO_EXEC as timestamp_ntz) as DT_TERMINO_EXEC,
        cast(DT_INICIO_PLAN_EXEC as timestamp_ntz) as DT_INICIO_PLAN_EXEC,
        cast(DT_TERMINO_PLAN_EXEC as timestamp_ntz) as DT_TERMINO_PLAN_EXEC,
        cast(DESC_OS as varchar) as DESC_OS,
        cast(DESC_ORD_STATUS as varchar) as DESC_ORD_STATUS,
        cast(VL_LATITUDE as number(23, 15)) as VL_LATITUDE,
        cast(VL_LONGITUDE as number(23, 15)) as VL_LONGITUDE,
        cast(FG_ORIGEM as varchar(5)) as FG_ORIGEM,
        cast(FG_STATUS as number(38, 0)) as FG_STATUS,
        cast(VL_ORDEM_SERVICO as varchar) as VL_ORDEM_SERVICO,
        cast(TICKET_NUMBER as number(38, 0)) as TICKET_NUMBER,
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
        partition by ID_CLIENTE, CD_ORDEM_SERVICO
        order by BI_UPDATED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

select
    ID_CLIENTE,
    CD_ORDEM_SERVICO,
    CD_ORD_STATUS,
    DT_CRIADO_EM,
    DT_ABERTURA,
    DT_ENCERRAMENTO,
    DT_INICIO_EXEC,
    DT_TERMINO_EXEC,
    DT_INICIO_PLAN_EXEC,
    DT_TERMINO_PLAN_EXEC,
    DESC_OS,
    DESC_ORD_STATUS,
    VL_LATITUDE,
    VL_LONGITUDE,
    FG_ORIGEM,
    FG_STATUS,
    VL_ORDEM_SERVICO,
    TICKET_NUMBER,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from deduplicated
