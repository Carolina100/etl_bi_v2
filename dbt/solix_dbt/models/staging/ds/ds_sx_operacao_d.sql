{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW -> DS (incremental por merge)                              ║
║                                                                              ║
║  ENTIDADE CURADA NA ORIGEM                                                   ║
║                                                                              ║
║  A origem atual do Airbyte e uma view PostgreSQL ja consolidada:             ║
║  bi.vw_sx_operacao_d                                                      ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le uma RAW unica gerada pelo Airbyte                                       ║
║  - padroniza tipos para o contrato DS                                         ║
║  - mantem 1 linha por chave natural no DS                                     ║
║  - trata inativacao como update da mesma linha                                ║
║  - nao faz delete fisico da entidade                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_OPERACAO_D' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_OPERACAO'] %}
{% set RAW_SOURCE_TABLE = 'SOLIX_BI.RAW.VW_SX_OPERACAO_D' %}
{% set BATCH_ID = invocation_id %}

{{ config(
    materialized='incremental',
    schema='DS',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['ds', 'incremental', MODEL_ALIAS | lower]
) }}

with raw_source as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_OPERACAO as number(38, 0)) as CD_OPERACAO,
        cast(DESC_OPERACAO as varchar) as DESC_OPERACAO,
        cast(CD_GRUPO_OPERACAO as number(38, 0)) as CD_GRUPO_OPERACAO,
        cast(DESC_GRUPO_OPERACAO as varchar) as DESC_GRUPO_OPERACAO,
        cast(CD_GRUPO_PARADA as number(38, 0)) as CD_GRUPO_PARADA,
        cast(FG_TIPO_OPERACAO as varchar) as FG_TIPO_OPERACAO,
        cast(CD_PROCESSO as number(38, 0)) as CD_PROCESSO,
        cast(DESC_PROCESSO as varchar) as DESC_PROCESSO,
        cast(FG_ATIVO as boolean) as FG_ATIVO,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ RAW_SOURCE_TABLE }}
),

latest_source as (
    select *
    from raw_source
    qualify row_number() over (
        partition by ID_CLIENTE, CD_OPERACAO
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

current_target as (
    {% if is_incremental() %}
    select
        ID_CLIENTE,
        CD_OPERACAO,
        DESC_OPERACAO,
        CD_GRUPO_OPERACAO,
        DESC_GRUPO_OPERACAO,
        CD_GRUPO_PARADA,
        FG_TIPO_OPERACAO,
        CD_PROCESSO,
        DESC_PROCESSO,
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT,
        AIRBYTE_EXTRACTED_AT
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as ID_CLIENTE,
        cast(null as number(38, 0)) as CD_OPERACAO,
        cast(null as varchar) as DESC_OPERACAO,
        cast(null as number(38, 0)) as CD_GRUPO_OPERACAO,
        cast(null as varchar) as DESC_GRUPO_OPERACAO,
        cast(null as number(38, 0)) as CD_GRUPO_PARADA,
        cast(null as varchar) as FG_TIPO_OPERACAO,
        cast(null as number(38, 0)) as CD_PROCESSO,
        cast(null as varchar) as DESC_PROCESSO,
        cast(null as boolean) as FG_ATIVO,
        cast(null as varchar) as ETL_BATCH_ID,
        cast(null as timestamp_ntz) as BI_CREATED_AT,
        cast(null as timestamp_ntz) as BI_UPDATED_AT,
        cast(null as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(null as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    where 1 = 0
    {% endif %}
),

changed_or_new as (
    select
        s.ID_CLIENTE,
        s.CD_OPERACAO,
        s.DESC_OPERACAO,
        s.CD_GRUPO_OPERACAO,
        s.DESC_GRUPO_OPERACAO,
        s.CD_GRUPO_PARADA,
        s.FG_TIPO_OPERACAO,
        s.CD_PROCESSO,
        s.DESC_PROCESSO,
        s.FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,
        case
            when t.ID_CLIENTE is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_OPERACAO, '') <> coalesce(s.DESC_OPERACAO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_GRUPO_OPERACAO, -999) <> coalesce(s.CD_GRUPO_OPERACAO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_GRUPO_OPERACAO, '') <> coalesce(s.DESC_GRUPO_OPERACAO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_GRUPO_PARADA, -999) <> coalesce(s.CD_GRUPO_PARADA, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.FG_TIPO_OPERACAO, '') <> coalesce(s.FG_TIPO_OPERACAO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_PROCESSO, -999) <> coalesce(s.CD_PROCESSO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_PROCESSO, '') <> coalesce(s.DESC_PROCESSO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.FG_ATIVO, false) <> coalesce(s.FG_ATIVO, false)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            else t.BI_UPDATED_AT
        end as BI_UPDATED_AT,
        s.SOURCE_UPDATED_AT,
        s.AIRBYTE_EXTRACTED_AT
    from latest_source s
    left join current_target t
        on s.ID_CLIENTE = t.ID_CLIENTE
       and s.CD_OPERACAO = t.CD_OPERACAO
    where
        t.ID_CLIENTE is null
        or (
            t.ID_CLIENTE is not null
            and (
                coalesce(t.DESC_OPERACAO, '') <> coalesce(s.DESC_OPERACAO, '')
                or coalesce(t.CD_GRUPO_OPERACAO, -999) <> coalesce(s.CD_GRUPO_OPERACAO, -999)
                or coalesce(t.DESC_GRUPO_OPERACAO, '') <> coalesce(s.DESC_GRUPO_OPERACAO, '')
                or coalesce(t.CD_GRUPO_PARADA, -999) <> coalesce(s.CD_GRUPO_PARADA, -999)
                or coalesce(t.FG_TIPO_OPERACAO, '') <> coalesce(s.FG_TIPO_OPERACAO, '')
                or coalesce(t.CD_PROCESSO, -999) <> coalesce(s.CD_PROCESSO, -999)
                or coalesce(t.DESC_PROCESSO, '') <> coalesce(s.DESC_PROCESSO, '')
                or coalesce(t.FG_ATIVO, false) <> coalesce(s.FG_ATIVO, false)
                or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                   <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                or coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
                   <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
            )
        )
)

select
    ID_CLIENTE,
    CD_OPERACAO,
    DESC_OPERACAO,
    CD_GRUPO_OPERACAO,
    DESC_GRUPO_OPERACAO,
    CD_GRUPO_PARADA,
    FG_TIPO_OPERACAO,
    CD_PROCESSO,
    DESC_PROCESSO,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
