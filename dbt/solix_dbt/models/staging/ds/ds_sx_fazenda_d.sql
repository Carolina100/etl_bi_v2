{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW -> DS (incremental por merge)                              ║
║                                                                              ║
║  ENTIDADE CURADA NA ORIGEM                                                   ║
║                                                                              ║
║  A origem atual do Airbyte e uma view PostgreSQL ja consolidada:             ║
║  bi.vw_sx_fazenda_d                                                      ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le uma RAW unica gerada pelo Airbyte                                       ║
║  - padroniza tipos para o contrato DS                                         ║
║  - mantem 1 linha por chave natural no DS                                     ║
║  - trata inativacao como update da mesma linha                                ║
║  - nao faz delete fisico da entidade                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_FAZENDA_D' %}  
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_FAZENDA', 'CD_TALHAO', 'CD_ZONA'] %}
{% set RAW_SOURCE_TABLE = 'SOLIX_BI.RAW.VW_SX_FAZENDA_D' %}
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
        cast(CD_FAZENDA as varchar(20)) as CD_FAZENDA,
        cast(DESC_FAZENDA as varchar) as DESC_FAZENDA,
        cast(CD_TALHAO as varchar(20)) as CD_TALHAO,
        cast(DESC_TALHAO as varchar) as DESC_TALHAO,
        cast(CD_ZONA as varchar(20)) as CD_ZONA,
        cast(AREA_METRO as number(38, 8)) as AREA_METRO,
        cast(CD_PRODUTOR as number(38, 0)) as CD_PRODUTOR,
        cast(DESC_PRODUTOR as varchar) as DESC_PRODUTOR,
        cast(FG_ATIVO as boolean) as FG_ATIVO,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ RAW_SOURCE_TABLE }}
),

latest_source as (
    select *
    from raw_source
    qualify row_number() over (
        partition by ID_CLIENTE, CD_FAZENDA, CD_TALHAO, CD_ZONA
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

current_target as (
    {% if is_incremental() %}
    select
        ID_CLIENTE,
        CD_FAZENDA,
        DESC_FAZENDA,
        CD_TALHAO,
        DESC_TALHAO,
        CD_ZONA,
        AREA_METRO,
        CD_PRODUTOR,
        DESC_PRODUTOR,
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
        cast(null as varchar(20)) as CD_FAZENDA,
        cast(null as varchar) as DESC_FAZENDA,
        cast(null as varchar(20)) as CD_TALHAO,
        cast(null as varchar) as DESC_TALHAO,
        cast(null as varchar(20)) as CD_ZONA,
        cast(null as number(38, 8)) as AREA_METRO,
        cast(null as number(38, 0)) as CD_PRODUTOR,
        cast(null as varchar) as DESC_PRODUTOR,
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
        s.CD_FAZENDA,
        s.DESC_FAZENDA,
        s.CD_TALHAO,
        s.DESC_TALHAO,
        s.CD_ZONA,
        s.AREA_METRO,
        s.CD_PRODUTOR,
        s.DESC_PRODUTOR,
        s.FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,
        case
            when t.ID_CLIENTE is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
                when coalesce(t.DESC_FAZENDA, '') <> coalesce(s.DESC_FAZENDA, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_TALHAO, -999) <> coalesce(s.CD_TALHAO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
                when coalesce(t.DESC_TALHAO, '') <> coalesce(s.DESC_TALHAO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_ZONA, -999) <> coalesce(s.CD_ZONA, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.AREA_METRO, -999) <> coalesce(s.AREA_METRO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_PRODUTOR, -999) <> coalesce(s.CD_PRODUTOR, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_PRODUTOR, '') <> coalesce(s.DESC_PRODUTOR, '')
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
       and s.CD_FAZENDA = t.CD_FAZENDA
       and s.CD_TALHAO = t.CD_TALHAO
       and s.CD_ZONA = t.CD_ZONA
    where
        t.ID_CLIENTE is null
        or t.ID_CLIENTE is not null and (
            coalesce(t.DESC_FAZENDA, '') <> coalesce(s.DESC_FAZENDA, '')
            or coalesce(t.CD_TALHAO, -999) <> coalesce(s.CD_TALHAO, -999)
            or coalesce(t.DESC_TALHAO, '') <> coalesce(s.DESC_TALHAO, '')
            or coalesce(t.CD_ZONA, -999) <> coalesce(s.CD_ZONA, -999)
            or coalesce(t.AREA_METRO, -999) <> coalesce(s.AREA_METRO, -999)
            or coalesce(t.CD_PRODUTOR, -999) <> coalesce(s.CD_PRODUTOR, -999)
            or coalesce(t.DESC_PRODUTOR, '') <> coalesce(s.DESC_PRODUTOR, '')
            or coalesce(t.FG_ATIVO, false) <> coalesce(s.FG_ATIVO, false)
            or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
            or coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
                <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
        )
)

select
    ID_CLIENTE,
    CD_FAZENDA,
    DESC_FAZENDA,
    CD_TALHAO,
    DESC_TALHAO,
    CD_ZONA,
    AREA_METRO,
    CD_PRODUTOR,
    DESC_PRODUTOR,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
