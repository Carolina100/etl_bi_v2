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

{% set MODEL_ALIAS = 'SX_FRENTE_D' %}  
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_CORPORATIVO', 'CD_REGIONAL', 'CD_UNIDADE', 'CD_FRENTE'] %}
{% set RAW_SOURCE_TABLE = 'SOLIX_BI.RAW.VW_SX_FRENTE_D' %}
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
        cast(CD_CORPORATIVO as number(38, 0)) as CD_CORPORATIVO,
        cast(CD_REGIONAL as number(38, 0)) as CD_REGIONAL,
        cast(CD_UNIDADE as number(38, 0)) as CD_UNIDADE,
        cast(CD_FRENTE as number(38, 0)) as CD_FRENTE,
        cast(DESC_CORPORATIVO as varchar) as DESC_CORPORATIVO,
        cast(DESC_REGIONAL as varchar) as DESC_REGIONAL,
        cast(DESC_UNIDADE as varchar) as DESC_UNIDADE,
        cast(DESC_FRENTE as varchar) as DESC_FRENTE,
        cast(FG_ATIVO as boolean) as FG_ATIVO,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ RAW_SOURCE_TABLE }}
),

latest_source as (
    select *
    from raw_source
    qualify row_number() over (
        partition by ID_CLIENTE, CD_CORPORATIVO, CD_REGIONAL, CD_UNIDADE, CD_FRENTE
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

current_target as (
    {% if is_incremental() %}
    select
        ID_CLIENTE,
        CD_CORPORATIVO,
        CD_REGIONAL,
        CD_UNIDADE,
        CD_FRENTE,
        DESC_CORPORATIVO,
        DESC_REGIONAL,
        DESC_UNIDADE,
        DESC_FRENTE,
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
        cast(null as number(38, 0)) as CD_CORPORATIVO,
        cast(null as number(38, 0)) as CD_REGIONAL,
        cast(null as number(38, 0)) as CD_UNIDADE,
        cast(null as number(38, 0)) as CD_FRENTE,
        cast(null as varchar) as DESC_CORPORATIVO,
        cast(null as varchar) as DESC_REGIONAL,
        cast(null as varchar) as DESC_UNIDADE,
        cast(null as varchar) as DESC_FRENTE,
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
        s.CD_CORPORATIVO,
        s.CD_REGIONAL,
        s.CD_UNIDADE,
        s.CD_FRENTE,
        s.DESC_CORPORATIVO,
        s.DESC_REGIONAL,
        s.DESC_UNIDADE,
        s.DESC_FRENTE,
        s.FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,
        case
            when t.ID_CLIENTE is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_CORPORATIVO, -999) <> coalesce(s.CD_CORPORATIVO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_CORPORATIVO, '') <> coalesce(s.DESC_CORPORATIVO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_REGIONAL, -999) <> coalesce(s.CD_REGIONAL, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_UNIDADE, -999) <> coalesce(s.CD_UNIDADE, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_UNIDADE, '') <> coalesce(s.DESC_UNIDADE, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_FRENTE, '') <> coalesce(s.DESC_FRENTE, '')
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
       and s.CD_CORPORATIVO = t.CD_CORPORATIVO
       and s.CD_REGIONAL = t.CD_REGIONAL
       and s.CD_UNIDADE = t.CD_UNIDADE
       and s.CD_FRENTE = t.CD_FRENTE
    where
        t.ID_CLIENTE is null
        or t.ID_CLIENTE is not null and (
            coalesce(t.DESC_CORPORATIVO, '') <> coalesce(s.DESC_CORPORATIVO, '')
            or coalesce(t.CD_CORPORATIVO, -999) <> coalesce(s.CD_CORPORATIVO, -999)
            or coalesce(t.CD_REGIONAL, -999) <> coalesce(s.CD_REGIONAL, -999)
            or coalesce(t.CD_UNIDADE, -999) <> coalesce(s.CD_UNIDADE, -999)
            or coalesce(t.CD_FRENTE, -999) <> coalesce(s.CD_FRENTE, -999)
            or coalesce(t.DESC_UNIDADE, '') <> coalesce(s.DESC_UNIDADE, '')
            or coalesce(t.DESC_FRENTE, '') <> coalesce(s.DESC_FRENTE, '')
            or coalesce(t.FG_ATIVO, false) <> coalesce(s.FG_ATIVO, false)
            or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
            or coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
                <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
        )
)

select
    ID_CLIENTE,
    CD_CORPORATIVO,
    CD_REGIONAL,
    CD_UNIDADE,
    CD_FRENTE,
    DESC_CORPORATIVO,
    DESC_REGIONAL,
    DESC_UNIDADE,
    DESC_FRENTE,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,  
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
