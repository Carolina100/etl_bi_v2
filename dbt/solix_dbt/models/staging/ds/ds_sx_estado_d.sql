{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW → DS (incremental por merge)                               ║
║                                                                              ║
║  ENTIDADE SIMPLES / CURRENT STATE                                            ║
║                                                                              ║
║  QUANDO REUTILIZAR ESTE MODELO PARA OUTRA ENTIDADE SIMPLES, AJUSTE:         ║
║  1. BLOCO DE CONFIGURACAO                                                    ║
║  2. CTE base_entity_stage / latest                                           ║
║  3. COMPARACAO em changed_or_new                                             ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le uma unica tabela RAW                                                   ║
║  - calcula a ultima versao por chave natural                                 ║
║  - mantem 1 linha por chave natural no DS                                    ║
║  - suporta current-state com soft delete quando houver FG_ATIVO              ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

-- ============================================================================
-- BLOCO DE CONFIGURACAO — AJUSTE SO AQUI PARA REUSO
-- ============================================================================

-- Nome final da tabela no schema DS
{% set MODEL_ALIAS = 'SX_ESTADO_D' %}

-- Chave natural da entidade no DS
{% set NATURAL_KEY_COLUMNS = ['CD_ESTADO'] %}

-- Tabela RAW da entidade
{% set BASE_RAW_TABLE = 'SOLIX_BI.RAW.CDT_ESTADO' %}

-- Coluna de data de atualizacao da origem
{% set SOURCE_UPDATED_AT_COLUMN = 'DT_UPDATED' %}

-- A entidade possui FG_ATIVO na origem?
-- false = entidade global/compartilhada sem delete logico explicito
{% set HAS_FG_ATIVO = false %}

-- Nome da coluna de flag na origem, caso exista
{% set FG_ATIVO_COLUMN = 'FG_ATIVO' %}

-- Batch tecnico do dbt
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

-- ============================================================================
-- PASSO 1 — LEITURA E DEDUPLICACAO DA TABELA DIRIGENTE
-- Nesta entidade a propria tabela RAW e a verdade do cadastro.
-- ============================================================================

with base_entity_stage as (
    select
        cast(CD_ESTADO as varchar) as CD_ESTADO,
        cast(DESC_ESTADO as varchar) as DESC_ESTADO,
        {% if HAS_FG_ATIVO %}
        cast({{ FG_ATIVO_COLUMN }} as number(1, 0)) as FG_ATIVO,
        {% else %}
        cast(1 as number(1, 0)) as FG_ATIVO,
        {% endif %}
        cast({{ SOURCE_UPDATED_AT_COLUMN }} as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ BASE_RAW_TABLE }}
),

base_entity_latest as (
    select *
    from base_entity_stage
    qualify row_number() over (
        partition by CD_ESTADO
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

-- ============================================================================
-- PASSO 2 — ESTADO ATUAL DA TABELA DS
-- Usado para identificar apenas inserts ou updates necessarios.
-- ============================================================================

current_target as (
    {% if is_incremental() %}
    select
        CD_ESTADO,
        DESC_ESTADO,
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT,
        AIRBYTE_EXTRACTED_AT
    from {{ this }}
    {% else %}
    select
        cast(null as varchar) as CD_ESTADO,
        cast(null as varchar) as DESC_ESTADO,
        cast(null as number(1, 0)) as FG_ATIVO,
        cast(null as varchar) as ETL_BATCH_ID,
        cast(null as timestamp_ntz) as BI_CREATED_AT,
        cast(null as timestamp_ntz) as BI_UPDATED_AT,
        cast(null as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(null as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    where 1 = 0
    {% endif %}
),

-- ============================================================================
-- PASSO 3 — DETECCAO DE NOVOS OU ALTERADOS
-- REGRAS:
-- - registro novo -> insert
-- - registro existente alterado -> update
-- - BI_CREATED_AT e preservado
-- - BI_UPDATED_AT so muda quando houve alteracao real
-- ============================================================================

changed_or_new as (
    select
        s.CD_ESTADO,
        s.DESC_ESTADO,
        s.FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,
        case
            when t.CD_ESTADO is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_ESTADO, '') <> coalesce(s.DESC_ESTADO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            {% if HAS_FG_ATIVO %}
            when coalesce(t.FG_ATIVO, 1) <> coalesce(s.FG_ATIVO, 1)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            {% endif %}
            else t.BI_UPDATED_AT
        end as BI_UPDATED_AT,
        s.SOURCE_UPDATED_AT,
        s.AIRBYTE_EXTRACTED_AT
    from base_entity_latest s
    left join current_target t
        on s.CD_ESTADO = t.CD_ESTADO
    where t.CD_ESTADO is null
       or coalesce(t.DESC_ESTADO, '') <> coalesce(s.DESC_ESTADO, '')
       or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
          <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
       {% if HAS_FG_ATIVO %}
       or coalesce(t.FG_ATIVO, 1) <> coalesce(s.FG_ATIVO, 1)
       {% endif %}
)

select
    CD_ESTADO,
    DESC_ESTADO,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
