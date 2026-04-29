{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DIMENSIONAL DW                                                      ║
║                                                                              ║
║  DIMENSAO GLOBAL / CURRENT STATE                                             ║
║                                                                              ║
║  QUANDO REUTILIZAR ESTE MODELO PARA OUTRA DIMENSAO, AJUSTE:                 ║
║  1. BLOCO DE CONFIGURACAO                                                    ║
║  2. CTE watermark_control                                                    ║
║  3. CTE staged_source                                                        ║
║  4. CHAVE NATURAL EM existing_dimension                                      ║
║  5. CTE business_rows                                                        ║
║  6. orphan_row                                                               ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le do staging DS                                                          ║
║  - preserva a surrogate key em registros ja existentes                       ║
║  - usa sequence para novos registros                                         ║
║  - faz merge incremental pela chave natural                                  ║
║  - trata FG_ATIVO como atributo mutavel do current-state                     ║
║  - nao faz delete fisico no DW                                               ║
║  - controla watermark global por PIPELINE_NAME                               ║
║  - garante a existencia do registro orfao                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

-- ============================================================================
-- BLOCO DE CONFIGURACAO — AJUSTE SO AQUI PARA REUSO
-- ============================================================================

-- Nome final da tabela no DW
{% set MODEL_ALIAS = 'SX_FRENTE_D' %}

-- Modelo staging que alimenta esta dimensao
{% set STAGING_MODEL_NAME = 'stg_ds__sx_frente_d' %}

-- Sequence da surrogate key
{% set SEQUENCE_NAME = 'SOLIX_BI.DW.SEQ_SX_FRENTE_D' %}

-- Nome da surrogate key
{% set SURROGATE_KEY_COLUMN = 'SK_FRENTE' %}

-- Chave natural da dimensao
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_CORPORATIVO', 'CD_REGIONAL', 'CD_UNIDADE', 'CD_FRENTE'] %}

-- Identificador do pipeline na tabela de watermark
{% set WATERMARK_PIPELINE_NAME = 'dim_sx_frente_d' %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    post_hook=[
      "
      merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
      using (
          select
              '" ~ WATERMARK_PIPELINE_NAME ~ "' as PIPELINE_NAME,
              max(BI_UPDATED_AT) as LAST_BI_UPDATED_AT,
              '" ~ invocation_id ~ "' as LAST_SUCCESS_BATCH_ID,
              'INCREMENTAL_WATERMARK' as LAST_LOAD_MODE,
              '" ~ invocation_id ~ "' as LAST_RUN_BATCH_ID,
              'SUCCESS' as LAST_RUN_STATUS,
              null as LAST_ERROR_MESSAGE,
              convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_RUN_STARTED_AT,
              convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_RUN_COMMITTED_AT,
              convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
          from {{ this }}
          where " ~ SURROGATE_KEY_COLUMN ~ " <> -1
      ) as src
      on tgt.PIPELINE_NAME = src.PIPELINE_NAME
      when matched then update set
          tgt.LAST_BI_UPDATED_AT = src.LAST_BI_UPDATED_AT,
          tgt.LAST_SUCCESS_BATCH_ID = src.LAST_SUCCESS_BATCH_ID,
          tgt.LAST_LOAD_MODE = src.LAST_LOAD_MODE,
          tgt.LAST_RUN_BATCH_ID = src.LAST_RUN_BATCH_ID,
          tgt.LAST_RUN_STATUS = src.LAST_RUN_STATUS,
          tgt.LAST_ERROR_MESSAGE = src.LAST_ERROR_MESSAGE,
          tgt.LAST_RUN_STARTED_AT = src.LAST_RUN_STARTED_AT,
          tgt.LAST_RUN_COMMITTED_AT = src.LAST_RUN_COMMITTED_AT,
          tgt.UPDATED_AT = src.UPDATED_AT
      when not matched then insert (
          PIPELINE_NAME,
          LAST_BI_UPDATED_AT,
          LAST_SUCCESS_BATCH_ID,
          LAST_LOAD_MODE,
          LAST_RUN_BATCH_ID,
          LAST_RUN_STATUS,
          LAST_ERROR_MESSAGE,
          LAST_RUN_STARTED_AT,
          LAST_RUN_COMMITTED_AT,
          UPDATED_AT
      ) values (
          src.PIPELINE_NAME,
          src.LAST_BI_UPDATED_AT,
          src.LAST_SUCCESS_BATCH_ID,
          src.LAST_LOAD_MODE,
          src.LAST_RUN_BATCH_ID,
          src.LAST_RUN_STATUS,
          src.LAST_ERROR_MESSAGE,
          src.LAST_RUN_STARTED_AT,
          src.LAST_RUN_COMMITTED_AT,
          src.UPDATED_AT
      )
      "
    ],
    tags=['dw', 'dimension', 'sx_frente_d']
) }}

-- ============================================================================
-- PASSO 1 — LEITURA DO WATERMARK GLOBAL
-- Esta dimensao segue o legado com controle global por PIPELINE_NAME.
-- ============================================================================

with watermark_control as (
    select
        coalesce(max(LAST_BI_UPDATED_AT), '1900-01-01'::timestamp_ntz) as LAST_BI_UPDATED_AT
    from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
    where PIPELINE_NAME = '{{ WATERMARK_PIPELINE_NAME }}'
),

-- ============================================================================
-- PASSO 2 — LEITURA DO STAGING
-- O staging ja entrega current-state no DS. Aqui fazemos apenas a leitura
-- com o recorte incremental da dimensao.
-- ============================================================================

staged_source as (
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
        s.ETL_BATCH_ID,
        s.BI_CREATED_AT,
        s.BI_UPDATED_AT
    from {{ ref(STAGING_MODEL_NAME) }} s
    cross join watermark_control w
    where 1 = 1
    {% if is_incremental() %}
      and s.BI_UPDATED_AT > coalesce(w.LAST_BI_UPDATED_AT, '1900-01-01'::timestamp_ntz)
    {% endif %}
),

-- ============================================================================
-- PASSO 3 — LEITURA DO ESTADO ATUAL DA DIMENSAO
-- Se a linha ja existir, preservamos a surrogate key.
-- ============================================================================

existing_dimension as (
    {% if is_incremental() %}
    select
        {{ SURROGATE_KEY_COLUMN }},
        ID_CLIENTE,
        CD_CORPORATIVO,
        CD_REGIONAL,
        CD_UNIDADE,
        CD_FRENTE
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as {{ SURROGATE_KEY_COLUMN }},
        cast(null as number(38, 0)) as ID_CLIENTE,
        cast(null as number(38, 0)) as CD_CORPORATIVO,
        cast(null as number(38, 0)) as CD_REGIONAL,
        cast(null as number(38, 0)) as CD_UNIDADE,
        cast(null as number(38, 0)) as CD_FRENTE
    where 1 = 0
    {% endif %}
),

-- ============================================================================
-- PASSO 4 — LINHAS DE NEGOCIO
-- Aqui definimos exatamente como os atributos da dimensao serao populados.
-- O merge final mantem uma unica linha por chave natural e atualiza FG_ATIVO
-- quando a origem sinaliza inativacao.
-- ============================================================================

business_rows as (
    select
        coalesce(existing_dimension.{{ SURROGATE_KEY_COLUMN }}, {{ SEQUENCE_NAME }}.nextval) as {{ SURROGATE_KEY_COLUMN }},
        staged_source.ID_CLIENTE,
        staged_source.CD_CORPORATIVO,
        staged_source.CD_REGIONAL,
        staged_source.CD_UNIDADE,
        staged_source.CD_FRENTE,
        upper(staged_source.DESC_CORPORATIVO) as DESC_CORPORATIVO,
        upper(staged_source.DESC_REGIONAL) as DESC_REGIONAL,
        upper(staged_source.DESC_UNIDADE) as DESC_UNIDADE,
        upper(staged_source.DESC_FRENTE) as DESC_FRENTE,
        staged_source.FG_ATIVO,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
    left join existing_dimension
        on staged_source.ID_CLIENTE = existing_dimension.ID_CLIENTE
       and staged_source.CD_CORPORATIVO = existing_dimension.CD_CORPORATIVO
       and staged_source.CD_REGIONAL = existing_dimension.CD_REGIONAL
       and staged_source.CD_UNIDADE = existing_dimension.CD_UNIDADE
       and staged_source.CD_FRENTE = existing_dimension.CD_FRENTE
),

-- ============================================================================
-- PASSO 5 — REGISTRO ORFAO
-- Linha padrao para preservar integridade referencial na fato.
-- ============================================================================

orphan_row as (
    select
        cast(-1 as number(38, 0)) as {{ SURROGATE_KEY_COLUMN }},
        cast(-1 as number(38, 0)) as ID_CLIENTE,
        cast(-1 as number(38, 0)) as CD_CORPORATIVO,
        cast(-1 as number(38, 0)) as CD_REGIONAL,
        cast(-1 as number(38, 0)) as CD_UNIDADE,
        cast(-1 as number(38, 0)) as CD_FRENTE,
        cast('UNDEFINED' as varchar) as DESC_CORPORATIVO,
        cast('UNDEFINED' as varchar) as DESC_REGIONAL,
        cast('UNDEFINED' as varchar) as DESC_UNIDADE,
        cast('UNDEFINED' as varchar) as DESC_FRENTE,
        cast(false as boolean) as FG_ATIVO,
        cast('{{ invocation_id }}' as varchar) as ETL_BATCH_ID,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_CREATED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_UPDATED_AT
),

-- ============================================================================
-- PASSO 6 — RESULTADO FINAL
-- No full-refresh, inclui a linha orfa. No incremental, envia apenas negocio.
-- ============================================================================

final as (
    {% if is_incremental() %}
    select * from business_rows
    {% else %}
    select * from orphan_row
    union all
    select * from business_rows
    {% endif %}
)

select
    {{ SURROGATE_KEY_COLUMN }},
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
    BI_UPDATED_AT
from final
