{#
  MODELO DIMENSIONAL DW

  AO REUTILIZAR PARA OUTRA DIMENSAO, NORMALMENTE TROQUE:
  1. MODEL_ALIAS
     Nome final da tabela no DW.

  2. STAGING_MODEL_NAME
     Nome do model staging que alimenta esta dimensao.

  3. SEQUENCE_NAME
     Sequence usada para gerar a surrogate key.

  4. SURROGATE_KEY_COLUMN
     Nome da surrogate key da dimensao.

  5. NATURAL_KEY_COLUMNS
     Chave natural da dimensao usada no merge.

  6. ORPHAN ROW
     Valores padrao do registro orfao.
     Padrao moderno sugerido:
     - surrogate key = -1
     - codigos = '-1'
     - descricoes = 'NAO INFORMADO'

  7. select final de business_rows
     Ajuste os campos para refletir a nova dimensao.

  O QUE ESTE MODELO FAZ:
  - le do staging DS
  - preserva a surrogate key em registros ja existentes
  - usa sequence para novos registros
  - faz merge incremental pela chave natural
  - garante a existencia do registro orfao
#}

{% set MODEL_ALIAS = 'SX_ESTADO_D' %}
{% set STAGING_MODEL_NAME = 'stg_ds__sx_estado_d' %}
{% set SEQUENCE_NAME = 'SOLIX_BI.DW.SEQ_SX_ESTADO_D' %}
{% set SURROGATE_KEY_COLUMN = 'SK_ESTADO' %}
{% set NATURAL_KEY_COLUMNS = ['CD_ESTADO'] %}
{% set WATERMARK_PIPELINE_NAME = 'dim_sx_estado_d' %}
{% set REPROCESS_FROM = var('dim_sx_estado_d_reprocess_from', none) %}

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
              case
                  -- Detecta full refresh em runtime pela linha orfa criada nesta execucao.
                  when exists (
                      select 1
                      from {{ this }} orphan_probe
                      where orphan_probe." ~ SURROGATE_KEY_COLUMN ~ " = -1
                        and orphan_probe.ETL_BATCH_ID = '" ~ invocation_id ~ "'
                  ) then 'FULL_REFRESH'
                  " ~ ("when 1 = 1 then 'REPROCESS_FROM_DATE'" if REPROCESS_FROM else "else 'INCREMENTAL_WATERMARK'") ~ "
              end as LAST_LOAD_MODE,
              '" ~ invocation_id ~ "' as LAST_RUN_BATCH_ID,
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
          tgt.LAST_RUN_STARTED_AT = src.LAST_RUN_STARTED_AT,
          tgt.LAST_RUN_COMMITTED_AT = src.LAST_RUN_COMMITTED_AT,
          tgt.UPDATED_AT = src.UPDATED_AT
      when not matched then insert (
          PIPELINE_NAME,
          LAST_BI_UPDATED_AT,
          LAST_SUCCESS_BATCH_ID,
          LAST_LOAD_MODE,
          LAST_RUN_BATCH_ID,
          LAST_RUN_STARTED_AT,
          LAST_RUN_COMMITTED_AT,
          UPDATED_AT
      ) values (
          src.PIPELINE_NAME,
          src.LAST_BI_UPDATED_AT,
          src.LAST_SUCCESS_BATCH_ID,
          src.LAST_LOAD_MODE,
          src.LAST_RUN_BATCH_ID,
          src.LAST_RUN_STARTED_AT,
          src.LAST_RUN_COMMITTED_AT,
          src.UPDATED_AT
      )
      "
    ],
    tags=['dw', 'dimension', 'sx_estado_d']
) }}

with watermark_control as (
    -- Dimensao global: o watermark e controlado apenas por PIPELINE_NAME.
    -- Se no futuro esta dimensao passar a ser segregada por cliente,
    -- ajustar a leitura e a gravacao da CTL_PIPELINE_WATERMARK para
    -- considerar PIPELINE_NAME + ID_CLIENTE.
    select
        coalesce(max(LAST_BI_UPDATED_AT), '1900-01-01'::timestamp_ntz) as LAST_BI_UPDATED_AT
    from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
    where PIPELINE_NAME = '{{ WATERMARK_PIPELINE_NAME }}'
),

staged_source as (
    select
        -- ▼ Colunas de negócio da dimensão (alterar conforme a tabela)
        s.CD_ESTADO,
        s.DESC_ESTADO,
        -- ▲ Fim das colunas de negócio
        s.FG_ATIVO,
        s.ETL_BATCH_ID,
        s.BI_CREATED_AT,
        s.BI_UPDATED_AT
    from {{ ref(STAGING_MODEL_NAME) }} s
    cross join watermark_control w
    where 1 = 1
    {% if is_incremental() %}
      {% if REPROCESS_FROM %}
        and s.BI_UPDATED_AT >= '{{ REPROCESS_FROM }}'::timestamp_ntz
      {% else %}
        and s.BI_UPDATED_AT > coalesce(w.LAST_BI_UPDATED_AT, '1900-01-01'::timestamp_ntz)
      {% endif %}
    {% endif %}
),

existing_dimension as (
    {% if is_incremental() %}
    select
        {{ SURROGATE_KEY_COLUMN }},
        -- ▼ Chave(s) natural(is) da dimensão
        CD_ESTADO
        -- ▲ Fim da(s) chave(s) natural(is)
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as {{ SURROGATE_KEY_COLUMN }},
        -- ▼ Chave(s) natural(is) da dimensão tratada nula
        cast(null as varchar) as CD_ESTADO
        -- ▲ Fim da(s) chave(s) natural(is) tratada nula
    where 1 = 0
    {% endif %}
),

business_rows as (
    select
        coalesce(existing_dimension.{{ SURROGATE_KEY_COLUMN }}, {{ SEQUENCE_NAME }}.nextval) as {{ SURROGATE_KEY_COLUMN }},
        -- ▼ Colunas de negócio sendo populadas (aplique UPPER() etc se necessário)
        staged_source.CD_ESTADO,
        upper(staged_source.DESC_ESTADO) as DESC_ESTADO,
        -- ▲ Fim das colunas de negócio populadas
        staged_source.FG_ATIVO,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
    left join existing_dimension
        -- ▼ Condição de Join (Chave Natural)
        on staged_source.CD_ESTADO = existing_dimension.CD_ESTADO
        -- ▲ Fim Condição de Join
),

orphan_row as (
    select
        cast(-1 as number(38, 0)) as {{ SURROGATE_KEY_COLUMN }},
        -- ▼ Valores default (órfão) para as colunas de negócio
        cast('-1' as varchar) as CD_ESTADO,
        cast('UNDEFINED' as varchar) as DESC_ESTADO,
        -- ▲ Fim valores default
        cast(1 as number(1, 0)) as FG_ATIVO,
        cast('{{ invocation_id }}' as varchar) as ETL_BATCH_ID,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_CREATED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_UPDATED_AT
),

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
    -- ▼ Colunas de negócio selecionadas
    CD_ESTADO,
    DESC_ESTADO,
    -- ▲ Fim colunas selecionadas
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from final
