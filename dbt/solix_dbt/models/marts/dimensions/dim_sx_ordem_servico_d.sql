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

{% set MODEL_ALIAS = 'SX_ORDEM_SERVICO_D' %}
{% set STAGING_MODEL_NAME = 'stg_ds__sx_ordem_servico_d' %}
{% set SEQUENCE_NAME = 'SOLIX_BI.DW.SEQ_SX_ORDEM_SERVICO_D' %}
{% set SURROGATE_KEY_COLUMN = 'SK_ORDEM_SERVICO' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_ORDEM_SERVICO'] %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['dw', 'dimension', 'sx_ordem_servico_d']
) }}

with staged_source as (
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
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT
    from {{ ref(STAGING_MODEL_NAME) }}
    {% if is_incremental() %}
    where BI_UPDATED_AT > (
        select coalesce(max(BI_UPDATED_AT), '1900-01-01'::timestamp_ntz)
        from {{ this }}
        where {{ SURROGATE_KEY_COLUMN }} <> -1
    )
    {% endif %}
),

existing_dimension as (
    {% if is_incremental() %}
    select
        {{ SURROGATE_KEY_COLUMN }} as SK_ORDEM_SERVICO,
        ID_CLIENTE,
        CD_ORDEM_SERVICO
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as SK_ORDEM_SERVICO,
        cast(null as number(38, 0)) as ID_CLIENTE,
        cast(null as number(38, 0)) as CD_ORDEM_SERVICO
    where 1 = 0
    {% endif %}
),

business_rows as (
    select
        coalesce(existing_dimension.SK_ORDEM_SERVICO, {{ SEQUENCE_NAME }}.nextval) as SK_ORDEM_SERVICO,
        staged_source.ID_CLIENTE,
        staged_source.CD_ORDEM_SERVICO,
        staged_source.CD_ORD_STATUS,
        staged_source.DT_CRIADO_EM,
        staged_source.DT_ABERTURA,
        staged_source.DT_ENCERRAMENTO,
        staged_source.DT_INICIO_EXEC,
        staged_source.DT_TERMINO_EXEC,
        staged_source.DT_INICIO_PLAN_EXEC,
        staged_source.DT_TERMINO_PLAN_EXEC,
        upper(staged_source.DESC_OS) as DESC_OS,
        upper(staged_source.DESC_ORD_STATUS) as DESC_ORD_STATUS,
        staged_source.VL_LATITUDE,
        staged_source.VL_LONGITUDE,
        staged_source.FG_ORIGEM,
        staged_source.FG_STATUS,
        upper(staged_source.VL_ORDEM_SERVICO) as VL_ORDEM_SERVICO,
        staged_source.TICKET_NUMBER,
        staged_source.FG_ATIVO,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
    left join existing_dimension
        on staged_source.ID_CLIENTE = existing_dimension.ID_CLIENTE
       and staged_source.CD_ORDEM_SERVICO = existing_dimension.CD_ORDEM_SERVICO
),

orphan_row as (
    select
        cast(-1 as number(38, 0)) as SK_ORDEM_SERVICO,
        cast(-1 as number(38, 0)) as ID_CLIENTE,
        cast(-1 as number(38, 0)) as CD_ORDEM_SERVICO,
        cast(-1 as number(38, 0)) as CD_ORD_STATUS,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_CRIADO_EM,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_ABERTURA,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_ENCERRAMENTO,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_INICIO_EXEC,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_TERMINO_EXEC,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_INICIO_PLAN_EXEC,
        cast('1970-01-01 00:00:00' as timestamp_ntz) as DT_TERMINO_PLAN_EXEC,
        cast('UNDEFINED' as varchar) as DESC_OS,
        cast('UNDEFINED' as varchar) as DESC_ORD_STATUS,
        cast(-1 as number(23, 15)) as VL_LATITUDE,
        cast(-1 as number(23, 15)) as VL_LONGITUDE,
        cast('-1' as varchar(5)) as FG_ORIGEM,
        cast(-1 as number(38, 0)) as FG_STATUS,
        cast('UNDEFINED' as varchar) as VL_ORDEM_SERVICO,
        cast(-1 as number(38, 0)) as TICKET_NUMBER,
        cast(-1 as number(1, 0)) as FG_ATIVO,
        cast('DBT_ORPHAN_ROW' as varchar) as ETL_BATCH_ID,
        convert_timezone('America/Sao_Paulo', current_timestamp())::timestamp_ntz as BI_CREATED_AT,
        convert_timezone('America/Sao_Paulo', current_timestamp())::timestamp_ntz as BI_UPDATED_AT
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
    SK_ORDEM_SERVICO,
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
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from final
