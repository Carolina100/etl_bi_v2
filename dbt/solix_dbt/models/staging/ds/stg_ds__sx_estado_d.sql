{#
  MODELO STAGING

  OBJETIVO:
  - expor a tabela DS curada para a camada DW
  - manter o contrato estavel consumido pelas dimensoes do DW
#}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_estado_d']
) }}

with curated_ds as (
    select
        CD_ESTADO,
        DESC_ESTADO,
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT,
        AIRBYTE_EXTRACTED_AT
    from {{ source('ds', 'sx_estado_d') }}
),

typed_data as (
    select
        cast(CD_ESTADO as varchar) as CD_ESTADO,
        cast(DESC_ESTADO as varchar) as DESC_ESTADO,
        cast(FG_ATIVO as number(1, 0)) as FG_ATIVO,
        cast(ETL_BATCH_ID as varchar) as ETL_BATCH_ID,
        cast(BI_CREATED_AT as timestamp_ntz) as BI_CREATED_AT,
        cast(BI_UPDATED_AT as timestamp_ntz) as BI_UPDATED_AT,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from curated_ds
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
from typed_data
