-- ============================================================================
-- REGISTRA O INICIO DA EXTRACAO NO PIPELINE DE INGESTAO
-- RESPONSAVEL SUGERIDO: AIRFLOW / ORQUESTRADOR
--
-- PARAMETROS ESPERADOS:
--   <PIPELINE_NAME> : identificador do pipeline na tabela de controle
-- ============================================================================

merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
using (
    select
        '<PIPELINE_NAME>' as PIPELINE_NAME,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_EXTRACT_STARTED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.PIPELINE_NAME = src.PIPELINE_NAME
when matched then update set
    tgt.LAST_EXTRACT_STARTED_AT = src.LAST_EXTRACT_STARTED_AT,
    tgt.UPDATED_AT = src.UPDATED_AT
when not matched then insert (
    PIPELINE_NAME,
    LAST_EXTRACT_STARTED_AT,
    UPDATED_AT
) values (
    src.PIPELINE_NAME,
    src.LAST_EXTRACT_STARTED_AT,
    src.UPDATED_AT
);
