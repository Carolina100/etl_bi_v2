-- ============================================================================
-- REGISTRA O FIM DA EXTRACAO NO PIPELINE DE INGESTAO
-- RESPONSAVEL SUGERIDO: AIRFLOW / ORQUESTRADOR
--
-- PARAMETROS ESPERADOS:
--   <PIPELINE_NAME> : identificador do pipeline na tabela de controle
--   <ID_CLIENTE>    : 0 para pipeline global, ou id real para pipeline por cliente
-- ============================================================================

merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
using (
    select
        '<PIPELINE_NAME>' as PIPELINE_NAME,
        <ID_CLIENTE> as ID_CLIENTE,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_EXTRACT_ENDED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.PIPELINE_NAME = src.PIPELINE_NAME
and tgt.ID_CLIENTE = src.ID_CLIENTE
when matched then update set
    tgt.LAST_EXTRACT_ENDED_AT = src.LAST_EXTRACT_ENDED_AT,
    tgt.UPDATED_AT = src.UPDATED_AT
when not matched then insert (
    PIPELINE_NAME,
    ID_CLIENTE,
    LAST_EXTRACT_ENDED_AT,
    UPDATED_AT
) values (
    src.PIPELINE_NAME,
    src.ID_CLIENTE,
    src.LAST_EXTRACT_ENDED_AT,
    src.UPDATED_AT
);
