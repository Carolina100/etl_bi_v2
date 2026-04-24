-- ============================================================================
-- CTL_PIPELINE_WATERMARK
-- Controle de watermark por pipeline dimensional.
--
-- LAST_RUN_STATUS convencao oficial:
--   RUNNING   -> pipeline em execucao (gravado no inicio de cada run)
--   SUCCESS   -> pipeline concluido com sucesso (gravado pelo dbt post_hook ou Airflow)
--   FAILED    -> pipeline falhou (gravado pelo Airflow em caso de erro)
-- ============================================================================
create table if not exists SOLIX_BI.DS.CTL_PIPELINE_WATERMARK (
    PIPELINE_NAME         varchar not null,
    LAST_BI_UPDATED_AT    timestamp_ntz,
    LAST_SUCCESS_BATCH_ID varchar,
    LAST_LOAD_MODE        varchar,
    LAST_RUN_BATCH_ID     varchar,
    LAST_RUN_STATUS       varchar,
    LAST_ERROR_MESSAGE    varchar,
    LAST_RUN_STARTED_AT   timestamp_ntz,
    LAST_RUN_COMMITTED_AT timestamp_ntz,
    UPDATED_AT            timestamp_ntz not null default convert_timezone('UTC', current_timestamp())::timestamp_ntz,
    constraint PK_CTL_PIPELINE_WATERMARK primary key (PIPELINE_NAME)
);
