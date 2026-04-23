create transient table if not exists SOLIX_BI.DS.SX_ESTADO_D (
    CD_ESTADO varchar not null,
    DESC_ESTADO varchar,
    DESC_ESTADO_EN_US varchar,
    DESC_ESTADO_PT_BR varchar,
    DESC_ESTADO_ES_ES varchar,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    SOURCE_UPDATED_AT timestamp_ntz,
    AIRBYTE_EXTRACTED_AT timestamp_ntz,
    constraint UK_DS_SX_ESTADO_D unique (CD_ESTADO)
);
