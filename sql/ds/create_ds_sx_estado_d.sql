create table if not exists SOLIX_BI.DS.SX_ESTADO_D (
    CD_ESTADO varchar not null,
    DESC_ESTADO varchar,
    FG_ATIVO number(1, 0) not null default 1,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    SOURCE_UPDATED_AT timestamp_ntz,
    AIRBYTE_EXTRACTED_AT timestamp_ntz,
    constraint UK_DS_SX_ESTADO_D unique (CD_ESTADO)
);
