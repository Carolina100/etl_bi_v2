create transient table if not exists SOLIX_BI.DS.SX_CLIENTE_D (
    ID_CLIENTE number(38, 0) not null,
    NOME_CLIENTE varchar,
    OWNER_CLIENTE varchar,
    SERVER_CLIENTE varchar,
    FG_ATIVO boolean,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    SOURCE_UPDATED_AT timestamp_ntz,
    AIRBYTE_EXTRACTED_AT timestamp_ntz,
    constraint UK_DS_SX_CLIENTE_D unique (ID_CLIENTE)
);
