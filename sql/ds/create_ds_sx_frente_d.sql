create transient table if not exists SOLIX_BI.DS.SX_FRENTE_D (
    ID_CLIENTE number(38, 0) not null,
    CD_CORPORATIVO number(38, 0) not null,
    CD_REGIONAL number(38, 0) not null,
    CD_UNIDADE number(38, 0) not null,
    CD_FRENTE number(38, 0) not null,
    DESC_CORPORATIVO varchar(1000),
    DESC_REGIONAL varchar(1000),
    DESC_UNIDADE varchar(1000),
    DESC_FRENTE varchar(1000),
    FG_ATIVO boolean not null default true,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    SOURCE_UPDATED_AT timestamp_ntz,
    AIRBYTE_EXTRACTED_AT timestamp_ntz,
    constraint UK_DS_SX_FRENTE_D unique (
        ID_CLIENTE,
        CD_CORPORATIVO,
        CD_REGIONAL,
        CD_UNIDADE,
        CD_FRENTE
    )
);


