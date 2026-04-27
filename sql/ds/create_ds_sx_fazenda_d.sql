create transient table if not exists SOLIX_BI.DS.SX_FAZENDA_D (
    ID_CLIENTE number(38, 0) not null,
    CD_FAZENDA varchar(20) not null,
    DESC_FAZENDA varchar(255),
    CD_TALHAO varchar(20) not null,
    DESC_TALHAO varchar (255),
    CD_ZONA varchar(20) not null,
    AREA_METRO number(38, 8),
    CD_PRODUTOR number(38, 0),
    DESC_PRODUTOR varchar(255),
    FG_ATIVO boolean not null default true,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    SOURCE_UPDATED_AT timestamp_ntz,
    AIRBYTE_EXTRACTED_AT timestamp_ntz,
    constraint UK_DS_SX_FAZENDA_D unique (ID_CLIENTE, CD_FAZENDA, CD_ZONA, CD_TALHAO)
);
