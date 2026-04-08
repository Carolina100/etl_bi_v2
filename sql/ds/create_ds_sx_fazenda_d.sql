create table if not exists SOLIX_BI.DS.SX_FAZENDA_D (
    ID_CLIENTE number(38, 0) not null,
    CD_FAZENDA varchar(20) not null,
    DESC_FAZENDA varchar(255),
    CD_TALHAO varchar(20) not null,
    DESC_TALHAO varchar (255),
    CD_ZONA varchar(20) not null,
    AREA_TOTAL number(38, 8),
    DESC_PRODUTOR varchar(255),
    FG_ATIVO number(1, 0) not null default 1,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    constraint UK_DS_SX_FAZENDA_D unique (ID_CLIENTE, CD_FAZENDA, CD_ZONA, CD_TALHAO)
);
