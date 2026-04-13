create table if not exists SOLIX_BI.DS.SX_EQUIPAMENTO_D (
    ID_CLIENTE number(38, 0) not null,
    CD_EQUIPAMENTO varchar(20) not null,
    DESC_EQUIPAMENTO varchar(1000),
    CD_MODELO_EQUIPAMENTO number(38, 0),
    DESC_MODELO_EQUIPAMENTO varchar(1000),
    CD_TIPO_EQUIPAMENTO number(38, 0),
    DESC_TIPO_EQUIPAMENTO varchar(1000),
    DESC_STATUS varchar(20),
    TP_USO_EQUIPAMENTO number(38, 0),
    FG_ATIVO number(1, 0) not null default 1,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    SOURCE_UPDATED_AT timestamp_ntz,
    AIRBYTE_EXTRACTED_AT timestamp_ntz,
    constraint UK_DS_SX_EQUIPAMENTO_D unique (ID_CLIENTE, CD_EQUIPAMENTO)
);

