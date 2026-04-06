create table if not exists SOLIX_BI.DW.SX_OPERACAO_D (
    SK_OPERACAO number(38, 0) not null,
    ID_CLIENTE number(38, 0) not null,
    CD_OPERACAO number(38, 0) not null,
    DESC_OPERACAO varchar,
    CD_GRUPO_OPERACAO number(38, 0),
    DESC_GRUPO_OPERACAO varchar,
    CD_GRUPO_PARADA number(38, 0),
    DESC_GRUPO_PARADA varchar,
    FG_TIPO_OPERACAO varchar,
    CD_PROCESSO_TALHAO number(38, 0),
    DESC_PROCESSO_TALHAO varchar,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    constraint PK_DW_SX_OPERACAO_D primary key (SK_OPERACAO),
    constraint UK_DW_SX_OPERACAO_D unique (ID_CLIENTE, CD_OPERACAO)

);
