create table if not exists SOLIX_BI.DS.SX_OPERACAO_D (
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
    FG_ATIVO number(1, 0) not null default 1,
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    constraint UK_DS_SX_OPERACAO_D unique (ID_CLIENTE, CD_OPERACAO)
);
