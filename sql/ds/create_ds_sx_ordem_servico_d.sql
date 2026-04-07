create table if not exists SOLIX_BI.DS.SX_ORDEM_SERVICO_D (
    ID_CLIENTE number(38, 0) not null,
    CD_ORDEM_SERVICO number(38, 0) not null,
    CD_ORD_STATUS number(38, 0),
    DT_CRIADO_EM timestamp_ntz,
    DT_ABERTURA timestamp_ntz,
    DT_ENCERRAMENTO timestamp_ntz,
    DT_INICIO_EXEC timestamp_ntz,
    DT_TERMINO_EXEC timestamp_ntz,
    DT_INICIO_PLAN_EXEC timestamp_ntz,
    DT_TERMINO_PLAN_EXEC timestamp_ntz,
    DESC_OS varchar(4000),
    DESC_ORD_STATUS varchar(100),
    VL_LATITUDE number(23, 15),
    VL_LONGITUDE number(23, 15),
    FG_ORIGEM varchar(5),
    FG_STATUS number(38, 0),
    VL_ORDEM_SERVICO varchar(4000),
    TICKET_NUMBER number(38, 0),
    ETL_BATCH_ID varchar,
    BI_CREATED_AT timestamp_ntz,
    BI_UPDATED_AT timestamp_ntz,
    constraint UK_DS_SX_ORDEM_SERVICO_D unique (ID_CLIENTE, CD_ORDEM_SERVICO)
);


