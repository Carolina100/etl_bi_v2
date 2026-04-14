create table if not exists SOLIX_BI.DS.CTL_PIPELINE_WATERMARK (
    PIPELINE_NAME varchar not null,
    ID_CLIENTE number(38, 0) not null default 0,
    LAST_BI_UPDATED_AT timestamp_ntz,
    LAST_SUCCESS_BATCH_ID varchar,
    LAST_LOAD_MODE varchar,
    LAST_EXTRACT_STARTED_AT timestamp_ntz,
    LAST_EXTRACT_ENDED_AT timestamp_ntz,
    LAST_RUN_BATCH_ID varchar,
    LAST_RUN_STARTED_AT timestamp_ntz,
    LAST_RUN_COMMITTED_AT timestamp_ntz,
    UPDATED_AT timestamp_ntz,
    constraint PK_CTL_PIPELINE_WATERMARK primary key (PIPELINE_NAME, ID_CLIENTE)
);
