create or replace transient table SOLIX_BI.DS.CTL_BATCH_EXECUTION (
    BATCH_ID varchar not null,
    PIPELINE_NAME varchar not null,
    SOURCE_NAME varchar not null,
    TARGET_NAME varchar not null,
    STATUS varchar not null,
    STARTED_AT timestamp_ntz,
    ENDED_AT timestamp_ntz,
    ROWS_EXTRACTED number(38, 0),
    ROWS_LOADED number(38, 0),
    ROWS_AFFECTED number(38, 0),
    ERROR_MESSAGE varchar,
    DURATION_SECONDS number(38, 0),
    ORCHESTRATION_TYPE varchar,
    CREATED_AT timestamp_ntz not null default convert_timezone('UTC', current_timestamp())::timestamp_ntz,
    UPDATED_AT timestamp_ntz not null default convert_timezone('UTC', current_timestamp())::timestamp_ntz,
    constraint PK_CTL_BATCH_EXECUTION primary key (BATCH_ID)
);
