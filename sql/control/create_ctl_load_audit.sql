create or replace transient table SOLIX_BI.DS.CTL_LOAD_AUDIT (
    AUDIT_EVENT_ID number(38, 0) autoincrement start 1 increment 1 not null,
    BATCH_ID varchar not null,
    STEP_NAME varchar not null,
    SOURCE_NAME varchar not null,
    TARGET_NAME varchar not null,
    STATUS varchar not null,
    ROWS_PROCESSED number(38, 0),
    EVENT_TIME timestamp_ntz not null default convert_timezone('UTC', current_timestamp())::timestamp_ntz,
    DETAILS varchar,
    EXECUTION_ORDER number(38, 0),
    DURATION_SECONDS number(38, 0),
    STARTED_AT timestamp_ntz,
    ENDED_AT timestamp_ntz,
    CREATED_AT timestamp_ntz not null default convert_timezone('UTC', current_timestamp())::timestamp_ntz,
    constraint PK_CTL_LOAD_AUDIT primary key (AUDIT_EVENT_ID)
);
