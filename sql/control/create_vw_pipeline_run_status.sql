create or replace view SOLIX_BI.DS.VW_PIPELINE_RUN_STATUS as
with latest_batch as (
    select
        BATCH_ID,
        PIPELINE_NAME,
        SOURCE_NAME,
        TARGET_NAME,
        STATUS,
        STARTED_AT,
        ENDED_AT,
        ROWS_EXTRACTED,
        ROWS_LOADED,
        ROWS_AFFECTED,
        ERROR_MESSAGE,
        DURATION_SECONDS,
        ORCHESTRATION_TYPE,
        UPDATED_AT
    from SOLIX_BI.DS.CTL_BATCH_EXECUTION
    qualify row_number() over (
        partition by PIPELINE_NAME
        order by coalesce(STARTED_AT, UPDATED_AT) desc
    ) = 1
),
audit_summary as (
    select
        BATCH_ID,
        count(*) as AUDIT_EVENTS,
        count_if(STATUS = 'STARTED') as STARTED_EVENTS,
        count_if(STATUS = 'SUCCESS') as SUCCESS_EVENTS,
        count_if(STATUS = 'FAILED') as FAILED_EVENTS,
        min(STARTED_AT) as FIRST_STEP_STARTED_AT,
        max(ENDED_AT) as LAST_STEP_ENDED_AT,
        listagg(
            case when STATUS = 'FAILED' then STEP_NAME end,
            ', '
        ) within group (order by EXECUTION_ORDER, AUDIT_EVENT_ID) as FAILED_STEPS
    from SOLIX_BI.DS.CTL_LOAD_AUDIT
    group by BATCH_ID
)
select
    coalesce(w.PIPELINE_NAME, b.PIPELINE_NAME) as PIPELINE_NAME,
    b.BATCH_ID,
    b.SOURCE_NAME,
    b.TARGET_NAME,
    b.STATUS as BATCH_STATUS,
    w.LAST_RUN_STATUS as WATERMARK_STATUS,
    b.STARTED_AT as BATCH_STARTED_AT,
    b.ENDED_AT as BATCH_ENDED_AT,
    b.DURATION_SECONDS as BATCH_DURATION_SECONDS,
    b.ORCHESTRATION_TYPE,
    b.ROWS_EXTRACTED,
    b.ROWS_LOADED,
    b.ROWS_AFFECTED,
    w.LAST_BI_UPDATED_AT,
    w.LAST_SUCCESS_BATCH_ID,
    w.LAST_RUN_BATCH_ID,
    w.LAST_RUN_STARTED_AT,
    w.LAST_RUN_COMMITTED_AT,
    coalesce(b.ERROR_MESSAGE, w.LAST_ERROR_MESSAGE) as LAST_ERROR_MESSAGE,
    a.AUDIT_EVENTS,
    a.STARTED_EVENTS,
    a.SUCCESS_EVENTS,
    a.FAILED_EVENTS,
    a.FAILED_STEPS,
    a.FIRST_STEP_STARTED_AT,
    a.LAST_STEP_ENDED_AT,
    greatest(
        coalesce(b.UPDATED_AT, '1900-01-01'::timestamp_ntz),
        coalesce(w.UPDATED_AT, '1900-01-01'::timestamp_ntz)
    ) as UPDATED_AT
from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK w
full outer join latest_batch b
    on w.PIPELINE_NAME = b.PIPELINE_NAME
left join audit_summary a
    on b.BATCH_ID = a.BATCH_ID;
