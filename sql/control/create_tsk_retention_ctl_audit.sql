-- ============================================================================
-- TASK DE RETENCAO DAS TABELAS DE AUDITORIA
-- ----------------------------------------------------------------------------
-- Objetivo:
-- manter apenas o dia atual e o dia anterior nas tabelas operacionais
-- de auditoria, com execucao semanal no Snowflake.
--
-- Agendamento:
-- domingo, 22:00, fuso America/Sao_Paulo
--
-- Politica:
-- se rodar em 24/04/2026, mantem 24/04/2026 e 23/04/2026
-- remove 22/04/2026 para tras
-- ============================================================================

create or replace task SOLIX_BI.DS.TSK_RETENTION_CTL_AUDIT
warehouse = WH_ETL
schedule = 'USING CRON 0 22 * * 0 America/Sao_Paulo'
as
begin
    delete from SOLIX_BI.DS.CTL_LOAD_AUDIT
    where cast(EVENT_TIME as date) < dateadd(day, -1, current_date());

    delete from SOLIX_BI.DS.CTL_BATCH_EXECUTION
    where cast(STARTED_AT as date) < dateadd(day, -1, current_date());
end;

-- Ativar a task
alter task SOLIX_BI.DS.TSK_RETENTION_CTL_AUDIT resume;

-- Pausar a task, se necessario
-- alter task SOLIX_BI.DS.TSK_RETENTION_CTL_AUDIT suspend;
