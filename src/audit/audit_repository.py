from src.load.snowflake_loader import SnowflakeLoader

class AuditRepository:
    BRAZIL_NOW_SQL = "CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ"

    def __init__(self, loader: SnowflakeLoader) -> None:
        self.loader = loader

    def insert_batch_start(
        self,
        batch_id: str,
        pipeline_name: str,
        source_name: str,
        target_name: str,
        id_cliente: int,
        dt_inicio: str,
        dt_fim: str,
    ) -> None:
        sql = f"""
        INSERT INTO SOLIX_BI.DS.CTL_BATCH_EXECUTION (
            BATCH_ID, PIPELINE_NAME, SOURCE_NAME, TARGET_NAME, STATUS, STARTED_AT,
            ID_CLIENTE, DT_INICIO, DT_FIM
        )
        VALUES (
            '{batch_id}', '{pipeline_name}', '{source_name}', '{target_name}', 'RUNNING', {self.BRAZIL_NOW_SQL},
            {id_cliente}, '{dt_inicio}', '{dt_fim}'
        )
        """
        self.loader.execute(sql)

    def update_batch_success(self, batch_id: str, rows_extracted: int, rows_loaded: int) -> None:
        sql = f"""
        UPDATE SOLIX_BI.DS.CTL_BATCH_EXECUTION
        SET STATUS = 'SUCCESS',
            ENDED_AT = {self.BRAZIL_NOW_SQL},
            ROWS_EXTRACTED = {rows_extracted},
            ROWS_LOADED = {rows_loaded},
            ERROR_MESSAGE = NULL
        WHERE BATCH_ID = '{batch_id}'
        """
        self.loader.execute(sql)

    def update_batch_error(self, batch_id: str, error_message: str) -> None:
        msg = error_message.replace("'", "''")
        sql = f"""
        UPDATE SOLIX_BI.DS.CTL_BATCH_EXECUTION
        SET STATUS = 'ERROR',
            ENDED_AT = {self.BRAZIL_NOW_SQL},
            ERROR_MESSAGE = '{msg}'
        WHERE BATCH_ID = '{batch_id}'
        """
        self.loader.execute(sql)

    def insert_audit_event(
        self,
        batch_id: str,
        step_name: str,
        source_name: str,
        target_name: str,
        status: str,
        rows_processed: int,
        details: str,
        id_cliente: int,
        dt_inicio: str,
        dt_fim: str,
    ) -> None:
        det = details.replace("'", "''")
        sql = f"""
        INSERT INTO SOLIX_BI.DS.CTL_LOAD_AUDIT (
            BATCH_ID, STEP_NAME, SOURCE_NAME, TARGET_NAME, STATUS, ROWS_PROCESSED, EVENT_TIME, DETAILS,
            ID_CLIENTE, DT_INICIO, DT_FIM
        )
        VALUES (
            '{batch_id}', '{step_name}', '{source_name}', '{target_name}', '{status}', {rows_processed}, {self.BRAZIL_NOW_SQL}, '{det}',
            {id_cliente}, '{dt_inicio}', '{dt_fim}'
        )
        """
        self.loader.execute(sql)
