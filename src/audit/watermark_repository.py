from __future__ import annotations

from datetime import datetime

from src.load.snowflake_loader import SnowflakeLoader

WATERMARK_TABLE_NAME = "SOLIX_BI.DS.CTL_PIPELINE_WATERMARK"


class WatermarkRepository:
    def __init__(self, loader: SnowflakeLoader) -> None:
        self.loader = loader

    def get_last_source_updated_at(self, pipeline_name: str, id_cliente: int) -> datetime | None:
        sql = f"""
        SELECT LAST_SOURCE_UPDATED_AT
        FROM {WATERMARK_TABLE_NAME}
        WHERE PIPELINE_NAME = '{pipeline_name}'
          AND ID_CLIENTE = {id_cliente}
        """
        row = self.loader.fetch_one(sql)
        return row[0] if row else None

    def upsert_success_watermark(
        self,
        *,
        pipeline_name: str,
        id_cliente: int,
        last_source_updated_at: datetime,
        batch_id: str,
        load_mode: str,
        extraction_started_at: datetime,
        extraction_ended_at: datetime,
    ) -> None:
        formatted_source_updated_at = last_source_updated_at.strftime("%Y-%m-%d %H:%M:%S")
        formatted_extraction_started_at = extraction_started_at.strftime("%Y-%m-%d %H:%M:%S")
        formatted_extraction_ended_at = extraction_ended_at.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        MERGE INTO {WATERMARK_TABLE_NAME} AS target
        USING (
            SELECT
                '{pipeline_name}' AS PIPELINE_NAME,
                {id_cliente} AS ID_CLIENTE,
                TO_TIMESTAMP_NTZ('{formatted_source_updated_at}') AS LAST_SOURCE_UPDATED_AT,
                '{batch_id}' AS LAST_SUCCESS_BATCH_ID,
                '{load_mode}' AS LAST_LOAD_MODE,
                TO_TIMESTAMP_NTZ('{formatted_extraction_started_at}') AS LAST_EXTRACT_STARTED_AT,
                TO_TIMESTAMP_NTZ('{formatted_extraction_ended_at}') AS LAST_EXTRACT_ENDED_AT
        ) AS source
            ON target.PIPELINE_NAME = source.PIPELINE_NAME
           AND target.ID_CLIENTE = source.ID_CLIENTE
        WHEN MATCHED THEN
            UPDATE SET
                LAST_SOURCE_UPDATED_AT = source.LAST_SOURCE_UPDATED_AT,
                LAST_SUCCESS_BATCH_ID = source.LAST_SUCCESS_BATCH_ID,
                LAST_LOAD_MODE = source.LAST_LOAD_MODE,
                LAST_EXTRACT_STARTED_AT = source.LAST_EXTRACT_STARTED_AT,
                LAST_EXTRACT_ENDED_AT = source.LAST_EXTRACT_ENDED_AT,
                UPDATED_AT = CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
        WHEN NOT MATCHED THEN
            INSERT (
                PIPELINE_NAME,
                ID_CLIENTE,
                LAST_SOURCE_UPDATED_AT,
                LAST_SUCCESS_BATCH_ID,
                LAST_LOAD_MODE,
                LAST_EXTRACT_STARTED_AT,
                LAST_EXTRACT_ENDED_AT,
                UPDATED_AT
            )
            VALUES (
                source.PIPELINE_NAME,
                source.ID_CLIENTE,
                source.LAST_SOURCE_UPDATED_AT,
                source.LAST_SUCCESS_BATCH_ID,
                source.LAST_LOAD_MODE,
                source.LAST_EXTRACT_STARTED_AT,
                source.LAST_EXTRACT_ENDED_AT,
                CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
            )
        """
        self.loader.execute(sql)
