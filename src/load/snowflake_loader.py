from pathlib import Path
import snowflake.connector

from src.common.config import Settings
from src.common.snowflake_auth import build_snowflake_connection_kwargs

class SnowflakeLoader:
    def __init__(self) -> None:
        self.conn = snowflake.connector.connect(
            **build_snowflake_connection_kwargs(
                role=Settings.SNOWFLAKE_ROLE_INGESTAO,
            )
        )

    def close(self) -> None:
        self.conn.close()

    def execute(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def upload_file_to_stage(self, local_file_path: str, stage_name: str) -> None:
        resolved = Path(local_file_path).resolve().as_posix()
        sql = f"PUT file://{resolved} {stage_name} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def delete_by_id_cliente(self, full_table_name: str, id_cliente: int) -> None:
        self.execute(f"DELETE FROM {full_table_name} WHERE ID_CLIENTE = {id_cliente}")

    def copy_into_table(
        self,
        full_table_name: str,
        stage_name: str,
        file_name: str,
        file_format_name: str,
        columns: list[str] | None = None,
    ) -> None:
        target_columns = ""
        if columns:
            target_columns = " (\n            " + ",\n            ".join(columns) + "\n        )"

        sql = f"""
        COPY INTO {full_table_name}{target_columns}
        FROM {stage_name}/{file_name}
        FILE_FORMAT = (
            FORMAT_NAME = {file_format_name}
            SKIP_HEADER = 1
        )
        """
        self.execute(sql)
