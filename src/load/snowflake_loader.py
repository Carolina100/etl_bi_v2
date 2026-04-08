from pathlib import Path
import uuid
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

    def fetch_one(self, sql: str):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def fetch_all(self, sql: str):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def upload_file_to_stage(self, local_file_path: str, stage_name: str) -> None:
        resolved = Path(local_file_path).resolve().as_posix()
        sql = f"PUT file://{resolved} {stage_name} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def remove_file_from_stage(self, stage_name: str, file_name: str) -> None:
        self.execute(f"REMOVE {stage_name}/{file_name}")

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

    def merge_file_into_table(
        self,
        *,
        full_table_name: str,
        stage_name: str,
        file_name: str,
        file_format_name: str,
        columns: list[str],
        key_columns: list[str],
        preserve_on_update_columns: list[str] | None = None,
        update_current_timestamp_columns: list[str] | None = None,
        mark_missing_as_inactive: bool = False,
        inactive_flag_column: str = "FG_ATIVO",
        scope_condition_sql: str | None = None,
    ) -> None:
        temp_table_name = f"TMP_{uuid.uuid4().hex[:20].upper()}"
        preserve_on_update_columns = preserve_on_update_columns or []
        update_current_timestamp_columns = update_current_timestamp_columns or []
        update_columns = [column for column in columns if column not in key_columns]
        merge_condition = " AND ".join(f"t.{column} = s.{column}" for column in key_columns)
        update_assignments: list[str] = []
        for column in update_columns:
            if column in preserve_on_update_columns:
                update_assignments.append(f"t.{column} = COALESCE(t.{column}, s.{column})")
            elif column in update_current_timestamp_columns:
                update_assignments.append(
                    "t."
                    + column
                    + " = CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ"
                )
            else:
                update_assignments.append(f"t.{column} = s.{column}")

        update_set = ",\n            ".join(update_assignments)
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"s.{column}" for column in columns)
        copy_columns = " (\n            " + ",\n            ".join(columns) + "\n        )"

        self.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table_name} LIKE {full_table_name}")
        self.execute(
            f"""
            COPY INTO {temp_table_name}{copy_columns}
            FROM {stage_name}/{file_name}
            FILE_FORMAT = (
                FORMAT_NAME = {file_format_name}
                SKIP_HEADER = 1
            )
            """
        )
        self.execute(
            f"""
            MERGE INTO {full_table_name} AS t
            USING {temp_table_name} AS s
                ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
        )
        if mark_missing_as_inactive:
            if not scope_condition_sql:
                raise ValueError(
                    "scope_condition_sql e obrigatorio quando mark_missing_as_inactive=True."
                )

            inactive_assignments = [f"t.{inactive_flag_column} = 0"]
            for column in update_current_timestamp_columns:
                inactive_assignments.append(
                    "t."
                    + column
                    + " = CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ"
                )

            inactive_set = ",\n                    ".join(inactive_assignments)
            self.execute(
                f"""
                UPDATE {full_table_name} AS t
                SET
                    {inactive_set}
                WHERE {scope_condition_sql}
                  AND COALESCE(t.{inactive_flag_column}, 1) <> 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {temp_table_name} AS s
                      WHERE {merge_condition}
                  )
                """
            )
        self.remove_file_from_stage(stage_name=stage_name, file_name=file_name)
