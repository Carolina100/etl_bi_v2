from pathlib import Path
import pandas as pd
import oracledb

from src.common.config import Settings

class OracleExtractor:
    def __init__(self) -> None:
        self.dsn = oracledb.makedsn(
            Settings.ORACLE_HOST,
            Settings.ORACLE_PORT,
            service_name=Settings.ORACLE_SERVICE
        )

    def read_sql_file(self, sql_path: str) -> str:
        return Path(sql_path).read_text(encoding="utf-8")

    def extract_to_dataframe(self, sql_path: str) -> pd.DataFrame:
        query = self.read_sql_file(sql_path)

        with oracledb.connect(
            user=Settings.ORACLE_USER,
            password=Settings.ORACLE_PASSWORD,
            dsn=self.dsn
        ) as conn:
            df = pd.read_sql(query, conn)

        return df