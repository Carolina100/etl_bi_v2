from __future__ import annotations

from contextlib import closing

import pandas as pd
import psycopg

from src.common.config import Settings
from src.common.logger import get_logger

logger = get_logger(__name__)


def validate_postgres_settings() -> None:
    required_settings = {
        "POSTGRES_HOST": Settings.POSTGRES_HOST,
        "POSTGRES_DATABASE": Settings.POSTGRES_DATABASE,
        "POSTGRES_USER": Settings.POSTGRES_USER,
        "POSTGRES_PASSWORD": Settings.POSTGRES_PASSWORD,
    }

    missing = [name for name, value in required_settings.items() if not value]
    if missing:
        raise ValueError(
            "Variaveis obrigatorias ausentes para PostgreSQL: " + ", ".join(sorted(missing))
        )


def get_postgres_connection():
    validate_postgres_settings()
    logger.info(
        "Abrindo conexao com PostgreSQL em host=%s, database=%s, schema=%s.",
        Settings.POSTGRES_HOST,
        Settings.POSTGRES_DATABASE,
        Settings.POSTGRES_SCHEMA,
    )
    return psycopg.connect(
        host=Settings.POSTGRES_HOST,
        port=Settings.POSTGRES_PORT,
        dbname=Settings.POSTGRES_DATABASE,
        user=Settings.POSTGRES_USER,
        password=Settings.POSTGRES_PASSWORD,
    )


def read_sql_from_postgres(query: str, params: dict | None = None) -> pd.DataFrame:
    with closing(get_postgres_connection()) as conn:
        return pd.read_sql(query, conn, params=params)
