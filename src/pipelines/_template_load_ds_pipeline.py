import argparse
from contextlib import closing
from datetime import datetime
from pathlib import Path
import re
import uuid
from zoneinfo import ZoneInfo

import oracledb
import pandas as pd
import snowflake.connector

from src.audit.audit_repository import AuditRepository
from src.common.config import Settings
from src.common.logger import get_logger
from src.common.snowflake_auth import build_snowflake_connection_kwargs, validate_snowflake_auth_settings
from src.load.snowflake_loader import SnowflakeLoader

logger = get_logger(__name__)
_ORACLE_CLIENT_INITIALIZED = False
BRAZIL_TIMEZONE = ZoneInfo("America/Sao_Paulo")

# ============================================================================
# TEMPLATE DE PIPELINE DS
#
# COMO USAR:
# 1. Copie este arquivo
# 2. Renomeie, por exemplo, para: load_sx_cidade_d.py
# 3. Troque apenas os blocos marcados em "TROQUE AQUI"
#
# O QUE ESTA PIPELINE FAZ:
# - recebe id_cliente e periodo
# - busca NAME_OWNER no Snowflake
# - extrai do Oracle
# - adiciona ETL_BATCH_ID e ETL_LOADED_AT
# - grava no DS do Snowflake
# - registra auditoria
#
# CAMPOS QUE VOCE NORMALMENTE VAI TROCAR:
# - PIPELINE_NAME
# - OUTPUT_FOLDER_NAME
# - TARGET_TABLE
# - SOURCE_NAME
# - TARGET_COLUMNS
# - ORACLE_EXTRACTION_QUERY
# ============================================================================

# ============================================================================
# TROQUE AQUI - IDENTIDADE DA PIPELINE
# ============================================================================
PIPELINE_NAME = "load_nome_da_tabela_d"
OUTPUT_FOLDER_NAME = "nome_da_tabela_d"
TARGET_TABLE = "SOLIX_BI.DS.NOME_DA_TABELA_D"
SOURCE_NAME = "ORACLE.NOME_DA_TABELA"

# ============================================================================
# TROQUE AQUI - COLUNAS DE DESTINO NO DS
# - mantenha na ordem exata da tabela Snowflake
# - inclua as colunas tecnicas obrigatorias
# ============================================================================
TARGET_COLUMNS = [
    "ID_CLIENTE",
    "CODIGO_NEGOCIO",
    "DESCRICAO_NEGOCIO",
    "ETL_BATCH_ID",
    "ETL_LOADED_AT",
]

# ============================================================================
# TROQUE AQUI - QUERY ORACLE
# REGRAS:
# - use {name_owner} quando a origem for segregada por cliente
# - retorne apenas colunas de negocio
# - deixe ETL_BATCH_ID e ETL_LOADED_AT para a funcao prepare_dataframe_for_load
# ============================================================================
ORACLE_EXTRACTION_QUERY = """
SELECT
    :id_cliente AS ID_CLIENTE,
    t.CODIGO_NEGOCIO,
    t.DESCRICAO_NEGOCIO
FROM {name_owner}.nome_tabela_oracle t
WHERE t.UPDATED_ON BETWEEN
      TO_DATE(:data_inicio, 'DD/MM/YYYY HH24:MI:SS')
  AND TO_DATE(:data_fim, 'DD/MM/YYYY HH24:MI:SS')
"""

# ============================================================================
# INFRAESTRUTURA COMPARTILHADA
# NORMALMENTE NAO PRECISA TROCAR NADA DAQUI PARA BAIXO
# ============================================================================
CLIENT_LOOKUP_QUERY = """
SELECT NAME_OWNER
FROM SOLIX_BI.DS.SX_CLIENTE_D
WHERE ID_CLIENTE = %(id_cliente)s
  AND FL_ATIVO = TRUE
"""


def validate_required_settings() -> None:
    required_settings = {
        "ORACLE_HOST": Settings.ORACLE_HOST,
        "ORACLE_SERVICE": Settings.ORACLE_SERVICE,
        "ORACLE_USER": Settings.ORACLE_USER,
        "ORACLE_PASSWORD": Settings.ORACLE_PASSWORD,
        "SNOWFLAKE_ACCOUNT": Settings.SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": Settings.SNOWFLAKE_USER,
        "SNOWFLAKE_WAREHOUSE": Settings.SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DATABASE": Settings.SNOWFLAKE_DATABASE,
        "SNOWFLAKE_SCHEMA": Settings.SNOWFLAKE_SCHEMA,
        "SNOWFLAKE_ROLE_INGESTAO": Settings.SNOWFLAKE_ROLE_INGESTAO,
        "SNOWFLAKE_STAGE": Settings.SNOWFLAKE_STAGE,
        "SNOWFLAKE_FILE_FORMAT": Settings.SNOWFLAKE_FILE_FORMAT,
    }

    missing = [name for name, value in required_settings.items() if not value]
    if missing:
        raise ValueError(
            "Variaveis obrigatorias ausentes no .env: " + ", ".join(sorted(missing))
        )

    valid_driver_modes = {"auto", "thin", "thick"}
    if Settings.ORACLE_DRIVER_MODE not in valid_driver_modes:
        raise ValueError(
            "ORACLE_DRIVER_MODE invalido. Use um destes valores: "
            + ", ".join(sorted(valid_driver_modes))
        )

    validate_snowflake_auth_settings()


def parse_date(date_text: str) -> datetime.date:
    try:
        return datetime.strptime(date_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Data invalida '{date_text}'. Use o formato YYYY-MM-DD."
        ) from exc


def get_snowflake_connection():
    logger.info("Abrindo conexao com Snowflake.")
    return snowflake.connector.connect(
        **build_snowflake_connection_kwargs(
            role=Settings.SNOWFLAKE_ROLE_INGESTAO,
        )
    )


def initialize_oracle_client() -> None:
    global _ORACLE_CLIENT_INITIALIZED

    if _ORACLE_CLIENT_INITIALIZED:
        return

    oracle_driver_mode = Settings.ORACLE_DRIVER_MODE
    oracle_client_lib_dir = Settings.ORACLE_CLIENT_LIB_DIR

    if oracle_driver_mode == "thin":
        logger.info(
            "ORACLE_DRIVER_MODE=thin. Usando modo thin do python-oracledb sem Oracle Client local."
        )
        _ORACLE_CLIENT_INITIALIZED = True
        return

    if oracle_driver_mode == "thick":
        if not oracle_client_lib_dir:
            raise ValueError(
                "ORACLE_DRIVER_MODE=thick exige que ORACLE_CLIENT_LIB_DIR esteja preenchido."
            )
        logger.info(
            "ORACLE_DRIVER_MODE=thick. Inicializando Oracle Client com lib_dir=%s.",
            oracle_client_lib_dir,
        )
        oracledb.init_oracle_client(lib_dir=oracle_client_lib_dir)
        _ORACLE_CLIENT_INITIALIZED = True
        return

    client_path = Path(oracle_client_lib_dir).expanduser() if oracle_client_lib_dir else None
    if client_path and client_path.exists():
        logger.info(
            "ORACLE_DRIVER_MODE=auto. Oracle Client encontrado em %s. Usando modo thick.",
            oracle_client_lib_dir,
        )
        oracledb.init_oracle_client(lib_dir=oracle_client_lib_dir)
    elif oracle_client_lib_dir:
        logger.warning(
            "ORACLE_DRIVER_MODE=auto, mas ORACLE_CLIENT_LIB_DIR=%s nao existe neste ambiente. "
            "Fazendo fallback para thin mode.",
            oracle_client_lib_dir,
        )
    else:
        logger.info(
            "ORACLE_DRIVER_MODE=auto e ORACLE_CLIENT_LIB_DIR nao informado. "
            "Usando modo thin do python-oracledb."
        )

    _ORACLE_CLIENT_INITIALIZED = True


def get_oracle_connection():
    initialize_oracle_client()
    logger.info("Abrindo conexao com Oracle.")
    dsn = oracledb.makedsn(
        Settings.ORACLE_HOST,
        Settings.ORACLE_PORT,
        service_name=Settings.ORACLE_SERVICE,
    )
    return oracledb.connect(
        user=Settings.ORACLE_USER,
        password=Settings.ORACLE_PASSWORD,
        dsn=dsn,
    )


def get_name_owner(id_cliente: int) -> str:
    logger.info("Buscando NAME_OWNER do cliente %s no Snowflake.", id_cliente)

    with closing(get_snowflake_connection()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(CLIENT_LOOKUP_QUERY, {"id_cliente": id_cliente})
            row = cursor.fetchone()

    if not row:
        raise ValueError(
            f"Nenhum NAME_OWNER ativo foi encontrado para o id_cliente {id_cliente}."
        )

    name_owner = row[0]
    logger.info("NAME_OWNER encontrado: %s", name_owner)
    return name_owner


def validate_name_owner(name_owner: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name_owner):
        raise ValueError(f"NAME_OWNER invalido para uso na query Oracle: {name_owner}")
    return name_owner


def extract_data_from_oracle(
    id_cliente: int,
    name_owner: str,
    data_inicio: str,
    data_fim: str,
) -> pd.DataFrame:
    validated_name_owner = validate_name_owner(name_owner)
    logger.info(
        "Iniciando extracao no Oracle para NAME_OWNER=%s entre %s e %s.",
        validated_name_owner,
        data_inicio,
        data_fim,
    )

    params = {
        "id_cliente": id_cliente,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
    }
    query = ORACLE_EXTRACTION_QUERY.format(name_owner=validated_name_owner)

    with closing(get_oracle_connection()) as conn:
        dataframe = pd.read_sql(query, conn, params=params)

    logger.info("Extracao concluida com %s linhas.", len(dataframe))
    return dataframe


def prepare_dataframe_for_load(
    dataframe: pd.DataFrame,
    etl_batch_id: str,
    etl_loaded_at: str,
) -> pd.DataFrame:
    prepared = dataframe.copy()
    prepared["ETL_BATCH_ID"] = etl_batch_id
    prepared["ETL_LOADED_AT"] = etl_loaded_at
    return prepared[TARGET_COLUMNS]


def get_output_directory() -> Path:
    output_dir = Path.home() / "Documents" / "etl_bi_exports" / OUTPUT_FOLDER_NAME
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def build_output_file_path(id_cliente: int, data_inicio: str, data_fim: str) -> Path:
    normalized_start = data_inicio.replace("/", "").replace(":", "").replace(" ", "_")
    normalized_end = data_fim.replace("/", "").replace(":", "").replace(" ", "_")
    unique_suffix = uuid.uuid4().hex[:8]
    file_name = (
        f"{OUTPUT_FOLDER_NAME}_cliente_{id_cliente}_{normalized_start}_{normalized_end}_{unique_suffix}.csv"
    )
    return get_output_directory() / file_name


def save_dataframe_to_csv(dataframe: pd.DataFrame, output_file_path: Path) -> Path:
    logger.info("Salvando CSV em %s.", output_file_path)
    dataframe.to_csv(output_file_path, index=False, encoding="utf-8")
    return output_file_path


def load_data_to_snowflake(output_file_path: Path, id_cliente: int) -> None:
    logger.info("Enviando arquivo para o stage %s.", Settings.SNOWFLAKE_STAGE)

    loader = SnowflakeLoader()
    try:
        loader.delete_by_id_cliente(full_table_name=TARGET_TABLE, id_cliente=id_cliente)
        loader.upload_file_to_stage(
            local_file_path=str(output_file_path),
            stage_name=Settings.SNOWFLAKE_STAGE,
        )
        loader.copy_into_table(
            full_table_name=TARGET_TABLE,
            stage_name=Settings.SNOWFLAKE_STAGE,
            file_name=output_file_path.name,
            file_format_name=Settings.SNOWFLAKE_FILE_FORMAT,
            columns=TARGET_COLUMNS,
        )
    finally:
        loader.close()


def get_audit_repository() -> tuple[SnowflakeLoader, AuditRepository]:
    loader = SnowflakeLoader()
    repository = AuditRepository(loader)
    return loader, repository


def remove_local_file(output_file_path: Path) -> None:
    if output_file_path.exists():
        output_file_path.unlink()
        logger.info("Arquivo local temporario removido: %s.", output_file_path)


def run_pipeline(id_cliente: int, data_inicio_text: str, data_fim_text: str) -> dict[str, str | int]:
    output_file_path: Path | None = None
    batch_id: str | None = None
    audit_loader: SnowflakeLoader | None = None
    audit_repository: AuditRepository | None = None

    try:
        validate_required_settings()

        data_inicio = parse_date(data_inicio_text)
        data_fim = parse_date(data_fim_text)
        if data_inicio > data_fim:
            raise ValueError("data_inicio nao pode ser maior que data_fim.")

        batch_id = uuid.uuid4().hex
        etl_loaded_at = datetime.now(BRAZIL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        audit_dt_inicio = data_inicio.isoformat()
        audit_dt_fim = data_fim.isoformat()

        audit_loader, audit_repository = get_audit_repository()
        audit_repository.insert_batch_start(
            batch_id=batch_id,
            pipeline_name=PIPELINE_NAME,
            source_name=SOURCE_NAME,
            target_name=TARGET_TABLE,
            id_cliente=id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        name_owner = get_name_owner(id_cliente)
        extraction_start = data_inicio.strftime("%d/%m/%Y 00:00:00")
        extraction_end = data_fim.strftime("%d/%m/%Y 23:59:59")
        dataframe = extract_data_from_oracle(
            id_cliente=id_cliente,
            name_owner=name_owner,
            data_inicio=extraction_start,
            data_fim=extraction_end,
        )
        audit_repository.insert_audit_event(
            batch_id=batch_id,
            step_name="ORACLE_EXTRACT",
            source_name=SOURCE_NAME,
            target_name=TARGET_TABLE,
            status="SUCCESS",
            rows_processed=len(dataframe),
            details="Extracao Oracle concluida.",
            id_cliente=id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        dataframe = prepare_dataframe_for_load(
            dataframe=dataframe,
            etl_batch_id=batch_id,
            etl_loaded_at=etl_loaded_at,
        )
        output_file_path = build_output_file_path(
            id_cliente=id_cliente,
            data_inicio=extraction_start,
            data_fim=extraction_end,
        )
        save_dataframe_to_csv(dataframe, output_file_path)
        load_data_to_snowflake(output_file_path, id_cliente)

        audit_repository.insert_audit_event(
            batch_id=batch_id,
            step_name="SNOWFLAKE_LOAD",
            source_name=SOURCE_NAME,
            target_name=TARGET_TABLE,
            status="SUCCESS",
            rows_processed=len(dataframe),
            details="Carga no Snowflake concluida.",
            id_cliente=id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )
        audit_repository.update_batch_success(
            batch_id=batch_id,
            rows_extracted=len(dataframe),
            rows_loaded=len(dataframe),
        )

        return {
            "batch_id": batch_id,
            "id_cliente": id_cliente,
            "rows_extracted": len(dataframe),
            "rows_loaded": len(dataframe),
            "target_table": TARGET_TABLE,
        }
    except Exception as exc:
        if audit_repository and batch_id:
            try:
                audit_repository.insert_audit_event(
                    batch_id=batch_id,
                    step_name="PIPELINE_ERROR",
                    source_name=SOURCE_NAME,
                    target_name=TARGET_TABLE,
                    status="ERROR",
                    rows_processed=0,
                    details=str(exc),
                    id_cliente=id_cliente,
                    dt_inicio=data_inicio.isoformat(),
                    dt_fim=data_fim.isoformat(),
                )
                audit_repository.update_batch_error(batch_id=batch_id, error_message=str(exc))
            except Exception as audit_exc:
                logger.error("Falha ao registrar erro na auditoria: %s", audit_exc)
        logger.exception("Erro na execucao da pipeline: %s", exc)
        raise SystemExit(1) from exc
    finally:
        if output_file_path is not None:
            remove_local_file(output_file_path)
        if audit_loader is not None:
            audit_loader.close()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"Executa a pipeline {PIPELINE_NAME}.")
    parser.add_argument("--id_cliente", type=int, required=True)
    parser.add_argument("--data_inicio", type=str, required=True)
    parser.add_argument("--data_fim", type=str, required=True)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    run_pipeline(
        id_cliente=args.id_cliente,
        data_inicio_text=args.data_inicio,
        data_fim_text=args.data_fim,
    )


if __name__ == "__main__":
    main()
