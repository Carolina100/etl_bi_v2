import argparse
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
import uuid

import oracledb
import pandas as pd
import snowflake.connector

from src.audit.audit_repository import AuditRepository
from src.audit.watermark_repository import WatermarkRepository
from src.common.config import Settings
from src.common.logger import get_logger
from src.common.snowflake_auth import build_snowflake_connection_kwargs, validate_snowflake_auth_settings
from src.load.snowflake_loader import SnowflakeLoader

logger = get_logger(__name__)
_ORACLE_CLIENT_INITIALIZED = False
UTC_TIMEZONE = timezone.utc

# ============================================================================
# CONFIGURACAO DA PIPELINE
#
# ESTE ARQUIVO IMPLEMENTA A CARGA DA ENTIDADE SX_OPERACAO_D NO NOVO PADRAO
# INCREMENTAL.
#
# O QUE ESTA PIPELINE FAZ:
# 1. recebe o id_cliente
# 2. decide o modo de carga:
#    - INCREMENTAL_WATERMARK: quando nao ha data manual
#    - MANUAL_BACKFILL: quando data_inicio e data_fim sao informadas
# 3. busca o NAME_OWNER do cliente no Snowflake
# 4. le no Snowflake o ultimo watermark processado para o cliente
# 5. consulta no Oracle apenas os registros alterados no periodo efetivo
# 6. adiciona ETL_BATCH_ID, BI_CREATED_AT e BI_UPDATED_AT
#    - BI_CREATED_AT: primeira entrada no BI
#    - BI_UPDATED_AT: ultima atualizacao no BI
# 7. gera CSV temporario
# 8. envia o arquivo para o stage do Snowflake
# 9. aplica MERGE no DS pela chave natural
# 10. atualiza o watermark somente em caso de sucesso
# 11. registra auditoria de batch e eventos
#
# DIFERENCA PARA O MODELO ANTIGO:
# - antes a carga dependia sempre de data_inicio e data_fim informadas manualmente
# - antes o DS fazia DELETE por cliente e depois COPY completo
# - agora o modo padrao e incremental por UPDATED_ON da origem
# - agora o DS faz MERGE incremental, preservando registros que nao mudaram
# - agora o DS preserva BI_CREATED_AT e atualiza BI_UPDATED_AT a cada merge
#
# QUANDO USAR CADA MODO:
# - INCREMENTAL_WATERMARK:
#   uso normal do dia a dia
# - MANUAL_BACKFILL:
#   reprocessamento historico, contingencia ou correcao de dados
#
# REGRAS IMPORTANTES:
# - o watermark so avanca quando a carga termina com sucesso
# - o backfill manual continua disponivel como excecao operacional
# - a chave natural do DS para esta entidade e:
#   ID_CLIENTE + CD_OPERACAO
# ============================================================================

PIPELINE_NAME = "load_sx_operacao_d"
OUTPUT_FOLDER_NAME = "sx_operacao_d"
TARGET_TABLE = "SOLIX_BI.DS.SX_OPERACAO_D"
SOURCE_NAME = "ORACLE.CDT_OPERACAO"
TARGET_COLUMNS = [
    "ID_CLIENTE",
    "CD_OPERACAO",
    "DESC_OPERACAO",
    "CD_GRUPO_OPERACAO",
    "DESC_GRUPO_OPERACAO",
    "CD_GRUPO_PARADA",
    "DESC_GRUPO_PARADA",
    "FG_TIPO_OPERACAO",
    "CD_PROCESSO_TALHAO",
    "DESC_PROCESSO_TALHAO",
    "FG_ATIVO",
    "ETL_BATCH_ID",
    "BI_CREATED_AT",
    "BI_UPDATED_AT",
]
NATURAL_KEY_COLUMNS = ["ID_CLIENTE", "CD_OPERACAO"]
SOURCE_UPDATED_AT_COLUMN = "SOURCE_UPDATED_ON"
LOAD_MODE_INCREMENTAL = "INCREMENTAL_WATERMARK"
LOAD_MODE_MANUAL = "MANUAL_BACKFILL"
LOAD_MODE_FULL = "FULL_RECONCILIATION"
INITIAL_INCREMENTAL_START = datetime(1900, 1, 1, 0, 0, 0)
INCREMENTAL_LOOKBACK_MINUTES = 10

ORACLE_EXTRACTION_QUERY = """
WITH base AS (
    SELECT
         :id_cliente AS ID_CLIENTE
        ,o.cd_operacao AS CD_OPERACAO
        ,o.desc_operac AS DESC_OPERACAO
        ,NVL(o.cd_grupo_operac, -1) AS CD_GRUPO_OPERACAO
        ,NVL(go.desc_grupo_operac, '* UNDEFINED *') AS DESC_GRUPO_OPERACAO
        ,NVL(o.cd_grupo_parada, -1) AS CD_GRUPO_PARADA
        ,NVL(gp.desc_grupo_parada, '* UNDEFINED *') AS DESC_GRUPO_PARADA
        ,NVL(o.fg_tipo_operac, '-1') AS FG_TIPO_OPERACAO
        ,COALESCE(o.cd_processo, -1) AS CD_PROCESSO_TALHAO
        ,COALESCE(pt.desc_processo, '* UNDEFINED *') AS DESC_PROCESSO_TALHAO
        ,GREATEST(
            NVL(o.UPDATED_ON,  TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
            NVL(go.UPDATED_ON, TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
            NVL(gp.UPDATED_ON, TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
            NVL(pt.UPDATED_ON, TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
        ) AS SOURCE_UPDATED_ON
    FROM {name_owner}.cdt_operacao o
    LEFT JOIN {name_owner}.cdt_grupo_operacao go
        ON o.cd_grupo_operac = go.cd_grupo_operac
    LEFT JOIN {name_owner}.cdt_grupo_parada gp
        ON o.cd_grupo_parada = gp.cd_grupo_parada
    LEFT JOIN {name_owner}.cdt_processos_talhao pt
        ON o.cd_processo = pt.cd_processo
),
ranked AS (
    SELECT
        base.*,
        ROW_NUMBER() OVER (
            PARTITION BY ID_CLIENTE, CD_OPERACAO
            ORDER BY SOURCE_UPDATED_ON DESC
        ) AS RN
    FROM base
)
SELECT
    ID_CLIENTE,
    CD_OPERACAO,
    DESC_OPERACAO,
    CD_GRUPO_OPERACAO,
    DESC_GRUPO_OPERACAO,
    CD_GRUPO_PARADA,
    DESC_GRUPO_PARADA,
    FG_TIPO_OPERACAO,
    CD_PROCESSO_TALHAO,
    DESC_PROCESSO_TALHAO,
    SOURCE_UPDATED_ON
FROM ranked
WHERE RN = 1
  AND SOURCE_UPDATED_ON >= TO_TIMESTAMP(:updated_on_start, 'DD/MM/YYYY HH24:MI:SS')
  AND SOURCE_UPDATED_ON < TO_TIMESTAMP(:updated_on_end, 'DD/MM/YYYY HH24:MI:SS')

"""

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


def parse_date(date_text: str) -> date:
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
    updated_on_start: str,
    updated_on_end: str,
) -> pd.DataFrame:
    validated_name_owner = validate_name_owner(name_owner)
    logger.info(
        "Iniciando extracao no Oracle para NAME_OWNER=%s entre %s e %s.",
        validated_name_owner,
        updated_on_start,
        updated_on_end,
    )

    params = {
        "id_cliente": id_cliente,
        "updated_on_start": updated_on_start,
        "updated_on_end": updated_on_end,
    }

    query = ORACLE_EXTRACTION_QUERY.format(name_owner=validated_name_owner)

    with closing(get_oracle_connection()) as conn:
        dataframe = pd.read_sql(query, conn, params=params)

    logger.info("Extracao concluida com %s linhas.", len(dataframe))
    return dataframe


def prepare_dataframe_for_load(
    dataframe: pd.DataFrame,
    etl_batch_id: str,
    bi_timestamp: str,
) -> pd.DataFrame:
    prepared = dataframe.copy()
    prepared["FG_ATIVO"] = 1
    prepared["ETL_BATCH_ID"] = etl_batch_id
    prepared["BI_CREATED_AT"] = bi_timestamp
    prepared["BI_UPDATED_AT"] = bi_timestamp
    prepared = prepared[TARGET_COLUMNS]
    return prepared


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


def load_data_to_snowflake(
    output_file_path: Path,
    *,
    id_cliente: int,
    full_reconciliation: bool,
) -> None:
    logger.info("Enviando arquivo para o stage %s.", Settings.SNOWFLAKE_STAGE)

    loader = SnowflakeLoader()
    try:
        loader.upload_file_to_stage(
            local_file_path=str(output_file_path),
            stage_name=Settings.SNOWFLAKE_STAGE,
        )
        logger.info("Executando MERGE incremental na tabela %s.", TARGET_TABLE)
        loader.merge_file_into_table(
            full_table_name=TARGET_TABLE,
            stage_name=Settings.SNOWFLAKE_STAGE,
            file_name=output_file_path.name,
            file_format_name=Settings.SNOWFLAKE_FILE_FORMAT,
            columns=TARGET_COLUMNS,
            key_columns=NATURAL_KEY_COLUMNS,
            preserve_on_update_columns=["BI_CREATED_AT"],
            update_current_timestamp_columns=["BI_UPDATED_AT"],
            mark_missing_as_inactive=full_reconciliation,
            inactive_flag_column="FG_ATIVO",
            scope_condition_sql=f"t.ID_CLIENTE = {int(id_cliente)}",
        )
    finally:
        loader.close()


def get_audit_repository() -> tuple[SnowflakeLoader, AuditRepository, WatermarkRepository]:
    loader = SnowflakeLoader()
    audit_repository = AuditRepository(loader)
    watermark_repository = WatermarkRepository(loader)
    return loader, audit_repository, watermark_repository


def remove_local_file(output_file_path: Path) -> None:
    if output_file_path.exists():
        output_file_path.unlink()
        logger.info("Arquivo local temporario removido: %s.", output_file_path)


def format_oracle_timestamp(value: datetime) -> str:
    return value.strftime("%d/%m/%Y %H:%M:%S")


def normalize_source_updated_on(dataframe: pd.DataFrame) -> datetime | None:
    if dataframe.empty or SOURCE_UPDATED_AT_COLUMN not in dataframe.columns:
        return None

    parsed = pd.to_datetime(dataframe[SOURCE_UPDATED_AT_COLUMN], errors="coerce")
    max_timestamp = parsed.max()
    if pd.isna(max_timestamp):
        return None

    return max_timestamp.to_pydatetime().replace(tzinfo=None)


def resolve_load_window(
    *,
    id_cliente: int,
    watermark_repository: WatermarkRepository,
    data_inicio_text: str | None,
    data_fim_text: str | None,
    full_reconciliation: bool,
) -> dict[str, object]:
    manual_range_informed = bool(data_inicio_text or data_fim_text)
    if manual_range_informed and not (data_inicio_text and data_fim_text):
        raise ValueError("Informe data_inicio e data_fim juntos para executar backfill manual.")
    if manual_range_informed and full_reconciliation:
        raise ValueError(
            "Use datas para backfill manual ou full_reconciliation, mas nao os dois ao mesmo tempo."
        )

    if full_reconciliation:
        extraction_ended_at = datetime.now(UTC_TIMEZONE).replace(tzinfo=None)
        return {
            "load_mode": LOAD_MODE_FULL,
            "manual_data_inicio": None,
            "manual_data_fim": None,
            "extraction_started_at": INITIAL_INCREMENTAL_START,
            "extraction_ended_at": extraction_ended_at,
            "last_watermark": watermark_repository.get_last_source_updated_at(
                pipeline_name=PIPELINE_NAME,
                id_cliente=id_cliente,
            ),
        }

    if data_inicio_text and data_fim_text:
        data_inicio = parse_date(data_inicio_text)
        data_fim = parse_date(data_fim_text)
        if data_inicio > data_fim:
            raise ValueError("data_inicio nao pode ser maior que data_fim.")

        extraction_started_at = datetime.combine(data_inicio, datetime.min.time())
        extraction_ended_at = datetime.combine(data_fim + timedelta(days=1), datetime.min.time())
        return {
            "load_mode": LOAD_MODE_MANUAL,
            "manual_data_inicio": data_inicio.isoformat(),
            "manual_data_fim": data_fim.isoformat(),
            "extraction_started_at": extraction_started_at,
            "extraction_ended_at": extraction_ended_at,
            "last_watermark": None,
        }

    last_watermark = watermark_repository.get_last_source_updated_at(
        pipeline_name=PIPELINE_NAME,
        id_cliente=id_cliente,
    )
    extraction_ended_at = datetime.now(UTC_TIMEZONE).replace(tzinfo=None)
    extraction_started_at = INITIAL_INCREMENTAL_START
    if last_watermark:
        extraction_started_at = last_watermark - timedelta(minutes=INCREMENTAL_LOOKBACK_MINUTES)

    return {
        "load_mode": LOAD_MODE_INCREMENTAL,
        "manual_data_inicio": None,
        "manual_data_fim": None,
        "extraction_started_at": extraction_started_at,
        "extraction_ended_at": extraction_ended_at,
        "last_watermark": last_watermark,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"Executa a pipeline {PIPELINE_NAME}."
    )
    parser.add_argument(
        "--id_cliente",
        type=int,
        required=True,
        help="Identificador do cliente na tabela SOLIX_BI.DS.SX_CLIENTE_D.",
    )
    parser.add_argument(
        "--data_inicio",
        type=str,
        required=False,
        help="Data inicial no formato YYYY-MM-DD. Opcional para backfill manual.",
    )
    parser.add_argument(
        "--data_fim",
        type=str,
        required=False,
        help="Data final no formato YYYY-MM-DD. Opcional para backfill manual.",
    )
    parser.add_argument(
        "--full_reconciliation",
        action="store_true",
        help="Executa foto completa da fonte e inativa no DS os registros ausentes para o cliente.",
    )
    return parser


def run_pipeline(
    id_cliente: int,
    data_inicio_text: str | None = None,
    data_fim_text: str | None = None,
    full_reconciliation: bool = False,
) -> dict[str, str | int | None]:
    output_file_path: Path | None = None
    batch_id: str | None = None
    audit_loader: SnowflakeLoader | None = None
    audit_repository: AuditRepository | None = None
    watermark_repository: WatermarkRepository | None = None
    audit_dt_inicio = datetime.now(UTC_TIMEZONE).isoformat()
    audit_dt_fim = audit_dt_inicio
    resolved_load_mode = LOAD_MODE_INCREMENTAL

    try:
        validate_required_settings()

        batch_id = uuid.uuid4().hex
        etl_batch_id = batch_id
        bi_timestamp = datetime.now(UTC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("BATCH_ID gerado para a carga: %s.", batch_id)
        logger.info("BI timestamps gerados para a carga: %s.", bi_timestamp)

        audit_loader, audit_repository, watermark_repository = get_audit_repository()
        load_window = resolve_load_window(
            id_cliente=id_cliente,
            watermark_repository=watermark_repository,
            data_inicio_text=data_inicio_text,
            data_fim_text=data_fim_text,
            full_reconciliation=full_reconciliation,
        )
        watermark_repository.mark_run_started(
            pipeline_name=PIPELINE_NAME,
            id_cliente=id_cliente,
            batch_id=batch_id,
            run_started_at=datetime.now(UTC_TIMEZONE).replace(tzinfo=None),
        )

        resolved_load_mode = str(load_window["load_mode"])
        extraction_started_at = load_window["extraction_started_at"]
        extraction_ended_at = load_window["extraction_ended_at"]
        audit_dt_inicio = extraction_started_at.isoformat()
        audit_dt_fim = extraction_ended_at.isoformat()
        extraction_start = format_oracle_timestamp(extraction_started_at)
        extraction_end = format_oracle_timestamp(extraction_ended_at)

        logger.info(
            "Modo de carga resolvido para id_cliente=%s: %s. Janela efetiva: %s -> %s. Ultimo watermark=%s",
            id_cliente,
            resolved_load_mode,
            extraction_start,
            extraction_end,
            load_window["last_watermark"],
        )

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
        dataframe = extract_data_from_oracle(
            id_cliente=id_cliente,
            name_owner=name_owner,
            updated_on_start=extraction_start,
            updated_on_end=extraction_end,
        )
        max_source_updated_on = normalize_source_updated_on(dataframe)
        extract_details = (
            f"Extracao concluida para id_cliente={id_cliente}, "
            f"name_owner={name_owner}, load_mode={resolved_load_mode}, "
            f"janela={extraction_start} a {extraction_end}, "
            f"max_source_updated_on={max_source_updated_on}."
        )
        if dataframe.empty:
            extract_details = (
                f"Extracao concluida sem dados para id_cliente={id_cliente}, "
                f"name_owner={name_owner}, load_mode={resolved_load_mode}, "
                f"janela={extraction_start} a {extraction_end}. "
                "Nenhum registro foi encontrado na origem para esta entidade "
                "e nenhuma linha sera aplicada no DS."
            )

        audit_repository.insert_audit_event(
            batch_id=batch_id,
            step_name="ORACLE_EXTRACT",
            source_name=SOURCE_NAME,
            target_name=TARGET_TABLE,
            status="SUCCESS",
            rows_processed=len(dataframe),
            details=extract_details,
            id_cliente=id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        rows_loaded = 0
        should_apply_to_ds = full_reconciliation or not dataframe.empty
        if should_apply_to_ds:
            dataframe_to_load = prepare_dataframe_for_load(
                dataframe=dataframe,
                etl_batch_id=etl_batch_id,
                bi_timestamp=bi_timestamp,
            )
            output_file_path = build_output_file_path(
                id_cliente=id_cliente,
                data_inicio=extraction_start,
                data_fim=extraction_end,
            )
            save_dataframe_to_csv(dataframe_to_load, output_file_path)
            load_data_to_snowflake(
                output_file_path,
                id_cliente=id_cliente,
                full_reconciliation=full_reconciliation,
            )
            rows_loaded = len(dataframe_to_load)
            logger.info(
                "Carga concluida para id_cliente=%s. Linhas aplicadas no DS=%s. load_mode=%s.",
                id_cliente,
                rows_loaded,
                resolved_load_mode,
            )
        else:
            logger.info(
                "Nenhuma alteracao encontrada para id_cliente=%s na janela %s -> %s.",
                id_cliente,
                extraction_start,
                extraction_end,
            )

        audit_repository.insert_audit_event(
            batch_id=batch_id,
            step_name="SNOWFLAKE_MERGE",
            source_name=SOURCE_NAME,
            target_name=TARGET_TABLE,
            status="SUCCESS",
            rows_processed=rows_loaded,
            details=(
                f"Merge concluido na tabela {TARGET_TABLE} para id_cliente={id_cliente}. "
                f"load_mode={resolved_load_mode}. rows_loaded={rows_loaded}. "
                + (
                    "Nenhuma linha foi aplicada porque nao ha registros na origem para esta entidade."
                    if rows_loaded == 0
                    else "Linhas da janela processada foram reconciliadas com sucesso no DS."
                )
            ),
            id_cliente=id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        if max_source_updated_on is not None:
            watermark_repository.upsert_success_watermark(
                pipeline_name=PIPELINE_NAME,
                id_cliente=id_cliente,
                last_source_updated_at=max_source_updated_on,
                batch_id=batch_id,
                load_mode=resolved_load_mode,
                extraction_started_at=extraction_started_at,
                extraction_ended_at=extraction_ended_at,
                run_committed_at=datetime.now(UTC_TIMEZONE).replace(tzinfo=None),
            )
            logger.info(
                "Watermark atualizado para id_cliente=%s com SOURCE_UPDATED_ON=%s.",
                id_cliente,
                max_source_updated_on,
            )

        audit_repository.update_batch_success(
            batch_id=batch_id,
            rows_extracted=len(dataframe),
            rows_loaded=rows_loaded,
        )

        logger.info(
            "Pipeline finalizada com sucesso para id_cliente=%s. Total de linhas extraidas: %s.",
            id_cliente,
            len(dataframe),
        )
        return {
            "batch_id": batch_id,
            "id_cliente": id_cliente,
            "rows_extracted": len(dataframe),
            "rows_loaded": rows_loaded,
            "target_table": TARGET_TABLE,
            "load_mode": resolved_load_mode,
            "window_start": audit_dt_inicio,
            "window_end": audit_dt_fim,
            "max_source_updated_on": max_source_updated_on.isoformat() if max_source_updated_on else None,
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
                    details=f"load_mode={resolved_load_mode}. erro={exc}",
                    id_cliente=id_cliente,
                    dt_inicio=audit_dt_inicio,
                    dt_fim=audit_dt_fim,
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


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    run_pipeline(
        id_cliente=args.id_cliente,
        data_inicio_text=args.data_inicio,
        data_fim_text=args.data_fim,
        full_reconciliation=args.full_reconciliation,
    )


if __name__ == "__main__":
    main()
