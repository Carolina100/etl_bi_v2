import argparse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import uuid

import pandas as pd

from src.audit.audit_repository import AuditRepository
from src.audit.watermark_repository import WatermarkRepository
from src.common.config import Settings
from src.common.logger import get_logger
from src.common.postgres import read_sql_from_postgres, validate_postgres_settings
from src.common.snowflake_auth import validate_snowflake_auth_settings
from src.load.snowflake_loader import SnowflakeLoader

logger = get_logger(__name__)
UTC_TIMEZONE = timezone.utc

PIPELINE_NAME = "load_sx_detalhes_operacao_f"
OUTPUT_FOLDER_NAME = "sx_detalhes_operacao_f"
TARGET_TABLE = "SOLIX_BI.DS.SX_DETALHES_OPERACAO_F"
SOURCE_NAME = "POSTGRES.PUBLIC.TEST_EXTRACT_BI_HORA"
TARGET_COLUMNS = [
    "ID_CLIENTE",
    "DT_DIA_FECHAMENTO",
    "DATA",
    "HORA",
    "CD_EQUIPAMENTO",
    "CD_GRUPO_EQUIPAMENTO",
    "CD_OPERACAO",
    "CD_OPERACAO_CB",
    "CD_ORDEM_SERVICO",
    "CD_UNIDADE",
    "CD_REGIONAL",
    "CD_CORPORATIVO",
    "CD_FAZENDA",
    "CD_ZONA",
    "CD_TALHAO",
    "CD_MISSAO",
    "CD_ESTADO",
    "VL_TEMPO_SEGUNDOS",
    "VL_VELOCIDADE_MEDIA",
    "VL_AREA_OPERACIONAL_M2",
    "FG_STATUS",
    "ETL_BATCH_ID",
    "BI_CREATED_AT",
    "BI_UPDATED_AT",
]
NATURAL_KEY_COLUMNS = [
    "ID_CLIENTE",
    "DT_DIA_FECHAMENTO",
    "DATA",
    "HORA",
    "CD_EQUIPAMENTO",
    "CD_GRUPO_EQUIPAMENTO",
    "CD_OPERACAO",
    "CD_OPERACAO_CB",
    "CD_ORDEM_SERVICO",
    "CD_UNIDADE",
    "CD_REGIONAL",
    "CD_CORPORATIVO",
    "CD_FAZENDA",
    "CD_ZONA",
    "CD_TALHAO",
    "CD_MISSAO",
    "CD_ESTADO",
]
SOURCE_UPDATED_AT_COLUMN = "SOURCE_UPDATED_ON"
LOAD_MODE_INCREMENTAL = "INCREMENTAL_WATERMARK"
LOAD_MODE_MANUAL = "MANUAL_BACKFILL"
INITIAL_INCREMENTAL_START = datetime(1900, 1, 1, 0, 0, 0)
INCREMENTAL_LOOKBACK_MINUTES = 10
FACT_STATUS_ACTIVE = "A"
FACT_STATUS_INACTIVE = "I"

POSTGRES_EXTRACTION_QUERY = """
WITH base AS (
    SELECT
        cd_cliente AS ID_CLIENTE,
        dt_dia_fechamento AS DT_DIA_FECHAMENTO,
        dt_hora::date AS DATA,
        EXTRACT(hour FROM dt_hora)::int AS HORA,
        coalesce(cd_equipamento, '-1') AS CD_EQUIPAMENTO,
        coalesce(cd_grupo_equipamento, '-1') AS CD_GRUPO_EQUIPAMENTO,
        coalesce(cd_operacao, '-1') AS CD_OPERACAO,
        coalesce(cd_operacao_cb, '-1') AS CD_OPERACAO_CB,
        coalesce(cd_ordem_servico,'-1') AS CD_ORDEM_SERVICO,
        coalesce(cd_unidade, '-1') AS CD_UNIDADE,
        coalesce(cd_regional, '-1') AS CD_REGIONAL,
        coalesce(cd_corporativo, '-1') AS CD_CORPORATIVO,
        coalesce(cd_fazenda, '-1') AS CD_FAZENDA,
        coalesce(cd_zona, '-1') AS CD_ZONA,
        coalesce(cd_talhao, '-1') AS CD_TALHAO,
        coalesce(cd_missao, '-1') AS CD_MISSAO,
        coalesce(cd_estado, '-1') AS CD_ESTADO,
        vl_tempo_segundos AS VL_TEMPO_SEGUNDOS,
        vl_velocidade_media AS VL_VELOCIDADE_MEDIA,
        vl_area_operacional_m2 AS VL_AREA_OPERACIONAL_M2,
        coalesce(fg_status, 'A') AS FG_STATUS,
        dt_updated AS SOURCE_UPDATED_ON
    FROM test_extract_bi_hora
    WHERE cd_cliente = %(p_cd_cliente)s
      AND dt_updated >= %(dt_updated_inicial)s
      AND dt_updated < %(dt_updated_final)s
),
ranked AS (
    SELECT
        base.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                ID_CLIENTE,
                DT_DIA_FECHAMENTO,
                DATA,
                HORA,
                CD_EQUIPAMENTO,
                CD_GRUPO_EQUIPAMENTO,
                CD_OPERACAO,
                CD_OPERACAO_CB,
                CD_ORDEM_SERVICO,
                CD_UNIDADE,
                CD_REGIONAL,
                CD_CORPORATIVO,
                CD_FAZENDA,
                CD_ZONA,
                CD_TALHAO,
                CD_MISSAO,
                CD_ESTADO
            ORDER BY SOURCE_UPDATED_ON DESC
        ) AS RN
    FROM base
)
SELECT
    ID_CLIENTE,
    DT_DIA_FECHAMENTO,
    DATA,
    HORA,
    CD_EQUIPAMENTO,
    CD_GRUPO_EQUIPAMENTO,
    CD_OPERACAO,
    CD_OPERACAO_CB,
    CD_ORDEM_SERVICO,
    CD_UNIDADE,
    CD_REGIONAL,
    CD_CORPORATIVO,
    CD_FAZENDA,
    CD_ZONA,
    CD_TALHAO,
    CD_MISSAO,
    CD_ESTADO,
    VL_TEMPO_SEGUNDOS,
    VL_VELOCIDADE_MEDIA,
    VL_AREA_OPERACIONAL_M2,
    FG_STATUS,
    SOURCE_UPDATED_ON
FROM ranked
WHERE RN = 1
"""


def validate_required_settings() -> None:
    validate_postgres_settings()
    required_settings = {
        "SNOWFLAKE_STAGE": Settings.SNOWFLAKE_STAGE,
        "SNOWFLAKE_FILE_FORMAT": Settings.SNOWFLAKE_FILE_FORMAT,
    }
    missing = [name for name, value in required_settings.items() if not value]
    if missing:
        raise ValueError(
            "Variaveis obrigatorias ausentes no .env: " + ", ".join(sorted(missing))
        )
    validate_snowflake_auth_settings()


def parse_date(date_text: str) -> date:
    try:
        return datetime.strptime(date_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Data invalida '{date_text}'. Use o formato YYYY-MM-DD."
        ) from exc


def extract_data_from_postgres(
    id_cliente: int,
    updated_on_start: datetime,
    updated_on_end: datetime,
) -> pd.DataFrame:
    logger.info(
        "Iniciando extracao no PostgreSQL para id_cliente=%s entre %s e %s.",
        id_cliente,
        updated_on_start,
        updated_on_end,
    )
    dataframe = read_sql_from_postgres(
        POSTGRES_EXTRACTION_QUERY,
        params={
            "p_cd_cliente": id_cliente,
            "dt_updated_inicial": updated_on_start,
            "dt_updated_final": updated_on_end,
        },
    )
    dataframe.columns = dataframe.columns.str.upper()
    logger.info("Extracao concluida com %s linhas.", len(dataframe))
    return dataframe


def normalize_fact_status(value: object) -> str:
    normalized = str(value).strip().upper() if value is not None else ""
    if normalized in {"A", "ATIVO", "ATIVA", "1", "TRUE", "T"}:
        return FACT_STATUS_ACTIVE
    if normalized in {"I", "INATIVO", "INATIVA", "0", "FALSE", "F"}:
        return FACT_STATUS_INACTIVE
    return FACT_STATUS_ACTIVE


def build_status_summary(dataframe: pd.DataFrame) -> dict[str, int]:
    if dataframe.empty or "FG_STATUS" not in dataframe.columns:
        return {"ativos": 0, "inativos": 0}
    ativos = int((dataframe["FG_STATUS"] == FACT_STATUS_ACTIVE).sum())
    inativos = int((dataframe["FG_STATUS"] == FACT_STATUS_INACTIVE).sum())
    return {"ativos": ativos, "inativos": inativos}


def prepare_dataframe_for_load(
    dataframe: pd.DataFrame,
    etl_batch_id: str,
    bi_timestamp: str,
) -> pd.DataFrame:
    prepared = dataframe.copy()
    prepared["FG_STATUS"] = prepared["FG_STATUS"].apply(normalize_fact_status)
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


def load_data_to_snowflake(output_file_path: Path) -> None:
    logger.info("Enviando arquivo para o stage %s.", Settings.SNOWFLAKE_STAGE)
    loader = SnowflakeLoader()
    try:
        loader.upload_file_to_stage(
            local_file_path=str(output_file_path),
            stage_name=Settings.SNOWFLAKE_STAGE,
        )
        loader.merge_file_into_table(
            full_table_name=TARGET_TABLE,
            stage_name=Settings.SNOWFLAKE_STAGE,
            file_name=output_file_path.name,
            file_format_name=Settings.SNOWFLAKE_FILE_FORMAT,
            columns=TARGET_COLUMNS,
            key_columns=NATURAL_KEY_COLUMNS,
            preserve_on_update_columns=["BI_CREATED_AT"],
            update_current_timestamp_columns=["BI_UPDATED_AT"],
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
) -> dict[str, object]:
    manual_range_informed = bool(data_inicio_text or data_fim_text)
    if manual_range_informed and not (data_inicio_text and data_fim_text):
        raise ValueError("Informe data_inicio e data_fim juntos para executar backfill manual.")

    if data_inicio_text and data_fim_text:
        data_inicio = parse_date(data_inicio_text)
        data_fim = parse_date(data_fim_text)
        if data_inicio > data_fim:
            raise ValueError("data_inicio nao pode ser maior que data_fim.")
        extraction_started_at = datetime.combine(data_inicio, datetime.min.time())
        extraction_ended_at = datetime.combine(data_fim + timedelta(days=1), datetime.min.time())
        return {
            "load_mode": LOAD_MODE_MANUAL,
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
        "extraction_started_at": extraction_started_at,
        "extraction_ended_at": extraction_ended_at,
        "last_watermark": last_watermark,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"Executa a pipeline {PIPELINE_NAME}.")
    parser.add_argument("--id_cliente", type=int, required=True)
    parser.add_argument("--data_inicio", type=str, required=False)
    parser.add_argument("--data_fim", type=str, required=False)
    return parser


def run_pipeline(
    id_cliente: int,
    data_inicio_text: str | None = None,
    data_fim_text: str | None = None,
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
        bi_timestamp = datetime.now(UTC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        audit_loader, audit_repository, watermark_repository = get_audit_repository()
        load_window = resolve_load_window(
            id_cliente=id_cliente,
            watermark_repository=watermark_repository,
            data_inicio_text=data_inicio_text,
            data_fim_text=data_fim_text,
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

        audit_repository.insert_batch_start(
            batch_id=batch_id,
            pipeline_name=PIPELINE_NAME,
            source_name=SOURCE_NAME,
            target_name=TARGET_TABLE,
            id_cliente=id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        dataframe = extract_data_from_postgres(
            id_cliente=id_cliente,
            updated_on_start=extraction_started_at,
            updated_on_end=extraction_ended_at,
        )
        if not dataframe.empty:
            dataframe["FG_STATUS"] = dataframe["FG_STATUS"].apply(normalize_fact_status)
        status_summary = build_status_summary(dataframe)
        max_source_updated_on = normalize_source_updated_on(dataframe)
        extract_details = (
            f"Extracao concluida para id_cliente={id_cliente}, source={SOURCE_NAME}, "
            f"load_mode={resolved_load_mode}, janela={extraction_started_at} a {extraction_ended_at}, "
            f"max_source_updated_on={max_source_updated_on}, "
            f"ativos={status_summary['ativos']}, inativos={status_summary['inativos']}. "
            "Regra aplicada: A representa registro vigente; I representa o mesmo grão desativado na fonte."
        )
        if dataframe.empty:
            extract_details = (
                f"Extracao concluida sem dados para id_cliente={id_cliente}, source={SOURCE_NAME}, "
                f"load_mode={resolved_load_mode}, janela={extraction_started_at} a {extraction_ended_at}. "
                "Nenhum registro foi encontrado na origem para esta entidade e nenhuma linha sera aplicada no DS."
            )

        audit_repository.insert_audit_event(
            batch_id=batch_id,
            step_name="POSTGRES_EXTRACT",
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
        if not dataframe.empty:
            dataframe_to_load = prepare_dataframe_for_load(
                dataframe=dataframe,
                etl_batch_id=batch_id,
                bi_timestamp=bi_timestamp,
            )
            output_file_path = build_output_file_path(
                id_cliente=id_cliente,
                data_inicio=str(extraction_started_at),
                data_fim=str(extraction_ended_at),
            )
            save_dataframe_to_csv(dataframe_to_load, output_file_path)
            load_data_to_snowflake(output_file_path)
            rows_loaded = len(dataframe_to_load)

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
                f"ativos={status_summary['ativos']}, inativos={status_summary['inativos']}. "
                "O merge preserva o mesmo grão composto e atualiza FG_STATUS quando o registro retorna com novo estado."
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

        audit_repository.update_batch_success(
            batch_id=batch_id,
            rows_extracted=len(dataframe),
            rows_loaded=rows_loaded,
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
            except Exception:
                logger.exception("Falha ao registrar erro na auditoria.")
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
    )


if __name__ == "__main__":
    main()
