from __future__ import annotations

from pathlib import Path
from typing import Any

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from src.common.config import Settings


def validate_snowflake_auth_settings() -> None:
    if not Settings.SNOWFLAKE_PRIVATE_KEY_PATH:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH e obrigatorio para autenticacao keypair.")

    key_path = Path(Settings.SNOWFLAKE_PRIVATE_KEY_PATH).expanduser()
    if not key_path.exists():
        raise ValueError(
            f"Arquivo da chave privada do Snowflake nao encontrado: {key_path}"
        )


def load_private_key_bytes() -> bytes:
    if not Settings.SNOWFLAKE_PRIVATE_KEY_PATH:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH nao informado.")

    key_path = Path(Settings.SNOWFLAKE_PRIVATE_KEY_PATH).expanduser()
    passphrase = Settings.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    password = passphrase.encode("utf-8") if passphrase else None

    with key_path.open("rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password,
            backend=default_backend(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def build_snowflake_connection_kwargs(
    *,
    role: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    validate_snowflake_auth_settings()

    kwargs: dict[str, Any] = {
        "account": Settings.SNOWFLAKE_ACCOUNT,
        "user": Settings.SNOWFLAKE_USER,
        "warehouse": Settings.SNOWFLAKE_WAREHOUSE,
        "database": database or Settings.SNOWFLAKE_DATABASE,
        "schema": schema or Settings.SNOWFLAKE_SCHEMA,
        "role": role,
        "private_key": load_private_key_bytes(),
    }

    return {key: value for key, value in kwargs.items() if value is not None}
