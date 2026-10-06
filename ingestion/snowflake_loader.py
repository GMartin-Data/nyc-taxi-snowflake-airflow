"""Load a local file into a RAW table: key-pair session, PUT, then COPY INTO.

Reads SNOWFLAKE_ACCOUNT and, optionally, SNOWFLAKE_PRIVATE_KEY_PATH from the
environment. Every other name comes from the SQL scripts in snowflake/.
"""

from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
import structlog
from cryptography.hazmat.primitives import serialization
from snowflake.connector import SnowflakeConnection

USER = "AIRFLOW_SVC"
ROLE = "TRANSFORMER"
WAREHOUSE = "NYC_TAXI_WH"
STAGE = "NYC_TAXI.RAW.TLC_STAGE"

DEFAULT_KEY_PATH = "~/.ssh/snowflake/rsa_key.p8"

log = structlog.get_logger()


def connect() -> SnowflakeConnection:
    """Open a session as the service user, authenticated by its private key.

    Raises:
        KeyError: If SNOWFLAKE_ACCOUNT is not set.
    """
    key_path = Path(
        os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)
    ).expanduser()
    private_key = serialization.load_pem_private_key(
        key_path.read_bytes(), password=None
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=USER,
        private_key=private_key,
        role=ROLE,
        warehouse=WAREHOUSE,
    )


def put(conn: SnowflakeConnection, file: Path) -> str:
    """Upload a file to the root of the stage, as is.

    Returns:
        The PUT status: UPLOADED, or SKIPPED when the stage already holds the file.
    """
    with conn.cursor() as cur:
        row = cur.execute(
            f"PUT file://{file.resolve()} @{STAGE} AUTO_COMPRESS = FALSE OVERWRITE = FALSE"
        ).fetchone()
    status = row[6]
    log.info("put_done", file=file.name, status=status)
    return status


def is_loaded(conn: SnowflakeConnection, table: str, file_name: str) -> bool:
    """Tell whether a table already carries rows of a file, by _source_file.

    The file name travels as a bound variable, outside the SQL text.
    """
    with conn.cursor() as cur:
        row = cur.execute(
            f"SELECT COUNT(*) > 0 FROM {table} WHERE _source_file = %s", (file_name,)
        ).fetchone()
    return bool(row[0])


def copy_into(
    conn: SnowflakeConnection, table: str, file_name: str, file_format: str
) -> int:
    """Copy one staged file into a table, matching columns by name.

    Guarded: a file whose name the table already carries in _source_file is
    not copied, whatever Snowflake's own load history (kept 64 days) says.
    See adr/0001.

    Returns:
        The number of rows loaded: 0 when the guard or Snowflake skipped the file.
    """
    if is_loaded(conn, table, file_name):
        log.info("copy_skipped", table=table, file=file_name, reason="already loaded")
        return 0
    with conn.cursor() as cur:
        cur.execute(
            f"""
            COPY INTO {table}
            FROM @{STAGE}
            FILES = ('{file_name}')
            FILE_FORMAT = (FORMAT_NAME = {file_format})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                _source_file = METADATA$FILENAME,
                _loaded_at = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
            """
        )
        columns = [description[0] for description in cur.description]
        result = dict(zip(columns, cur.fetchone(), strict=True))

    # A skipped file yields a single column: "Copy executed with 0 files processed."
    if "rows_loaded" not in result:
        log.info("copy_skipped", table=table, file=file_name, message=result["status"])
        return 0
    log.info(
        "copy_done",
        table=table,
        file=result["file"],
        status=result["status"],
        rows_parsed=result["rows_parsed"],
        rows_loaded=result["rows_loaded"],
    )
    return int(result["rows_loaded"])
