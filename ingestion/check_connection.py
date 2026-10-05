"""Check that the service user can sign in to Snowflake with its key pair.

Usage:
    SNOWFLAKE_ACCOUNT=ORGANISATION-ACCOUNT python ingestion/check_connection.py

Exits with an error unless the session reports the expected user, role and
warehouse.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import snowflake.connector
import structlog
from cryptography.hazmat.primitives import serialization

# Names created by snowflake/01_infrastructure.sql
EXPECTED = ("AIRFLOW_SVC", "TRANSFORMER", "NYC_TAXI_WH")

DEFAULT_KEY_PATH = "~/.ssh/snowflake/rsa_key.p8"

log = structlog.get_logger()


def main() -> None:
    """Open a session as the service user and compare it with EXPECTED.

    Raises:
        KeyError: If SNOWFLAKE_ACCOUNT is not set.
        SystemExit: If the session reports another user, role or warehouse.
    """
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    key_path = Path(
        os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)
    ).expanduser()
    private_key = serialization.load_pem_private_key(
        key_path.read_bytes(), password=None
    )
    user, role, warehouse = EXPECTED

    with snowflake.connector.connect(
        account=account,
        user=user,
        private_key=private_key,
        role=role,
        warehouse=warehouse,
    ) as conn:
        actual = (
            conn.cursor()
            .execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            .fetchone()
        )

    if actual != EXPECTED:
        log.error("connection_check_failed", expected=EXPECTED, actual=actual)
        sys.exit(1)
    log.info(
        "connection_check_passed", user=actual[0], role=actual[1], warehouse=actual[2]
    )


if __name__ == "__main__":
    main()
