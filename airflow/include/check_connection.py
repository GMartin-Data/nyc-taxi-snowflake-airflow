"""Check that the service user can sign in to Snowflake with its key pair.

Usage:
    uv run --env-file .env python airflow/include/check_connection.py

Opens the session exactly as the loading scripts and the DAG do, then exits
with an error unless Snowflake reports the expected user, role and warehouse.
"""

from __future__ import annotations

import sys

import structlog

from snowflake_loader import ROLE, USER, WAREHOUSE, connect

# Names created by snowflake/01_infrastructure.sql
EXPECTED = (USER, ROLE, WAREHOUSE)

log = structlog.get_logger()


def main() -> None:
    """Open a session as the service user and compare it with EXPECTED.

    Raises:
        KeyError: If SNOWFLAKE_ACCOUNT is not set.
        SystemExit: If the session reports another user, role or warehouse.
    """
    with connect() as conn:
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
