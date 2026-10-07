"""Load one month of yellow taxi trips into NYC_TAXI.RAW.YELLOW_TRIPDATA.

Usage:
    uv run --env-file .env python airflow/include/load_month.py 2025-01

Downloads the TLC file unless data/ already holds it, uploads it to the stage
and copies it into the table. Replayable: a month already loaded adds no row.
"""

from __future__ import annotations

import sys

import structlog

from snowflake_loader import connect, copy_into, put
from tlc import DATA_DIR, download, file_name, source_url

TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"

log = structlog.get_logger()


def main(month: str) -> None:
    """Download, stage and copy the file of one month."""
    file = download(source_url(month), DATA_DIR / file_name(month))
    with connect() as conn:
        put(conn, file)
        rows_loaded = copy_into(conn, TABLE, file.name, FILE_FORMAT)
    log.info("month_loaded", month=month, rows_loaded=rows_loaded)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: load_month.py YYYY-MM")
    try:
        main(sys.argv[1])
    except ValueError as error:
        sys.exit(str(error))
