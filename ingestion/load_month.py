"""Load one month of yellow taxi trips into NYC_TAXI.RAW.YELLOW_TRIPDATA.

Usage:
    uv run --env-file .env python ingestion/load_month.py 2025-01

Downloads the TLC file unless data/ already holds it, uploads it to the stage
and copies it into the table. Replayable: a month already loaded adds no row.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import requests
import structlog

from snowflake_loader import connect, copy_into, put

TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

MONTH_PATTERN = re.compile(r"\d{4}-(0[1-9]|1[0-2])")

log = structlog.get_logger()


def file_name(month: str) -> str:
    """Name of the TLC file for a month written YYYY-MM.

    Raises:
        ValueError: If the month is not written YYYY-MM, month from 01 to 12.
    """
    if not MONTH_PATTERN.fullmatch(month):
        raise ValueError(f"month must be written YYYY-MM, got {month!r}")
    return f"yellow_tripdata_{month}.parquet"


def source_url(month: str) -> str:
    """Public URL of the TLC file for a month written YYYY-MM."""
    return f"{BASE_URL}/{file_name(month)}"


def download(url: str, destination: Path) -> Path:
    """Download a file by chunks, unless it is already on disk.

    The file is written under a temporary name and renamed at the end, so an
    interrupted download never leaves a truncated file in place.
    """
    if destination.exists():
        log.info("download_skipped", file=destination.name, reason="already on disk")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with partial.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    partial.replace(destination)
    log.info("download_done", file=destination.name, bytes=destination.stat().st_size)
    return destination


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
