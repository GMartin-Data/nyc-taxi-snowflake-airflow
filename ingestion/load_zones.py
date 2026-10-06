"""Load the TLC taxi zone lookup into NYC_TAXI.RAW.TAXI_ZONE_LOOKUP.

Usage:
    uv run --env-file .env python ingestion/load_zones.py

Downloads the CSV unless data/ already holds it, uploads it to the stage and
copies it into the table. Replayable: a second run adds no row.
"""

from __future__ import annotations

import structlog

from load_month import DATA_DIR, download
from snowflake_loader import connect, copy_into, put

TABLE = "NYC_TAXI.RAW.TAXI_ZONE_LOOKUP"
FILE_FORMAT = "NYC_TAXI.RAW.CSV_FF"
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

log = structlog.get_logger()


def main() -> None:
    """Download, stage and copy the zone lookup file."""
    file = download(URL, DATA_DIR / "taxi_zone_lookup.csv")
    with connect() as conn:
        put(conn, file)
        rows_loaded = copy_into(conn, TABLE, file.name, FILE_FORMAT)
    log.info("zones_loaded", rows_loaded=rows_loaded)


if __name__ == "__main__":
    main()
