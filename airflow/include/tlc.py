"""The TLC source: file names, public URLs and download of the monthly files.

Pure logic plus one network call, no Snowflake. Imported as a sibling by the
workstation scripts and as include.tlc by the DAGs.
"""

from __future__ import annotations

import re
from pathlib import Path

import requests
import structlog

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
# Repository root / data: where the workstation scripts cache downloaded files
DATA_DIR = Path(__file__).resolve().parents[2] / "data"

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
    interrupted download never leaves a truncated file in place, nor the
    temporary file behind: the error propagates after the cleanup.
    """
    if destination.exists():
        log.info("download_skipped", file=destination.name, reason="already on disk")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    try:
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with partial.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
    except BaseException:
        # BaseException, not Exception: a task killed by Airflow (SIGTERM) or
        # a Ctrl-C on the workstation must not leave the partial file either
        partial.unlink(missing_ok=True)
        raise
    partial.replace(destination)
    log.info("download_done", file=destination.name, bytes=destination.stat().st_size)
    return destination
