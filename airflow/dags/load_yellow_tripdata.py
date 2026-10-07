"""Load one month of yellow taxi trips per run into NYC_TAXI.RAW.YELLOW_TRIPDATA.

Monthly schedule over January to March 2025, with catchup: switching the DAG
on creates one run per month, dated the first day of that month, and each run
loads the TLC file of its month. Never trigger this DAG by hand: a manual run
is dated today and would look for a file the TLC has not published yet.

The PUT and COPY INTO are those of the workstation script, imported from
include/, so the replay guarantee of adr/0001 holds here too: a month already
in the table is skipped, the run succeeds with zero rows loaded.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pendulum
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task

from include import snowflake_loader, tlc

CONN_ID = "snowflake_nyc_taxi"
TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"
# Disk of the container running the task; the file lives there only between
# the download and the PUT
DOWNLOAD_DIR = Path("/tmp")

log = logging.getLogger(__name__)


@dag(
    dag_id="load_yellow_tripdata",
    schedule="@monthly",  # Airflow 3: the run of a month is created on its first day
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),  # last month loaded, inclusive
    catchup=True,  # replay the three months when the DAG is switched on
    max_active_runs=1,  # one month at a time, in order
    # default_args apply to every task: a file not yet published or a network
    # error is often fixed by retrying
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["nyc_taxi", "load"],
)
def load_yellow_tripdata() -> None:
    """Declare the four tasks and chain them; the function body is the DAG."""

    @task
    def file_name() -> str:
        """Name of the TLC file for the month this run processes."""
        # data_interval_start is the logical date of the run, fixed when the
        # run was created: the first day of its month, never "today"
        month = get_current_context()["data_interval_start"].strftime("%Y-%m")
        # tlc.file_name validates the YYYY-MM format before the name reaches SQL
        return tlc.file_name(month)

    @task
    def check_availability(name: str) -> str:
        """Fail, and so retry later, if the TLC has not published the file yet."""
        url = f"{tlc.BASE_URL}/{name}"
        # HEAD fetches the headers only, no download; the TLC distribution
        # answers 403 (not 404) for a missing file, raise_for_status covers both
        requests.head(url, timeout=30).raise_for_status()
        return url

    @task
    def download_and_put(url: str) -> str:
        """Download the file and upload it to the stage, then delete it.

        Both steps in one task: on an Airflow spread over several machines,
        two tasks may run on two machines that do not share a disk.
        """
        destination = DOWNLOAD_DIR / url.rsplit("/", 1)[-1]
        try:
            tlc.download(url, destination)
            # The hook resolves the connection from AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI
            # and hands out the same connection object as the workstation script
            with SnowflakeHook(snowflake_conn_id=CONN_ID).get_conn() as conn:
                snowflake_loader.put(conn, destination)
        finally:
            destination.unlink(missing_ok=True)  # the container's disk is small
        return destination.name

    @task
    def copy_into(name: str) -> int:
        """Copy the staged file into the RAW table, unless it is already there."""
        with SnowflakeHook(snowflake_conn_id=CONN_ID).get_conn() as conn:
            rows_loaded = snowflake_loader.copy_into(conn, TABLE, name, FILE_FORMAT)
        log.info("month loaded: file=%s rows_loaded=%s", name, rows_loaded)
        return rows_loaded

    # Each call passes the previous task's return value (a small string, via
    # XCom) and declares the dependency
    copy_into(download_and_put(check_availability(file_name())))


# Calling the decorated function is what registers the DAG
load_yellow_tripdata()
