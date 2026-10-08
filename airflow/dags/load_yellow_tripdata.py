"""Load one month of yellow taxi trips into RAW, then transform it to the marts.

Monthly schedule over January to March 2025, with catchup: switching the DAG
on creates one run per month, dated the first day of that month, and each run
loads the TLC file of its month. Never trigger this DAG by hand: a manual run
is dated today and would look for a file the TLC has not published yet.

The PUT and COPY INTO are those of the workstation script, imported from
include/, so the replay guarantee of adr/0001 holds here too: a month already
in the table is skipped, the run succeeds with zero rows loaded.

The transformations are the kit's SQL files under include/sql/, one task per
file, grouped by layer (staging, intermediate, marts). Three checks gate the
chain: the month is in RAW before anything runs, and no duplicate or excessive
rejection reaches the fact table.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
import requests
import structlog
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Param, TaskGroup, dag, get_current_context, task

from include import snowflake_loader, tlc

CONN_ID = "snowflake_nyc_taxi"
TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"
# Disk of the container running the task; the file lives there only between
# the download and the PUT
DOWNLOAD_DIR = Path("/tmp")
# Where the image copies include/sql: Jinja resolves "staging/x.sql" from here
SQL_DIR = "/usr/local/airflow/include/sql"

START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")
END_DATE = pendulum.datetime(2025, 3, 1, tz="UTC")  # last month loaded, inclusive

# Values the SQL files read through {{ params.x }}; the thresholds come from
# the kit's README. The calendar bounds follow the DAG's own dates so that
# DIM_DATE and the schedule cannot drift apart (end_month is exclusive).
# Jinja pastes each value into the SQL as is, and the Trigger and Backfill
# dialogs let whoever runs the DAG override it: the schema of each Param is
# checked when the run is requested, so a value that is not a number or a
# date never reaches a statement
PARAMS = {
    "max_trip_distance_miles": Param(100, type="integer", minimum=1),
    "max_trip_duration_min": Param(180, type="integer", minimum=1),
    "start_month": Param(START_DATE.to_date_string(), type="string", format="date"),
    "end_month": Param(
        END_DATE.add(months=1).to_date_string(), type="string", format="date"
    ),
    # Share of a month's trips int_trips__flagged may reject before the run
    # stops; the first quarter of 2025 loses about 7 %, so 10 % leaves room
    "max_pct_rejected": Param(10, type="integer", minimum=0, maximum=100),
}

# Same logger as include/: Airflow 3 writes the task log with structlog
log = structlog.get_logger()


def run_sql(
    task_id: str, sql_file: str, *, split_statements: bool | None = None
) -> SQLExecuteQueryOperator:
    """One task per SQL file, rendered with Jinja then run on the connection.

    split_statements=True is set on the files holding several statements
    (three CREATE, or a DELETE then an INSERT): the hook sends them one by
    one, in a single transaction committed at the end.
    """
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=CONN_ID,
        sql=sql_file,
        split_statements=split_statements,
    )


def check(task_id: str, sql_file: str) -> SQLCheckOperator:
    """A check runs a query returning one row and fails on any false value.

    retries=0 overrides the DAG's default_args: retrying changes nothing to
    the data, it would only delay the failure by ten minutes.
    """
    return SQLCheckOperator(task_id=task_id, conn_id=CONN_ID, sql=sql_file, retries=0)


@dag(
    dag_id="load_yellow_tripdata",
    schedule="@monthly",  # Airflow 3: the run of a month is created on its first day
    start_date=START_DATE,
    end_date=END_DATE,
    catchup=True,  # replay the three months when the DAG is switched on
    max_active_runs=1,  # one month at a time, in order
    # default_args apply to every task: a file not yet published or a network
    # error is often fixed by retrying
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    template_searchpath=SQL_DIR,
    params=PARAMS,
    tags=["nyc_taxi", "load", "transform"],
)
def load_yellow_tripdata() -> None:
    """Declare the tasks and chain them; the function body is the DAG."""

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
        """Fail if the TLC has not published the file.

        The DAG's retries cover a transient error (network, short outage),
        not the publication lag: the TLC publishes a month about two months
        later, far beyond two retries five minutes apart. The bounded window
        of this DAG only covers months already published.
        """
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
        log.info("month_loaded", file=name, rows_loaded=rows_loaded)
        return rows_loaded

    # Each call passes the previous task's return value (a small string, via
    # XCom) and declares the dependency
    loaded = copy_into(download_and_put(check_availability(file_name())))

    # The month must be in RAW before any transformation reads it: same
    # _source_file key as the loader's guard (adr/0001)
    check_raw_month_loaded = check(
        "check_raw_month_loaded", "controles/raw_mois_charge.sql"
    )
    # CREATE TABLE IF NOT EXISTS for the tables fed month by month: they must
    # exist before their first DELETE
    create_tables = run_sql("create_tables", "00_tables.sql", split_statements=True)

    # A TaskGroup chains like a task: every task of a layer waits for every
    # task of the previous one. Its tasks are named "<group>.<task>"
    with TaskGroup("staging") as staging:
        run_sql("codes_tlc", "staging/codes_tlc.sql", split_statements=True)
        run_sql("stg_tlc__taxi_zones", "staging/stg_tlc__taxi_zones.sql")
        run_sql("stg_tlc__yellow_trips", "staging/stg_tlc__yellow_trips.sql")

    with TaskGroup("intermediate") as intermediate:
        flagged = run_sql(
            "int_trips__flagged",
            "intermediate/int_trips__flagged.sql",
            split_statements=True,
        )
        enriched = run_sql(
            "int_trips__enriched",
            "intermediate/int_trips__enriched.sql",
            split_statements=True,
        )
        flagged >> enriched

    # Both checks read the intermediate tables of the month and stand before
    # marts: a duplicate or a broken file never reaches FCT_TRIPS
    check_no_duplicate_trip = check(
        "check_no_duplicate_trip", "controles/no_duplicate_trip.sql"
    )
    check_rejected_share = check("check_rejected_share", "controles/rejected_share.sql")

    with TaskGroup("marts") as marts:
        # The dims and the fact table read neither each other: they run in
        # parallel; the nested group keeps the Graph view to six edges
        with TaskGroup("dims") as dims:
            for name in (
                "dim_date",
                "dim_payment_type",
                "dim_rate_code",
                "dim_vendor",
                "dim_zone",
            ):
                run_sql(name, f"marts/{name}.sql")
        fct_trips = run_sql("fct_trips", "marts/fct_trips.sql", split_statements=True)
        for name in (
            "mart_daily_revenue",
            "mart_zone_hourly_demand",
            "mart_data_quality",
        ):
            # [group, task] >> task: the task waits for every leaf of the group
            [dims, fct_trips] >> run_sql(name, f"marts/{name}.sql")

    (
        loaded
        >> check_raw_month_loaded
        >> create_tables
        >> staging
        >> intermediate
        >> [check_no_duplicate_trip, check_rejected_share]
        >> marts
    )


# Calling the decorated function is what registers the DAG
load_yellow_tripdata()
