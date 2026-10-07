"""Structure of the project's DAGs, checked without Snowflake.

A DAG file only declares tasks and their order; nothing runs here. These tests
parse the files of dags/ the way the dag-processor does, then inspect the DAG
objects: do they import, are they tagged, do they have the expected shape.
They need Airflow installed, so they run inside the Astro image:

    astro dev pytest
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from itertools import pairwise

import pendulum
import pytest
from airflow.models import DagBag
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)

# Where the SQL files live inside the container, the only place Jinja looks
# for a templated "x.sql" besides the dags/ folder
SQL_DIR = "/usr/local/airflow/include/sql"
CONN_ID = "snowflake_nyc_taxi"

# One SQL task per kit file. A task inside a TaskGroup carries the group id as
# a prefix ("staging.codes_tlc"); that prefixed id is the one get_task() knows
SQL_TASKS = {
    "create_tables": "00_tables.sql",
    "staging.codes_tlc": "staging/codes_tlc.sql",
    "staging.stg_tlc__taxi_zones": "staging/stg_tlc__taxi_zones.sql",
    "staging.stg_tlc__yellow_trips": "staging/stg_tlc__yellow_trips.sql",
    "intermediate.int_trips__flagged": "intermediate/int_trips__flagged.sql",
    "intermediate.int_trips__enriched": "intermediate/int_trips__enriched.sql",
    "marts.dims.dim_date": "marts/dim_date.sql",
    "marts.dims.dim_payment_type": "marts/dim_payment_type.sql",
    "marts.dims.dim_rate_code": "marts/dim_rate_code.sql",
    "marts.dims.dim_vendor": "marts/dim_vendor.sql",
    "marts.dims.dim_zone": "marts/dim_zone.sql",
    "marts.fct_trips": "marts/fct_trips.sql",
    "marts.mart_daily_revenue": "marts/mart_daily_revenue.sql",
    "marts.mart_zone_hourly_demand": "marts/mart_zone_hourly_demand.sql",
    "marts.mart_data_quality": "marts/mart_data_quality.sql",
}
CHECK_TASKS = {
    "check_raw_month_loaded": "controles/raw_mois_charge.sql",
    "check_no_duplicate_trip": "controles/no_duplicate_trip.sql",
    "check_rejected_share": "controles/rejected_share.sql",
}
# Files holding several statements: three CREATE, or a DELETE then an INSERT
MULTI_STATEMENT_TASKS = {
    "create_tables",
    "staging.codes_tlc",
    "intermediate.int_trips__flagged",
    "intermediate.int_trips__enriched",
    "marts.fct_trips",
}

STAGING = {
    "staging.codes_tlc",
    "staging.stg_tlc__taxi_zones",
    "staging.stg_tlc__yellow_trips",
}
DIMS = {
    "marts.dims.dim_date",
    "marts.dims.dim_payment_type",
    "marts.dims.dim_rate_code",
    "marts.dims.dim_vendor",
    "marts.dims.dim_zone",
}
MARTS = {
    "marts.mart_daily_revenue",
    "marts.mart_zone_hourly_demand",
    "marts.mart_data_quality",
}
CHECKS_BEFORE_MARTS = {"check_no_duplicate_trip", "check_rejected_share"}

# The whole graph, as "task -> the tasks it waits for". Layers are chained as
# groups (every task of a layer waits for every task of the previous one);
# inside marts the dependencies are the strict ones: the dims and the fact
# table do not read each other, the three marts read both
EXPECTED_UPSTREAM = {
    "file_name": set(),
    "check_availability": {"file_name"},
    "download_and_put": {"check_availability"},
    "copy_into": {"download_and_put"},
    "check_raw_month_loaded": {"copy_into"},
    "create_tables": {"check_raw_month_loaded"},
    **{task_id: {"create_tables"} for task_id in STAGING},
    "intermediate.int_trips__flagged": STAGING,
    "intermediate.int_trips__enriched": {"intermediate.int_trips__flagged"},
    **{
        task_id: {"intermediate.int_trips__enriched"} for task_id in CHECKS_BEFORE_MARTS
    },
    **{task_id: CHECKS_BEFORE_MARTS for task_id in DIMS | {"marts.fct_trips"}},
    **{task_id: DIMS | {"marts.fct_trips"} for task_id in MARTS},
}
# One task per group, to prove the groups exist (a dotted task_id alone would
# not); the root group of a DAG has no id
EXPECTED_GROUP = {
    "check_raw_month_loaded": None,
    "staging.codes_tlc": "staging",
    "intermediate.int_trips__flagged": "intermediate",
    "marts.dims.dim_date": "marts.dims",
    "marts.fct_trips": "marts",
}


@contextmanager
def suppress_logging(namespace: str) -> Generator[None, None, None]:
    """Silence a logger while parsing, so the test output stays readable.

    Parsing a DAG folder makes Airflow log every file it reads; the only
    output we want is pytest's own.
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    """Parse dags/ once for the whole module and expose the result.

    A DagBag is the collection Airflow builds from a folder: the DAG objects
    found, keyed by dag_id, plus one entry per file that failed to import.
    Scope "module": parsing costs seconds, so it is done once, not per test.
    """
    with suppress_logging("airflow"):
        # include_examples=False: ignore Airflow's bundled example DAGs
        return DagBag(include_examples=False)


def test_dags_import_without_errors(dag_bag: DagBag) -> None:
    """Every file in dags/ must import; a broken DAG never reaches the scheduler."""
    assert dag_bag.import_errors == {}


def test_every_dag_is_tagged_nyc_taxi(dag_bag: DagBag) -> None:
    """Each DAG carries the project tag, used to filter the UI."""
    untagged = [
        dag_id for dag_id, dag in dag_bag.dags.items() if "nyc_taxi" not in dag.tags
    ]
    assert untagged == []


def test_check_snowflake_connection_is_a_manual_single_task_dag(
    dag_bag: DagBag,
) -> None:
    """The proof DAG runs on demand only, with a single task and no retry.

    schedule=None gives the DAG a timetable that cannot schedule anything:
    the only way to run it is the Trigger button. A wrong key would not be
    fixed by retrying, and each retry waits five minutes by default.
    """
    # dag_bag.dags is the in-memory result of parsing; get_dag() would query
    # Airflow's database, which does not exist in the test container
    dag = dag_bag.dags.get("check_snowflake_connection")
    assert dag is not None, "DAG check_snowflake_connection not found"
    # The timetable is the object behind schedule=; None yields one that never fires
    assert dag.timetable.can_be_scheduled is False, (
        "the check DAG must run on demand only"
    )
    assert dag.catchup is False
    assert dag.task_ids == ["who_am_i"]
    # default_args are applied to every task of the DAG; retries lives there
    assert dag.default_args.get("retries") == 0, "a wrong key is not fixed by retrying"


def test_load_yellow_tripdata_replays_the_first_quarter_of_2025_one_month_at_a_time(
    dag_bag: DagBag,
) -> None:
    """The loading DAG runs monthly over January to March 2025 and catches up.

    In Airflow 3, "@monthly" is a trigger timetable: the run of a month is
    created on the first day of that month, and its logical date is that day.
    With catchup=True and an inclusive end_date, switching the DAG on creates
    the runs of 2025-01-01, 2025-02-01 and 2025-03-01, and nothing after.
    max_active_runs=1 runs them one at a time, in order; retries=2 is the
    brief's answer to transient failures (file not yet published, network).
    """
    dag = dag_bag.dags.get("load_yellow_tripdata")
    assert dag is not None, "DAG load_yellow_tripdata not found"
    assert dag.schedule == "@monthly"
    assert dag.start_date == pendulum.datetime(2025, 1, 1, tz="UTC")
    # end_date is the last period loaded, not an exclusive bound (note 07)
    assert dag.end_date == pendulum.datetime(2025, 3, 1, tz="UTC")
    assert dag.catchup is True, "the DAG must replay the past months"
    assert dag.max_active_runs == 1, "one month at a time"
    assert dag.default_args.get("retries") == 2


def test_load_yellow_tripdata_chains_name_check_put_and_copy(dag_bag: DagBag) -> None:
    """The four loading tasks form one chain, at the head of the DAG."""
    dag = dag_bag.dags.get("load_yellow_tripdata")
    assert dag is not None, "DAG load_yellow_tripdata not found"
    chain = ["file_name", "check_availability", "download_and_put", "copy_into"]
    # upstream_task_ids are the tasks a task waits for: none for the first one,
    # exactly the previous one for each of the others
    assert dag.get_task(chain[0]).upstream_task_ids == set()
    for upstream, downstream in pairwise(chain):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_load_yellow_tripdata_declares_the_params_the_sql_files_expect(
    dag_bag: DagBag,
) -> None:
    """The SQL files read five params; four of them come from the kit's README.

    Jinja replaces "{{ params.x }}" by the value declared here. A missing key
    fails the task when the file is rendered, not when the DAG is parsed, so
    the keys are pinned by this test. start_month and end_month bound the
    calendar DIM_DATE: they follow the DAG's own dates (end_month is exclusive,
    one month after the last month loaded) so that the calendar and the
    schedule cannot drift apart.
    """
    dag = dag_bag.dags.get("load_yellow_tripdata")
    assert dag is not None, "DAG load_yellow_tripdata not found"
    assert dag.params["max_trip_distance_miles"] == 100
    assert dag.params["max_trip_duration_min"] == 180
    assert dag.params["start_month"] == dag.start_date.to_date_string()
    assert dag.params["end_month"] == dag.end_date.add(months=1).to_date_string()
    # Share of a month's trips that int_trips__flagged may reject before the
    # run stops; the first quarter of 2025 loses about 7 % (rejected plus
    # duplicates), so 10 % leaves room without hiding a broken file
    assert dag.params["max_pct_rejected"] == 10


def test_load_yellow_tripdata_runs_one_sql_task_per_kit_file(dag_bag: DagBag) -> None:
    """Every kit file is one SQLExecuteQueryOperator reading that file.

    The operator treats a "sql" value ending in ".sql" as a file to load from
    template_searchpath, then renders it with Jinja. Files holding several
    statements say so with split_statements=True: the hook then sends them one
    by one, in a single transaction committed at the end.
    """
    dag = dag_bag.dags.get("load_yellow_tripdata")
    assert dag is not None, "DAG load_yellow_tripdata not found"
    assert SQL_DIR in dag.template_searchpath
    for task_id, sql_file in SQL_TASKS.items():
        task = dag.get_task(task_id)
        assert isinstance(task, SQLExecuteQueryOperator), task_id
        assert task.conn_id == CONN_ID, task_id
        assert task.sql == sql_file, task_id
        # default_args apply: a transient Snowflake error is worth retrying
        assert task.retries == 2, task_id
    for task_id in MULTI_STATEMENT_TASKS:
        assert dag.get_task(task_id).split_statements is True, task_id


def test_load_yellow_tripdata_checks_read_their_file_and_never_retry(
    dag_bag: DagBag,
) -> None:
    """Each check is a SQLCheckOperator on its file, with retries=0.

    A check runs a query returning one row and fails on any false value.
    Retrying changes nothing to the data: with the DAG's two retries five
    minutes apart, a bad month would sit ten minutes in up_for_retry before
    failing for good. retries on the task overrides default_args.
    """
    dag = dag_bag.dags.get("load_yellow_tripdata")
    assert dag is not None, "DAG load_yellow_tripdata not found"
    for task_id, sql_file in CHECK_TASKS.items():
        task = dag.get_task(task_id)
        assert isinstance(task, SQLCheckOperator), task_id
        assert task.conn_id == CONN_ID, task_id
        assert task.sql == sql_file, task_id
        assert task.retries == 0, f"{task_id}: a failed check is not fixed by retrying"


def test_load_yellow_tripdata_orders_the_layers_and_gates_them_with_checks(
    dag_bag: DagBag,
) -> None:
    """The graph: load, check RAW, create tables, staging, intermediate, checks, marts.

    Comparing the whole "task -> upstream" map at once also fixes the set of
    tasks: a missing, extra or misplaced task shows up in the dict diff.
    Group membership is checked on one task per group: a task_id with dots
    could exist outside any group, the TaskGroup is what the Graph view folds.
    """
    dag = dag_bag.dags.get("load_yellow_tripdata")
    assert dag is not None, "DAG load_yellow_tripdata not found"
    upstream = {task.task_id: task.upstream_task_ids for task in dag.tasks}
    assert upstream == EXPECTED_UPSTREAM
    for task_id, group_id in EXPECTED_GROUP.items():
        assert dag.get_task(task_id).task_group.group_id == group_id, task_id
