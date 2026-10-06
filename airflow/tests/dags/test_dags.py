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

import pytest
from airflow.models import DagBag


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
