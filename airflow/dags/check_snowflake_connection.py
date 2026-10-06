"""Prove that Airflow reaches Snowflake as the service user, with its role and warehouse.

One task, no schedule: this DAG only runs when triggered by hand. It is the
smallest possible check of the connection defined in airflow/.env, written
before the loading DAG so that a key formatting mistake surfaces here, with a
clear message, rather than in the middle of a load.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

CONN_ID = "snowflake_nyc_taxi"
EXPECTED = ("AIRFLOW_SVC", "TRANSFORMER", "NYC_TAXI_WH")

# Airflow's task log is built on the standard logging module: anything logged
# here lands in the task's log in the UI
log = logging.getLogger(__name__)


@dag(
    dag_id="check_snowflake_connection",
    schedule=None,  # on demand only: the Trigger button is the way to run it
    start_date=pendulum.datetime(
        2025, 1, 1, tz="UTC"
    ),  # required even without a schedule
    catchup=False,
    default_args={"retries": 0},  # a wrong key is not fixed by retrying
    tags=["nyc_taxi", "check"],
)
def check_snowflake_connection() -> None:
    """Declare the single task of the DAG; the function body is the DAG definition."""

    @task
    def who_am_i() -> tuple[str, str, str]:
        """Ask Snowflake who we are and fail if it is not the expected identity.

        Returns:
            The (user, role, warehouse) triplet, stored as an XCom and visible
            in the UI.

        Raises:
            ValueError: If the session is not AIRFLOW_SVC / TRANSFORMER / NYC_TAXI_WH.
        """
        # The hook resolves the connection from AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI
        # at this very moment, inside the task process
        identity = SnowflakeHook(snowflake_conn_id=CONN_ID).get_first(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
        )
        log.info("connected: user=%s role=%s warehouse=%s", *identity)
        if tuple(identity) != EXPECTED:
            raise ValueError(
                f"unexpected session identity {identity}, expected {EXPECTED}"
            )
        return tuple(identity)

    who_am_i()


# Calling the decorated function is what registers the DAG; without this line
# the file declares nothing
check_snowflake_connection()
