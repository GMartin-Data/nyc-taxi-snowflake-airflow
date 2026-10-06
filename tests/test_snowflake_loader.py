"""Logic of copy_into around COPY INTO, played against a scripted cursor.

No Snowflake session: the cursor replays the results we dictate and records
the statements it received, so each test checks what the loader decides.
"""

from __future__ import annotations

from typing import Any, Self

from snowflake_loader import copy_into

TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE = "yellow_tripdata_2025-01.parquet"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"

# Results of the guard query: does the table already carry this _source_file?
NOT_LOADED = {"columns": ["COUNT(*) > 0"], "row": (False,)}
ALREADY_LOADED = {"columns": ["COUNT(*) > 0"], "row": (True,)}

# Results of COPY INTO: one row per processed file, or a lone status column
# when Snowflake's own load history made it skip the file.
COPY_LOADED = {
    "columns": [
        "file",
        "status",
        "rows_parsed",
        "rows_loaded",
        "error_limit",
        "errors_seen",
    ],
    "row": (FILE, "LOADED", 3475226, 3475226, 1, 0),
}
COPY_NO_FILE = {
    "columns": ["status"],
    "row": ("Copy executed with 0 files processed.",),
}


class ScriptedCursor:
    """Stand-in for a Snowflake cursor: replays scripted results, records the SQL."""

    def __init__(self, results: list[dict[str, Any]]) -> None:
        self._results = list(results)
        self._current: dict[str, Any] = {}
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> ScriptedCursor:
        self.executed.append((sql, params))
        self._current = self._results.pop(0)
        return self

    def fetchone(self) -> tuple[Any, ...]:
        return self._current["row"]

    @property
    def description(self) -> list[tuple[str]]:
        return [(name,) for name in self._current["columns"]]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class ScriptedConnection:
    """Stand-in for a Snowflake connection: hands out one scripted cursor."""

    def __init__(self, cursor: ScriptedCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> ScriptedCursor:
        return self._cursor


def test_the_guard_looks_for_the_file_name_in_source_file() -> None:
    cursor = ScriptedCursor([ALREADY_LOADED])
    copy_into(ScriptedConnection(cursor), TABLE, FILE, FILE_FORMAT)
    sql, params = cursor.executed[0]
    assert TABLE in sql
    assert "_source_file" in sql
    assert params == (FILE,)


def test_a_file_already_in_the_table_is_not_copied() -> None:
    cursor = ScriptedCursor([ALREADY_LOADED])
    rows_loaded = copy_into(ScriptedConnection(cursor), TABLE, FILE, FILE_FORMAT)
    assert rows_loaded == 0
    assert len(cursor.executed) == 1
    assert "COPY INTO" not in cursor.executed[0][0]


def test_a_file_absent_from_the_table_is_copied() -> None:
    cursor = ScriptedCursor([NOT_LOADED, COPY_LOADED])
    rows_loaded = copy_into(ScriptedConnection(cursor), TABLE, FILE, FILE_FORMAT)
    assert rows_loaded == 3475226
    assert len(cursor.executed) == 2
    assert cursor.executed[1][0].lstrip().startswith("COPY INTO")


def test_a_file_skipped_by_snowflake_yields_zero_rows() -> None:
    cursor = ScriptedCursor([NOT_LOADED, COPY_NO_FILE])
    rows_loaded = copy_into(ScriptedConnection(cursor), TABLE, FILE, FILE_FORMAT)
    assert rows_loaded == 0
