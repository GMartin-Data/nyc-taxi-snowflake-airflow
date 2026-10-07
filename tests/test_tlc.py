"""Pure logic of the TLC source: month validation, file name and URL."""

from __future__ import annotations

import pytest

from tlc import file_name, source_url


def test_file_name_follows_the_tlc_pattern() -> None:
    assert file_name("2025-01") == "yellow_tripdata_2025-01.parquet"


def test_source_url_points_to_the_tlc_distribution() -> None:
    assert source_url("2025-03") == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2025-03.parquet"
    )


@pytest.mark.parametrize(
    "month",
    ["2025-13", "2025-00", "202501", "2025-1", "01-2025", "2025-01-01", ""],
)
def test_malformed_month_is_rejected(month: str) -> None:
    with pytest.raises(ValueError):
        file_name(month)
    with pytest.raises(ValueError):
        source_url(month)
