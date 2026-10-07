"""The TLC source: month validation, file name, URL, and download cleanup."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Self

import pytest
import requests

import tlc
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


class InterruptedResponse:
    """Fake requests response whose body breaks after the first chunk."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> Any:
        yield b"first chunk"
        raise requests.ConnectionError("stream interrupted")


def test_interrupted_download_propagates_and_leaves_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed download raises and cleans up its .part file.

    The DAG task deletes the destination in a finally block; the partial
    file is download's own convention, so download must remove it itself.
    """
    monkeypatch.setattr(
        tlc.requests, "get", lambda *args, **kwargs: InterruptedResponse()
    )
    destination = tmp_path / "yellow_tripdata_2025-01.parquet"

    with pytest.raises(requests.ConnectionError):
        tlc.download(
            "https://example.invalid/yellow_tripdata_2025-01.parquet", destination
        )

    assert list(tmp_path.iterdir()) == []
