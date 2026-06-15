"""Unit tests for glue_jobs/dynamodb_ingestion_job.py.

All AWS interaction is mocked — no real S3 or DynamoDB calls are made.
"""

import os
import sys
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "glue_jobs")
)

import dynamodb_ingestion_job as job  # noqa: E402


def sample_dataframe(rows=1):
    """Build a KPI dataframe using numpy types (as pandas/pyarrow would)."""
    base = {
        "track_genre": "pop",
        "date": "2024-06-25",
        "listen_count": np.int64(120),
        "unique_listeners": np.int64(45),
        "total_listening_time_ms": np.int64(9000000),
        "avg_listening_time_per_user_ms": np.float64(200000.0),
        "is_top_5": np.bool_(True),
        "top_3_songs": [
            {"rank": np.int64(1), "track_name": "A", "artists": "X",
             "play_count": np.int64(50)},
            {"rank": np.int64(2), "track_name": "B", "artists": "Y",
             "play_count": np.int64(40)},
        ],
    }
    return pd.DataFrame([base for _ in range(rows)])


class FakeBatchWriter:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def put_item(self, Item):  # noqa: N803
        self._sink.append(Item)


class FakeTable:
    def __init__(self):
        self.items = []

    def batch_writer(self):
        return FakeBatchWriter(self.items)


def test_reshapes_row_to_dynamodb_item():
    df = sample_dataframe()
    item = job.reshape_row(df.iloc[0])

    assert item["genre"] == "pop"
    assert item["date"] == "2024-06-25"
    assert isinstance(item["date"], str)
    assert item["listen_count"] == 120 and isinstance(item["listen_count"], int)
    assert isinstance(item["avg_listening_time_per_user_ms"], float)
    assert isinstance(item["is_top_5"], bool)

    # No numpy types should survive anywhere in the item.
    assert not isinstance(item["listen_count"], np.generic)
    song = item["top_3_songs"][0]
    assert isinstance(song["rank"], int)
    assert isinstance(song["play_count"], int)
    assert not isinstance(song["rank"], np.generic)


def test_raises_on_missing_columns():
    df = sample_dataframe().drop(columns=["unique_listeners", "is_top_5"])
    with pytest.raises(ValueError) as exc_info:
        job.dataframe_to_items(df)
    message = str(exc_info.value)
    assert "unique_listeners" in message
    assert "is_top_5" in message


def test_batch_writer_called_with_correct_items():
    df = sample_dataframe(rows=3)
    table = FakeTable()
    written = job.run(
        "processed-bucket",
        "music_streaming_kpis",
        table=table,
        reader=lambda _path: df,
    )

    assert written == 3
    assert len(table.items) == 3
    # Floats must have been converted to Decimal for DynamoDB.
    first = table.items[0]
    assert isinstance(first["avg_listening_time_per_user_ms"], Decimal)
    assert first["genre"] == "pop"


def test_raises_on_empty_parquet():
    empty = pd.DataFrame()
    with pytest.raises(RuntimeError):
        job.run(
            "processed-bucket",
            "music_streaming_kpis",
            table=FakeTable(),
            reader=lambda _path: empty,
        )
