"""Unit tests for glue_jobs/transformation_job.py.

These tests exercise the pure transformation functions with a real local
SparkSession — no GlueContext and no AWS. They are skipped automatically if
PySpark is not installed in the runner.
"""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "glue_jobs")
)

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

import transformation_job as tj  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transformation-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_streams(spark, rows):
    return spark.createDataFrame(rows, ["user_id", "track_id", "listen_time"])


def make_songs(spark, rows):
    cols = ["track_id", "track_genre", "duration_ms", "track_name", "artists"]
    return spark.createDataFrame(rows, cols)


def test_join_adds_genre_and_duration(spark):
    streams = make_streams(
        spark, [("1", "t1", "2024-06-25 17:43:13")]
    )
    songs = make_songs(spark, [("t1", "pop", "1000", "Song1", "Artist1")])
    joined = tj.join_streams_songs(streams, songs)
    assert "track_genre" in joined.columns
    assert "duration_ms" in joined.columns
    assert joined.count() == 1


def test_date_extraction_from_listen_time(spark):
    streams = make_streams(spark, [("1", "t1", "2024-06-25 17:43:13")])
    cast, _ = tj.cast_stream_types(streams)
    dated = tj.add_date_column(cast)
    value = dated.select("date").collect()[0][0]
    assert str(value) == "2024-06-25"


def test_kpi_aggregation(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", "t1", "2024-06-25 11:00:00"),
            ("1", "t2", "2024-06-25 12:00:00"),
        ],
    )
    songs = make_songs(
        spark,
        [
            ("t1", "pop", "1000", "Song1", "Artist1"),
            ("t2", "pop", "2000", "Song2", "Artist2"),
        ],
    )
    cast, _ = tj.cast_stream_types(streams)
    songs_cast = tj.cast_song_types(songs)
    joined = tj.add_date_column(tj.join_streams_songs(cast, songs_cast))
    kpis = tj.compute_genre_kpis(joined).collect()

    assert len(kpis) == 1
    row = kpis[0]
    assert row["listen_count"] == 3
    assert row["unique_listeners"] == 2          # users 1 and 2
    assert row["total_listening_time_ms"] == 4000  # 1000 + 1000 + 2000


def test_top_3_songs_ranking(spark):
    # 5 songs in one genre on one day, with distinct play counts.
    stream_rows = []
    plays = {"t1": 5, "t2": 4, "t3": 3, "t4": 2, "t5": 1}
    for track, count in plays.items():
        for i in range(count):
            stream_rows.append((str(i), track, "2024-06-25 10:00:00"))
    streams = make_streams(spark, stream_rows)
    songs = make_songs(
        spark,
        [(t, "pop", "1000", "Name_%s" % t, "Artist") for t in plays],
    )
    cast, _ = tj.cast_stream_types(streams)
    joined = tj.add_date_column(
        tj.join_streams_songs(cast, tj.cast_song_types(songs))
    )
    top = tj.compute_top_3_songs(joined).collect()

    assert len(top) == 1
    songs_list = sorted(top[0]["top_3_songs"], key=lambda s: s["rank"])
    assert len(songs_list) == 3
    assert [s["rank"] for s in songs_list] == [1, 2, 3]
    assert songs_list[0]["play_count"] == 5
    assert songs_list[2]["play_count"] == 3


def test_drops_rows_with_null_track_id(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", None, "2024-06-25 11:00:00"),
        ],
    )
    songs = make_songs(spark, [("t1", "pop", "1000", "Song1", "Artist1")])
    joined = tj.join_streams_songs(streams, songs)
    track_ids = [r["track_id"] for r in joined.collect()]
    assert None not in track_ids
    assert joined.count() == 1
