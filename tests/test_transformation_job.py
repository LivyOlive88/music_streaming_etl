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


# ---------------------------------------------------------------------------
# tag_invalid_rows — data quality / quarantine tests
# ---------------------------------------------------------------------------

def test_valid_rows_all_pass(spark):
    """All valid rows should appear in clean, quarantine should be None."""
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", "t2", "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 2
    assert quarantine is None


def test_quarantine_null_user_id(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            (None, "t2", "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    assert quarantine.count() == 1
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "null user_id" in reason


def test_quarantine_null_track_id(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", None, "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "null track_id" in reason


def test_quarantine_null_listen_time(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", "t2", None),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "null listen_time" in reason


def test_quarantine_multiple_null_columns(spark):
    """Row with both null user_id and null track_id lists both in the reason."""
    streams = make_streams(spark, [(None, None, "2024-06-25 10:00:00")])
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 0
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "null user_id" in reason
    assert "null track_id" in reason


def test_quarantine_blank_user_id(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("  ", "t2", "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "blank user_id" in reason


def test_quarantine_blank_track_id(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", "", "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "blank track_id" in reason


def test_quarantine_non_numeric_user_id(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("abc", "t2", "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "non-numeric user_id" in reason


def test_quarantine_non_positive_user_id(spark):
    """user_id of 0 and negative values should be quarantined."""
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("0", "t2", "2024-06-25 11:00:00"),
            ("-5", "t3", "2024-06-25 12:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    assert quarantine.count() == 2
    reasons = [r["_quarantine_reason"] for r in quarantine.collect()]
    assert all("non-positive user_id" in r for r in reasons)


def test_quarantine_invalid_timestamp(spark):
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("2", "t2", "not-a-date"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 1
    assert quarantine is not None
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "invalid listen_time" in reason


def test_quarantine_duplicate_rows(spark):
    """Exact duplicate rows: only the first occurrence should be kept."""
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            ("1", "t1", "2024-06-25 10:00:00"),  # duplicate
            ("2", "t2", "2024-06-25 11:00:00"),
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 2
    assert quarantine is not None
    assert quarantine.count() == 1
    reason = quarantine.select("_quarantine_reason").collect()[0][0]
    assert "duplicate row" in reason


def test_quarantine_mixed_failures(spark):
    """A batch with multiple different failure types quarantines all bad rows."""
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),       # valid
            (None, "t2", "2024-06-25 11:00:00"),       # null user_id
            ("abc", "t3", "2024-06-25 12:00:00"),      # non-numeric user_id
            ("2", "t4", "not-a-date"),                  # bad timestamp
            ("3", "t5", "2024-06-25 13:00:00"),        # valid
        ],
    )
    clean, quarantine = tj.tag_invalid_rows(streams)
    assert clean.count() == 2
    assert quarantine is not None
    assert quarantine.count() == 3


def test_quarantine_reason_column_present(spark):
    """quarantine_df must always have a _quarantine_reason column."""
    streams = make_streams(spark, [(None, "t1", "2024-06-25 10:00:00")])
    _, quarantine = tj.tag_invalid_rows(streams)
    assert quarantine is not None
    assert "_quarantine_reason" in quarantine.columns


def test_clean_df_has_no_quarantine_reason_column(spark):
    """clean_df must never carry the _quarantine_reason helper column."""
    streams = make_streams(
        spark,
        [
            ("1", "t1", "2024-06-25 10:00:00"),
            (None, "t2", "2024-06-25 11:00:00"),
        ],
    )
    clean, _ = tj.tag_invalid_rows(streams)
    assert "_quarantine_reason" not in clean.columns
