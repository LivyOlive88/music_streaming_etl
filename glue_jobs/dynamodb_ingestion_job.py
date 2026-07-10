"""Glue Job 3 — dynamodb_ingestion_job.py (Python Shell).

Reads the KPI Parquet output produced by the transformation job and writes one
DynamoDB item per (genre, date) into the music_streaming_kpis table.

Runtime : AWS Glue Python Shell (boto3 + pandas + pyarrow available).
Input parameters:
    --processed_bucket  name of the processed S3 bucket
    --dynamodb_table    name of the DynamoDB table (music_streaming_kpis)

Structured as importable functions so reshaping and validation can be unit
tested without touching S3 or DynamoDB.
"""

import json
import logging
import sys
from decimal import Decimal

import boto3
import pandas as pd

# Columns the Parquet KPI dataframe must contain before we write to DynamoDB.
REQUIRED_COLUMNS = [
    "track_genre",
    "date",
    "listen_count",
    "unique_listeners",
    "total_listening_time_ms",
    "avg_listening_time_per_user_ms",
    "is_top_5",
    "top_3_songs",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
logger = logging.getLogger("dynamodb_ingestion_job")


def read_kpis(processed_bucket, reader=pd.read_parquet):
    """Read all KPI Parquet files under ``s3://{processed_bucket}/kpis/``.

    Args:
        processed_bucket: name of the processed S3 bucket.
        reader: parquet reader callable (injectable for testing).

    Raises:
        RuntimeError: if no Parquet data is found.
    """
    path = "s3://%s/kpis/" % processed_bucket
    logger.info("Reading KPI Parquet from %s", path)
    df = reader(path)
    if df is None or len(df) == 0:
        raise RuntimeError("No KPI Parquet data found at %s — nothing to ingest." % path)
    logger.info("Read %d KPI row(s) from Parquet.", len(df))
    return df


def validate_columns(df, required=REQUIRED_COLUMNS):
    """Raise ``ValueError`` if *df* is missing any required column."""
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(
            "KPI dataframe is missing required column(s): %s" % ", ".join(missing)
        )


def _to_native(value):
    """Convert numpy / pandas scalars to plain Python int / float / bool / str."""
    if hasattr(value, "item"):  # numpy scalar
        value = value.item()
    return value


def _to_int(value, field_name, row_identifier):
    """Convert *value* to ``int``, raising a clear error instead of crashing on NaN.

    A KPI Parquet column that contains any nulls gets upcast by pandas to
    float64 with NaN standing in for the null — ``int(float('nan'))`` raises
    a bare ``ValueError: cannot convert float NaN to integer`` with no context
    about which row caused it. This checks for that case explicitly first.
    """
    value = _to_native(value)
    if pd.isna(value):
        raise ValueError(
            "KPI row (%s) has a null/NaN value in required numeric column "
            "'%s' — cannot convert to int." % (row_identifier, field_name)
        )
    return int(value)


def _to_float(value, field_name, row_identifier):
    """Convert *value* to ``float``, raising a clear error instead of a bare NaN."""
    value = _to_native(value)
    if pd.isna(value):
        raise ValueError(
            "KPI row (%s) has a null/NaN value in required numeric column "
            "'%s' — cannot convert to float." % (row_identifier, field_name)
        )
    return float(value)


def _reshape_songs(raw_songs, row_identifier):
    """Normalise the top_3_songs cell into a list of plain-Python dict items."""
    if raw_songs is None:
        return []
    songs = []
    for song in list(raw_songs):
        song = dict(song)
        songs.append(
            {
                "rank": _to_int(song.get("rank"), "top_3_songs.rank", row_identifier),
                "track_name": str(song.get("track_name")),
                "artists": str(song.get("artists")),
                "play_count": _to_int(
                    song.get("play_count"), "top_3_songs.play_count", row_identifier
                ),
            }
        )
    return songs


def reshape_row(row):
    """Reshape a single pandas KPI row into a DynamoDB-ready item dict.

    All numeric values are coerced to plain Python int / float (no numpy types);
    ``date`` is emitted as a "YYYY-MM-DD" string. Float-to-Decimal conversion
    for DynamoDB happens later in ``write_items``.
    """
    date_value = _to_native(row["date"])
    if hasattr(date_value, "strftime"):
        date_value = date_value.strftime("%Y-%m-%d")

    genre = str(_to_native(row["track_genre"]))
    row_identifier = "genre=%s date=%s" % (genre, date_value)

    return {
        "genre": genre,
        "date": str(date_value),
        "listen_count": _to_int(row["listen_count"], "listen_count", row_identifier),
        "unique_listeners": _to_int(
            row["unique_listeners"], "unique_listeners", row_identifier
        ),
        "total_listening_time_ms": _to_int(
            row["total_listening_time_ms"], "total_listening_time_ms", row_identifier
        ),
        "avg_listening_time_per_user_ms": _to_float(
            row["avg_listening_time_per_user_ms"],
            "avg_listening_time_per_user_ms",
            row_identifier,
        ),
        "is_top_5": bool(_to_native(row["is_top_5"])),
        "top_3_songs": _reshape_songs(row["top_3_songs"], row_identifier),
    }


def dataframe_to_items(df):
    """Validate *df* and reshape every row into a DynamoDB item."""
    validate_columns(df)
    return [reshape_row(row) for _, row in df.iterrows()]


def _dynamo_safe(item):
    """Round-trip an item through JSON so every float becomes a Decimal.

    DynamoDB's resource API rejects Python floats; converting via
    ``parse_float=Decimal`` keeps ints as ints and turns floats into Decimals.
    """
    return json.loads(json.dumps(item), parse_float=Decimal)


def write_items(table, items):
    """Write *items* to DynamoDB via ``batch_writer`` (auto-batches 25/call).

    Re-raises any error after logging so Step Functions routes to JobFailed.
    """
    written = 0
    try:
        with table.batch_writer() as batch:
            for item in items:
                try:
                    batch.put_item(Item=_dynamo_safe(item))
                    written += 1
                except Exception:
                    logger.error(
                        "Failed to write item for genre=%s date=%s",
                        item.get("genre"),
                        item.get("date"),
                    )
                    raise
    except Exception as exc:
        logger.error("DynamoDB batch write failed after %d item(s): %s", written, exc)
        raise
    logger.info("Wrote %d item(s) to DynamoDB.", written)
    return written


def get_table(table_name):
    """Return a boto3 DynamoDB Table resource (helper so tests can patch it)."""
    return boto3.resource("dynamodb").Table(table_name)


def run(processed_bucket, table_name, table=None, reader=pd.read_parquet):
    """Read KPIs, reshape, and write to DynamoDB. Testable entry point."""
    logger.info("DynamoDB ingestion job started.")
    df = read_kpis(processed_bucket, reader=reader)
    items = dataframe_to_items(df)
    table = table or get_table(table_name)
    return write_items(table, items)


def _resolve_args(argv):
    """Resolve --processed_bucket and --dynamodb_table from Glue or argparse."""
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore

        opts = getResolvedOptions(argv, ["processed_bucket", "dynamodb_table"])
        return opts["processed_bucket"], opts["dynamodb_table"]
    except Exception:  # noqa: BLE001 - awsglue is unavailable outside Glue
        import argparse

        parser = argparse.ArgumentParser(description="Ingest KPIs into DynamoDB.")
        parser.add_argument("--processed_bucket", required=True)
        parser.add_argument("--dynamodb_table", required=True)
        known, _ = parser.parse_known_args(argv[1:])
        return known.processed_bucket, known.dynamodb_table


def main(argv=None):
    """Glue Python Shell entry point."""
    argv = argv if argv is not None else sys.argv
    processed_bucket, table_name = _resolve_args(argv)
    run(processed_bucket, table_name)


if __name__ == "__main__":
    main()
