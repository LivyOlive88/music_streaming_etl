"""Glue Job 1 — validation_job.py (Python Shell).

Validates the schema of incoming stream CSV files in the raw S3 bucket.

Runtime : AWS Glue Python Shell (boto3 + pandas available).
Trigger : First state in the Step Functions state machine.
Input   : --raw_bucket  (name of the raw S3 bucket, passed by Step Functions).
Output  : Raises an exception if validation fails (Step Functions catches it and
          routes to the JobFailed state). Exits cleanly if all files pass.

The module is intentionally structured as small, importable functions so that the
individual pieces can be unit tested without making real AWS calls.
"""

import logging
import sys

import boto3
from botocore.exceptions import ClientError

# The three columns every stream file must contain.
REQUIRED_COLUMNS = ["user_id", "track_id", "listen_time"]

# How many bytes to read from the start of each object when only the header is
# needed. A few KB is far more than enough for a single CSV header line.
HEADER_BYTE_RANGE = 8192

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
logger = logging.getLogger("validation_job")


def get_s3_client():
    """Return a boto3 S3 client. Wrapped in a helper so tests can patch it."""
    return boto3.client("s3")


def list_csv_files(s3_client, bucket):
    """List the keys of every ``.csv`` object in *bucket*.

    Raises:
        RuntimeError: if the bucket does not exist or cannot be accessed.
    """
    keys = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                if obj["Key"].lower().endswith(".csv"):
                    keys.append(obj["Key"])
    except ClientError as exc:
        raise RuntimeError(
            f"Could not list objects in bucket '{bucket}': {exc}"
        ) from exc
    return keys


def read_header(s3_client, bucket, key):
    """Read and return the column names from the header row of *key*.

    Only the first ``HEADER_BYTE_RANGE`` bytes are fetched via a ranged
    ``get_object`` call — the full file is never downloaded.
    """
    response = s3_client.get_object(
        Bucket=bucket, Key=key, Range=f"bytes=0-{HEADER_BYTE_RANGE - 1}"
    )
    body = response["Body"].read()
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    first_line = body.splitlines()[0] if body else ""
    return [column.strip() for column in first_line.split(",") if column.strip()]


def find_missing_columns(header, required=REQUIRED_COLUMNS):
    """Return the list of *required* columns that are absent from *header*."""
    present = set(header)
    return [column for column in required if column not in present]


def validate_files(s3_client, bucket, required=REQUIRED_COLUMNS):
    """Validate every CSV in *bucket*.

    Returns:
        dict mapping ``filename -> reason`` for each file that failed
        validation. An empty dict means every file passed.

    Behaviour:
        * No files found -> logs a warning and returns ``{}`` (clean exit).
        * A file whose header is missing required columns -> recorded as a
          failure listing the missing columns.
        * A file that cannot be read at all -> recorded as a failure (the job
          does not crash on a single unreadable file).
    """
    logger.info("Validation job started for bucket '%s'.", bucket)

    csv_keys = list_csv_files(s3_client, bucket)
    logger.info("Found %d .csv file(s) in raw bucket.", len(csv_keys))

    if not csv_keys:
        logger.warning(
            "No .csv files found in bucket '%s' — nothing to validate. "
            "Exiting cleanly.",
            bucket,
        )
        return {}

    failures = {}
    for key in csv_keys:
        logger.info("Checking file '%s'.", key)
        try:
            header = read_header(s3_client, bucket, key)
        except Exception as exc:  # noqa: BLE001 - any read error is a failure
            logger.error("Could not read header of '%s': %s", key, exc)
            failures[key] = f"unreadable file: {exc}"
            continue

        missing = find_missing_columns(header, required)
        if missing:
            logger.error(
                "File '%s' is missing required column(s): %s",
                key,
                ", ".join(missing),
            )
            failures[key] = f"missing columns: {', '.join(missing)}"
        else:
            logger.info("File '%s' passed schema validation.", key)

    return failures


def run(bucket, s3_client=None):
    """Validate *bucket* and raise ``ValueError`` if any file failed.

    This is the testable entry point: it raises on failure and returns the
    number of validated files on success.
    """
    s3_client = s3_client or get_s3_client()
    failures = validate_files(s3_client, bucket)

    if failures:
        report = "; ".join(f"{name} ({reason})" for name, reason in failures.items())
        message = f"Schema validation failed for {len(failures)} file(s): {report}"
        logger.error(message)
        raise ValueError(message)

    logger.info("All files passed schema validation. Validation job succeeded.")
    return "ok"


def _resolve_raw_bucket(argv):
    """Resolve the --raw_bucket argument.

    Uses Glue's getResolvedOptions when running inside Glue; falls back to a
    minimal argparse parser so the script is runnable/testable locally.
    """
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore

        return getResolvedOptions(argv, ["raw_bucket"])["raw_bucket"]
    except Exception:  # noqa: BLE001 - awsglue is unavailable outside Glue
        import argparse

        parser = argparse.ArgumentParser(description="Validate raw stream CSVs.")
        parser.add_argument("--raw_bucket", required=True)
        known, _ = parser.parse_known_args(argv[1:])
        return known.raw_bucket


def main(argv=None):
    """Glue Python Shell entry point."""
    argv = argv if argv is not None else sys.argv
    raw_bucket = _resolve_raw_bucket(argv)
    run(raw_bucket)


if __name__ == "__main__":
    main()
