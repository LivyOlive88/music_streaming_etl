"""One-time helper — upload the static reference datasets to the reference bucket.

Uploads ``data/songs/songs.csv`` -> ``s3://<reference_bucket>/songs.csv`` and
``data/users/users.csv`` -> ``s3://<reference_bucket>/users.csv``.

This is an operator convenience for when you would rather push the reference
files manually than rely on the Terraform ``aws_s3_object`` resources. Run it
once after the infrastructure is created.

Usage:
    python scripts/upload_reference_data.py --reference_bucket <bucket-name>
    python scripts/upload_reference_data.py --reference_bucket <bucket> \\
        --data_dir /path/to/data
"""

import argparse
import logging
import os
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
logger = logging.getLogger("upload_reference_data")

# Local relative path -> destination key in the reference bucket.
REFERENCE_FILES = {
    os.path.join("songs", "songs.csv"): "songs.csv",
    os.path.join("users", "users.csv"): "users.csv",
}


def upload_reference_data(reference_bucket, data_dir, s3_client=None):
    """Upload each reference file to *reference_bucket*.

    Returns the list of keys uploaded. Raises ``FileNotFoundError`` if a source
    file is missing and ``RuntimeError`` if an upload fails.
    """
    s3_client = s3_client or boto3.client("s3")
    uploaded = []

    for rel_path, key in REFERENCE_FILES.items():
        source = os.path.join(data_dir, rel_path)
        if not os.path.isfile(source):
            raise FileNotFoundError("Reference file not found: %s" % source)

        logger.info("Uploading %s -> s3://%s/%s", source, reference_bucket, key)
        try:
            s3_client.upload_file(source, reference_bucket, key)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(
                "Failed to upload %s to s3://%s/%s: %s"
                % (source, reference_bucket, key, exc)
            ) from exc
        uploaded.append(key)

    logger.info("Uploaded %d reference file(s) to %s.", len(uploaded), reference_bucket)
    return uploaded


def main(argv=None):
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Upload reference CSVs to S3.")
    parser.add_argument("--reference_bucket", required=True)
    parser.add_argument(
        "--data_dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data"),
        help="Path to the local data/ directory (default: ../data).",
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    upload_reference_data(args.reference_bucket, os.path.abspath(args.data_dir))


if __name__ == "__main__":
    main()
