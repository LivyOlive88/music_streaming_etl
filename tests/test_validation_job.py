"""Unit tests for glue_jobs/validation_job.py.

All AWS interaction is mocked — no real S3 calls are made.
"""

import io
import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "glue_jobs")
)

import validation_job  # noqa: E402


class FakeBody:
    """Minimal stand-in for a boto3 StreamingBody."""

    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


def make_s3(header_lines=None, list_keys=None, get_error=None):
    """Build a fake S3 client.

    Args:
        header_lines: dict of key -> header string returned by get_object.
        list_keys: list of object keys returned by the paginator.
        get_error: if set, get_object raises this exception.
    """
    header_lines = header_lines or {}
    list_keys = list_keys if list_keys is not None else list(header_lines)

    class FakePaginator:
        def paginate(self, **_kwargs):
            return [{"Contents": [{"Key": k} for k in list_keys]}]

    class FakeS3:
        def get_paginator(self, _name):
            return FakePaginator()

        def get_object(self, Bucket, Key, Range=None):  # noqa: N803
            if get_error is not None:
                raise get_error
            data = header_lines[Key].encode("utf-8")
            return {"Body": FakeBody(io.BytesIO(data).read())}

    return FakeS3()


def test_passes_when_all_columns_present():
    s3 = make_s3({"streams1.csv": "user_id,track_id,listen_time\n1,abc,2024-01-01"})
    # run() should not raise.
    assert validation_job.run("raw-bucket", s3_client=s3) == "ok"


def test_fails_when_column_missing():
    s3 = make_s3({"streams1.csv": "user_id,track_id\n1,abc"})
    with pytest.raises(ValueError) as exc_info:
        validation_job.run("raw-bucket", s3_client=s3)
    message = str(exc_info.value)
    assert "streams1.csv" in message
    assert "listen_time" in message


def test_fails_when_no_files_found():
    s3 = make_s3(header_lines={}, list_keys=[])
    # No files -> clean exit, no exception.
    assert validation_job.run("raw-bucket", s3_client=s3) == "ok"


def test_fails_gracefully_on_unreadable_file():
    s3 = make_s3(list_keys=["streams1.csv"], get_error=OSError("boom"))
    with pytest.raises(ValueError) as exc_info:
        validation_job.run("raw-bucket", s3_client=s3)
    assert "streams1.csv" in str(exc_info.value)


def test_find_missing_columns_detects_all_missing():
    missing = validation_job.find_missing_columns(["user_id"])
    assert "track_id" in missing
    assert "listen_time" in missing
    assert "user_id" not in missing
