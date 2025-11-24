"""
Integration coverage for SparkErrorClassifier using real Spark Connect + MinIO.
"""
import uuid

import pytest

from app.services.batch.errors import FileErrorCategory
from app.spark.spark_error_classifier import SparkErrorClassifier

pytestmark = pytest.mark.integration


def _assert_message_contains(raw: str, candidates: list[str]) -> bool:
    raw_lower = raw.lower()
    return any(c.lower() in raw_lower for c in candidates)


def test_bucket_not_found_is_classified(
    spark_session,
):
    """
    Reading from a missing bucket should surface Hadoop NoSuchBucket/UnknownStoreException.
    """
    missing_path = f"s3a://nonexistent-bucket-{uuid.uuid4()}/missing/file.json"

    with pytest.raises(Exception) as excinfo:
        spark_session.read.json(missing_path).collect()

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=missing_path)

    assert fpe.category == FileErrorCategory.BUCKET_NOT_FOUND
    assert fpe.retryable is False
    assert fpe.file_path == missing_path
    assert fpe.user_message == "Source bucket not found."
    assert _assert_message_contains(
        fpe.raw_error,
        ["NoSuchBucket", "bucket does not exist", "UnknownStoreException"],
    )


def test_path_not_found_is_classified(
    spark_session,
    lakehouse_bucket,
):
    """
    Reading from a valid bucket but missing object should be path_not_found.
    """
    missing_path = f"s3a://{lakehouse_bucket}/missing/{uuid.uuid4()}.json"

    with pytest.raises(Exception) as excinfo:
        spark_session.read.json(missing_path).collect()

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=missing_path)

    assert fpe.category == FileErrorCategory.PATH_NOT_FOUND
    assert fpe.retryable is False
    assert fpe.file_path == missing_path
    assert fpe.user_message == "Source path not found."
    assert _assert_message_contains(
        fpe.raw_error,
        ["does not exist", "Path does not exist", "FileNotFoundException"],
    )


def test_malformed_json_failfast_is_classified(
    spark_session,
    upload_file,
):
    """
    Malformed JSON with FAILFAST mode should map to data_malformed.
    """
    malformed_path = upload_file(
        key=f"data/malformed_{uuid.uuid4()}.json",
        content=b"{ this is not valid json }",
    )

    with pytest.raises(Exception) as excinfo:
        (
            spark_session.read.option("mode", "FAILFAST")
            .json(malformed_path)
            .collect()
        )

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=malformed_path)

    assert fpe.category == FileErrorCategory.DATA_MALFORMED
    assert fpe.retryable is False
    assert fpe.file_path == malformed_path
    assert _assert_message_contains(
        fpe.raw_error,
        ["malformed", "JSON", "parser", "Parse Mode: FAILFAST"],
    )


def test_invalid_format_option_sampling_ratio_is_classified(
    spark_session,
    upload_file,
):
    """
    Invalid numeric option should map to format_options_invalid.
    """
    valid_path = upload_file(
        key=f"data/valid_{uuid.uuid4()}.json",
        content=[{"id": 1, "name": "Alice"}],
    )

    with pytest.raises(Exception) as excinfo:
        (
            spark_session.read.option("samplingRatio", "not-a-number")
            .json(valid_path)
            .collect()
        )

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=valid_path)

    assert fpe.category == FileErrorCategory.FORMAT_OPTIONS_INVALID
    assert fpe.retryable is False
    assert fpe.file_path == valid_path
    assert _assert_message_contains(
        fpe.raw_error,
        ["For input string", "invalid", "NumberFormatException"],
    )
