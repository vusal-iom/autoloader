"""
Integration coverage for SparkErrorClassifier using real Spark Connect + MinIO.
"""
from typing import List, Optional
import uuid

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from app.services.batch.errors import FileErrorCategory, FileProcessingError
from app.spark.spark_error_classifier import SparkErrorClassifier

pytestmark = pytest.mark.integration


def classify_and_assert(
    exc: Exception,
    file_path: str,
    expected_category: FileErrorCategory,
    user_message: str,
    expected_retryable: bool = False,
    raw_error_contains: Optional[List[str]] = None,
) -> FileProcessingError:
    """
    Classify exception and assert common expectations.
    Returns the FileProcessingError for additional custom assertions.
    """
    fpe = SparkErrorClassifier.classify(exc, file_path=file_path)

    assert fpe.category == expected_category, f"Expected {expected_category}, got {fpe.category}"
    assert fpe.retryable == expected_retryable
    assert fpe.file_path == file_path
    assert fpe.user_message == user_message, \
        f"Expected '{user_message}', got '{fpe.user_message}'"

    if raw_error_contains:
        raw_lower = fpe.raw_error.lower()
        assert any(c.lower() in raw_lower for c in raw_error_contains), \
            f"Expected one of {raw_error_contains} in raw_error"

    return fpe


def test_bucket_not_found_is_classified(spark_session):
    """Reading from a missing bucket should surface Hadoop NoSuchBucket/UnknownStoreException."""
    missing_path = f"s3a://nonexistent-bucket-{uuid.uuid4()}/missing/file.json"

    with pytest.raises(Exception) as excinfo:
        spark_session.read.json(missing_path).collect()

    classify_and_assert(
        excinfo.value,
        file_path=missing_path,
        expected_category=FileErrorCategory.BUCKET_NOT_FOUND,
        user_message="Source bucket not found.",
        raw_error_contains=["NoSuchBucket", "bucket does not exist", "UnknownStoreException"],
    )


def test_path_not_found_is_classified(spark_session, lakehouse_bucket):
    """Reading from a valid bucket but missing object should be path_not_found."""
    missing_path = f"s3a://{lakehouse_bucket}/missing/{uuid.uuid4()}.json"

    with pytest.raises(Exception) as excinfo:
        spark_session.read.json(missing_path).collect()

    classify_and_assert(
        excinfo.value,
        file_path=missing_path,
        expected_category=FileErrorCategory.PATH_NOT_FOUND,
        user_message="Source path not found.",
        raw_error_contains=["does not exist", "Path does not exist", "FileNotFoundException"],
    )


def test_malformed_json_failfast_is_classified(spark_session, upload_file):
    """Malformed JSON with FAILFAST mode should map to data_malformed."""
    malformed_path = upload_file(
        key=f"data/malformed_{uuid.uuid4()}.json",
        content=b"{ this is not valid json }",
    )

    with pytest.raises(Exception) as excinfo:
        spark_session.read.option("mode", "FAILFAST").json(malformed_path).collect()

    # May hit Tier 1 (ErrorClass) or Tier 4 (format-level) depending on Spark version
    classify_and_assert(
        excinfo.value,
        file_path=malformed_path,
        expected_category=FileErrorCategory.DATA_MALFORMED,
        user_message="Malformed data encountered. Fix input file or adjust reader mode.",
        raw_error_contains=["malformed", "JSON", "parser", "Parse Mode: FAILFAST"],
    )


def test_invalid_format_option_sampling_ratio_is_classified(spark_session, upload_file):
    """Invalid numeric option should map to format_options_invalid."""
    valid_path = upload_file(
        key=f"data/valid_{uuid.uuid4()}.json",
        content=[{"id": 1, "name": "Alice"}],
    )

    with pytest.raises(Exception) as excinfo:
        spark_session.read.option("samplingRatio", "not-a-number").json(valid_path).collect()

    classify_and_assert(
        excinfo.value,
        file_path=valid_path,
        expected_category=FileErrorCategory.FORMAT_OPTIONS_INVALID,
        user_message="Invalid format options. Check numeric parameters.",
        raw_error_contains=["For input string", "invalid", "NumberFormatException"],
    )


def test_corrupt_parquet_is_classified_as_malformed(spark_session, upload_file):
    """Corrupted parquet (invalid magic bytes) should map to data_malformed."""
    # Create a file with parquet magic bytes but invalid content
    corrupt_content = b"PAR1" + b"\x00" * 100 + b"PAR1"

    corrupt_path = upload_file(
        key=f"data/corrupt_parquet_{uuid.uuid4()}.parquet",
        content=corrupt_content,
    )

    with pytest.raises(Exception) as excinfo:
        spark_session.read.parquet(corrupt_path).collect()

    classify_and_assert(
        excinfo.value,
        file_path=corrupt_path,
        expected_category=FileErrorCategory.DATA_MALFORMED,
        user_message="Malformed parquet file. Fix input file or verify format.",
        raw_error_contains=["ParquetFileFormat", "Parquet", "footer"],
    )


def test_analysis_exception_without_path_maps_to_unknown(spark_session):
    """Generic AnalysisException (table not found) should fall back to unknown."""
    with pytest.raises(Exception) as excinfo:
        spark_session.sql("SELECT * FROM nonexistent_table_123").collect()

    # Spark 3.5+ uses TABLE_OR_VIEW_NOT_FOUND error class format
    classify_and_assert(
        excinfo.value,
        file_path="s3a://irrelevant/path.json",
        expected_category=FileErrorCategory.UNKNOWN,
        user_message="Failed to analyze Spark plan.",
        raw_error_contains=["TABLE_OR_VIEW_NOT_FOUND", "cannot be found"],
    )


def test_python_exception_falls_back_to_unknown(spark_session):
    """Python worker errors (UDF) should safely fall back to UNKNOWN."""

    @udf(returnType=IntegerType())
    def boom(_):
        return 1 / 0

    df = spark_session.range(3).withColumn("boom", boom("id"))

    with pytest.raises(Exception) as excinfo:
        df.collect()

    classify_and_assert(
        excinfo.value,
        file_path="s3a://irrelevant/file.json",
        expected_category=FileErrorCategory.UNKNOWN,
        user_message="Unexpected error while reading the file. Check details.",
        raw_error_contains=["Python worker", "ZeroDivisionError", "PythonException"],
    )
