"""
Integration coverage for SparkErrorClassifier using real Spark Connect + MinIO.
"""
import tempfile
import uuid
from pathlib import Path

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

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


def test_missing_directory_path_not_found(
    spark_session,
    lakehouse_bucket,
):
    """
    Missing directory (glob-less) also maps to path_not_found.
    """
    missing_dir = f"s3a://{lakehouse_bucket}/missing_dir_{uuid.uuid4()}"

    with pytest.raises(Exception) as excinfo:
        spark_session.read.json(missing_dir).collect()

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=missing_dir)

    assert fpe.category == FileErrorCategory.PATH_NOT_FOUND
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
    assert fpe.user_message == "Malformed data encountered. Fix file or switch reader mode."
    assert _assert_message_contains(
        fpe.raw_error,
        ["malformed", "JSON", "parser", "Parse Mode: FAILFAST"],
    )


def test_malformed_csv_failfast_is_classified(
    spark_session,
    upload_file,
):
    """
    Malformed CSV row in FAILFAST mode should map to data_malformed.
    """
    csv_content = "id,name\n1,Alice\nbadline\n2,Bob"
    malformed_path = upload_file(
        key=f"data/malformed_csv_{uuid.uuid4()}.csv",
        content=csv_content,
    )

    with pytest.raises(Exception) as excinfo:
        (
            spark_session.read.option("header", "true")
            .option("mode", "FAILFAST")
            .csv(malformed_path)
            .collect()
        )

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=malformed_path)

    assert fpe.category == FileErrorCategory.DATA_MALFORMED
    assert fpe.retryable is False
    assert fpe.user_message == "Malformed data encountered. Fix input file or adjust reader mode."
    assert _assert_message_contains(
        fpe.raw_error,
        ["malformed", "CSV", "Parse Mode: FAILFAST", "corrupt"],
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
    assert fpe.user_message == "Invalid format options. Check numeric parameters."
    assert _assert_message_contains(
        fpe.raw_error,
        ["For input string", "invalid", "NumberFormatException"],
    )


def test_invalid_format_option_mode_is_classified(
    spark_session,
    upload_file,
):
    """
    Invalid mode option should map to format_options_invalid.
    """
    csv_path = upload_file(
        key=f"data/valid_csv_{uuid.uuid4()}.csv",
        content="id,name\n1,Alice\n2,Bob",
    )

    with pytest.raises(Exception) as excinfo:
        (
            spark_session.read.option("mode", "NOT_A_MODE")
            .csv(csv_path)
            .collect()
        )

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=csv_path)

    assert fpe.category == FileErrorCategory.FORMAT_OPTIONS_INVALID
    assert fpe.retryable is False
    assert fpe.user_message == "Invalid reader format option."
    assert _assert_message_contains(
        fpe.raw_error,
        ["Invalid", "mode", "NOT_A_MODE", "illegal"],
    )


def test_corrupt_parquet_is_classified_as_malformed(
    spark_session,
    upload_file,
):
    """
    Corrupted parquet (truncated footer) should map to data_malformed.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        df = spark_session.createDataFrame([("Alice", 1)], ["name", "id"])
        df.write.parquet(tmpdir)
        part_files = list(Path(tmpdir).glob("part-*"))
        assert part_files, "No parquet part files produced"
        original_bytes = part_files[0].read_bytes()
        truncated = original_bytes[: max(1, len(original_bytes) // 3)]

    corrupt_path = upload_file(
        key=f"data/corrupt_parquet_{uuid.uuid4()}.parquet",
        content=truncated,
    )

    with pytest.raises(Exception) as excinfo:
        spark_session.read.parquet(corrupt_path).collect()

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=corrupt_path)

    assert fpe.category == FileErrorCategory.DATA_MALFORMED
    assert fpe.retryable is False
    assert fpe.user_message == "Malformed data encountered. Fix input file or adjust reader mode."
    assert _assert_message_contains(
        fpe.raw_error,
        ["corrupt", "footer", "Malformed", "Parquet"],
    )


def test_glob_no_match_maps_to_path_not_found(
    spark_session,
    lakehouse_bucket,
):
    """
    Glob with no matches should map to path_not_found.
    """
    missing_glob = f"s3a://{lakehouse_bucket}/no_match_{uuid.uuid4()}/*/*.json"

    with pytest.raises(Exception) as excinfo:
        spark_session.read.json(missing_glob).collect()

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path=missing_glob)

    assert fpe.category == FileErrorCategory.PATH_NOT_FOUND
    assert fpe.retryable is False
    assert fpe.user_message == "Source path not found."
    assert _assert_message_contains(
        fpe.raw_error,
        ["does not exist", "Path does not exist", "FileNotFoundException"],
    )


def test_analysis_exception_without_path_maps_to_unknown(
    spark_session,
):
    """
    Generic AnalysisException (table not found) should fall back to unknown.
    """
    with pytest.raises(Exception) as excinfo:
        spark_session.sql("SELECT * FROM nonexistent_table_123").collect()

    fpe = SparkErrorClassifier.classify(
        excinfo.value, file_path="s3a://irrelevant/path.json"
    )

    assert fpe.category == FileErrorCategory.UNKNOWN
    assert fpe.retryable is False
    assert fpe.user_message == "Failed to analyze Spark plan."
    assert _assert_message_contains(fpe.raw_error, ["AnalysisException", "table or view not found"])


def test_python_exception_falls_back_to_unknown(
    spark_session,
):
    """
    Python worker errors (UDF) should safely fall back to UNKNOWN.
    """

    @udf(returnType=IntegerType())
    def boom(x):
        return 1 / 0

    df = spark_session.range(3).withColumn("boom", boom("id"))

    with pytest.raises(Exception) as excinfo:
        df.collect()

    fpe = SparkErrorClassifier.classify(excinfo.value, file_path="s3a://irrelevant/file.json")

    assert fpe.category == FileErrorCategory.UNKNOWN
    assert fpe.retryable is False
    assert _assert_message_contains(
        fpe.raw_error,
        ["Python worker", "ZeroDivisionError", "PythonException"],
    )
