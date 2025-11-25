"""
Robust classification of Spark / Spark Connect / Hadoop exceptions
into IOMETE domain-level FileProcessingError categories.

This classifier:
- Uses Spark ErrorClass when available (stable API)
- Uses JVM exception types (AnalysisException, NumberFormatException, etc.)
- Uses Hadoop canonical exception classes (UnknownStoreException, NoSuchBucket)
- Uses format-level indicators (malformed JSON/CSV/Parquet)
- Only uses fallback string heuristics when unavoidable
"""

from typing import Optional
import traceback

from pyspark.errors.exceptions.captured import CapturedException
from pyspark.errors.exceptions.connect import (NumberFormatException,
                                               SparkConnectGrpcException)
from pyspark.sql.utils import AnalysisException

from app.services.batch.errors import FileErrorCategory, FileProcessingError


class SparkErrorClassifier:
    """Normalize any Spark/Hadoop/Py4J/Spark-Connect error into a FileProcessingError."""

    # -----------------------------
    # Public API
    # -----------------------------
    @classmethod
    def classify(cls, exc: Exception, file_path: str) -> FileProcessingError:
        raw = cls._extract_raw_error(exc)
        spark_err_class = cls._extract_spark_error_class(exc)

        # ---- TIER 1: Spark ErrorClass (Most Stable) ----
        fpe = cls._classify_error_class(spark_err_class, raw, file_path)
        if fpe:
            return fpe

        # ---- TIER 2: JVM Exception Types ----
        fpe = cls._classify_jvm_exception_type(exc, raw, file_path)
        if fpe:
            return fpe

        # ---- TIER 3: Hadoop canonical exceptions ----
        fpe = cls._classify_hadoop_exceptions(raw, file_path)
        if fpe:
            return fpe

        # ---- TIER 4: Format-level errors ----
        fpe = cls._classify_format_level(raw, file_path)
        if fpe:
            return fpe

        # ---- TIER 5: Fallback ----
        return FileProcessingError(
            category=FileErrorCategory.UNKNOWN,
            retryable=False,
            file_path=file_path,
            user_message="Unexpected error while reading the file. Check details.",
            raw_error=raw,
        )

    # ========================================================================
    # Internal helpers
    # ========================================================================

    @staticmethod
    def _extract_raw_error(exc: Exception) -> str:
        """
        Returns a readable raw error string from SparkConnectGrpcException,
        CapturedException, or generic Python exception.
        """
        # SparkConnectGrpcException stores message in .message
        if isinstance(exc, SparkConnectGrpcException):
            return str(exc.message)

        # CapturedException overrides __str__ to include JVM stacktrace
        if isinstance(exc, CapturedException):
            return str(exc)

        # Generic fallback
        return "".join(traceback.format_exception_only(type(exc), exc)).strip()

    @staticmethod
    def _extract_spark_error_class(exc: Exception) -> Optional[str]:
        """
        Spark ErrorClass is available for:
        - SparkConnectGrpcException (Connect mode)
        - CapturedException if underlying JVM exception is SparkThrowable
        """
        get_cls = getattr(exc, "getErrorClass", None)
        if callable(get_cls):
            try:
                return get_cls()
            except Exception:
                return None
        return None

    # ========================================================================
    # Tier 1 — Spark ErrorClass (Stable API)
    # ========================================================================
    @staticmethod
    def _classify_error_class(error_class: Optional[str], raw: str, file_path: str):
        if not error_class:
            return None

        # Path does not exist (SparkConnect)
        if error_class == "PATH_NOT_FOUND":
            return FileProcessingError(
                category=FileErrorCategory.PATH_NOT_FOUND,
                retryable=False,
                file_path=file_path,
                user_message="Source path not found. Verify bucket/key/prefix.",
                raw_error=raw,
            )

        # Malformed record inference error
        if error_class in ("MALFORMED_RECORD", "JSON_PARSER_FAILURE"):
            return FileProcessingError(
                category=FileErrorCategory.DATA_MALFORMED,
                retryable=False,
                file_path=file_path,
                user_message="Malformed data encountered. Fix file or switch reader mode.",
                raw_error=raw,
            )

        # Invalid options given to datasource
        if error_class == "INVALID_FORMAT_OPTION":
            return FileProcessingError(
                category=FileErrorCategory.FORMAT_OPTIONS_INVALID,
                retryable=False,
                file_path=file_path,
                user_message="Invalid format options for the reader.",
                raw_error=raw,
            )

        return None

    # ========================================================================
    # Tier 2 — JVM Exception Type (Spark / Py4J)
    # ========================================================================
    @staticmethod
    def _classify_jvm_exception_type(exc: Exception, raw: str, file_path: str):
        # AnalysisException wraps many kinds of path/bucket issues
        if isinstance(exc, AnalysisException):
            if "Path does not exist" in raw:
                return FileProcessingError(
                    category=FileErrorCategory.PATH_NOT_FOUND,
                    retryable=False,
                    file_path=file_path,
                    user_message="Source path not found.",
                    raw_error=raw,
                )

            return FileProcessingError(
                category=FileErrorCategory.UNKNOWN,
                retryable=False,
                file_path=file_path,
                user_message="Failed to analyze Spark plan.",
                raw_error=raw,
            )

        # Spark NumberFormatException → invalid format options (e.g., samplingRatio)
        if isinstance(exc, NumberFormatException):
            return FileProcessingError(
                category=FileErrorCategory.FORMAT_OPTIONS_INVALID,
                retryable=False,
                file_path=file_path,
                user_message="Invalid format options. Check numeric parameters.",
                raw_error=raw,
            )

        return None

    # ========================================================================
    # Tier 3 — Hadoop canonical exceptions
    # ========================================================================
    @staticmethod
    def _classify_hadoop_exceptions(raw: str, file_path: str):
        # Bucket does not exist
        if "NoSuchBucket" in raw or "bucket does not exist" in raw or "UnknownStoreException" in raw:
            return FileProcessingError(
                category=FileErrorCategory.BUCKET_NOT_FOUND,
                retryable=False,
                file_path=file_path,
                user_message="Source bucket not found.",
                raw_error=raw,
            )

        # File / key does not exist
        if (
            "FileNotFoundException" in raw
            or "does not exist" in raw
            or "Path does not exist" in raw
            or "Not Found" in raw
        ):
            return FileProcessingError(
                category=FileErrorCategory.PATH_NOT_FOUND,
                retryable=False,
                file_path=file_path,
                user_message="Source path not found.",
                raw_error=raw,
            )

        return None

    # ========================================================================
    # Tier 4 — Format-level patterns (JSON/CSV/Parquet)
    # ========================================================================
    @staticmethod
    def _classify_format_level(raw: str, file_path: str):
        malformed_markers = [
            "Malformed records",
            "malformed",
            "Parse Mode: FAILFAST",
            "corrupt",
        ]

        if any(m.lower() in raw.lower() for m in malformed_markers):
            return FileProcessingError(
                category=FileErrorCategory.DATA_MALFORMED,
                retryable=False,
                file_path=file_path,
                user_message="Malformed data encountered. Fix input file or adjust reader mode.",
                raw_error=raw,
            )

        # format option errors
        if "For input string" in raw or "Invalid" in raw or "cannot parse" in raw.lower():
            return FileProcessingError(
                category=FileErrorCategory.FORMAT_OPTIONS_INVALID,
                retryable=False,
                file_path=file_path,
                user_message="Invalid reader format option.",
                raw_error=raw,
            )

        return None