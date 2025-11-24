import json

from pyspark.sql import DataFrame

from app.models.domain import Ingestion
from app.services.batch.errors import FileProcessingError, FileErrorCategory
from app.spark.connect_client import SparkConnectClient
from pyspark.errors import (
    AnalysisException,
    ParseException,
    IllegalArgumentException,
    NumberFormatException,
    PythonException
)


class SparkFileReader:
    """
    Service for reading files into Spark DataFrames.
    Handles format options, credentials, and schema inference.
    """

    def __init__(self, spark_client: SparkConnectClient):
        self.spark_client = spark_client

    def _classify_error(self, error: Exception) -> dict:
        """Classify reader-specific errors."""
        message = str(error) if error else "Unknown error"
        lower = message.lower()

        category = FileErrorCategory.UNKNOWN
        retryable = True
        user_message = message

        # 1. Check for specific PySpark exceptions (more robust)
        if isinstance(error, (NumberFormatException, IllegalArgumentException)):
            category = FileErrorCategory.FORMAT_OPTIONS_INVALID
            retryable = False
            user_message = "Invalid format options. Check mode/options for the reader."
        
        elif isinstance(error, AnalysisException):
            # AnalysisException covers missing files/paths
            if "path does not exist" in lower or "filenotfoundexception" in lower:
                category = FileErrorCategory.PATH_NOT_FOUND
                retryable = False
                user_message = "Source path not found. Verify bucket/key/prefix and retry."
            else:
                # Fallback for other analysis exceptions (e.g. schema issues)
                category = FileErrorCategory.SCHEMA_MISMATCH
                retryable = False
                user_message = "Schema analysis failed. Check if the schema matches the data."

        elif isinstance(error, ParseException):
            category = FileErrorCategory.DATA_MALFORMED
            retryable = False
            user_message = "Malformed data encountered. Fix the source file or switch to PERMISSIVE mode."

        # 2. Fallback to string matching for other errors or generic exceptions
        elif category == FileErrorCategory.UNKNOWN:
            if "malformed" in lower or "bad record" in lower or "parse" in lower:
                category = FileErrorCategory.DATA_MALFORMED
                retryable = False
                user_message = "Malformed data encountered. Fix the source file or switch to PERMISSIVE mode."
            elif "no such bucket" in lower or "nosuchbucket" in lower:
                category = FileErrorCategory.BUCKET_NOT_FOUND
                retryable = False
                user_message = "Source bucket not found. Verify bucket name and permissions."
            elif (
                "nosuchkey" in lower
                or "not found" in lower
                or "does not exist" in lower
            ):
                category = FileErrorCategory.PATH_NOT_FOUND
                retryable = False
                user_message = "Source path not found. Verify bucket/key/prefix and retry."
            elif (
                "unrecognized option" in lower
                or "unsupported option" in lower
                or "invalid" in lower
                or "not a valid" in lower
                or "for input string" in lower
                or "numberformatexception" in lower
            ):
                category = FileErrorCategory.FORMAT_OPTIONS_INVALID
                retryable = False
                user_message = "Invalid format options. Check mode/options for the reader."
            elif "inference" in lower or "unable to infer" in lower or "requires that the schema" in lower:
                category = FileErrorCategory.SCHEMA_INFERENCE_FAILURE
                retryable = False
                user_message = "Schema inference failed. Provide an explicit schema or fix the input."
            elif "access denied" in lower or "permission" in lower or "unauthorized" in lower or "forbidden" in lower:
                category = FileErrorCategory.AUTH
                retryable = False
                user_message = "Authentication/authorization failed when accessing the source. Check credentials."
            elif "connection" in lower or "timeout" in lower or "timed out" in lower:
                category = FileErrorCategory.CONNECTIVITY
                retryable = True
                user_message = "Temporary connectivity issue. Safe to retry."

        return {
            "category": category,
            "retryable": retryable,
            "user_message": user_message
        }

    def _wrap_error(self, file_path: str, error: Exception) -> FileProcessingError:
        """Wrap reader exceptions into FileProcessingError."""
        classification = self._classify_error(error)
        return FileProcessingError(
            category=classification["category"],
            retryable=classification["retryable"],
            user_message=classification["user_message"],
            raw_error=str(error),
            file_path=file_path
        )

    def read_file_with_schema(self, file_path: str, ingestion: Ingestion) -> DataFrame:
        """
        Read file with predefined schema.

        Args:
            file_path: Full S3 path
            ingestion: Ingestion configuration

        Returns:
            Spark DataFrame
        """
        spark = self.spark_client.connect()

        # Configure AWS credentials for S3 access
        if ingestion.source_type == "s3" and ingestion.source_credentials:
            credentials = json.loads(ingestion.source_credentials) if isinstance(ingestion.source_credentials, str) else ingestion.source_credentials
            if credentials.get('aws_access_key_id'):
                spark.conf.set("spark.hadoop.fs.s3a.access.key", credentials['aws_access_key_id'])
                spark.conf.set("spark.hadoop.fs.s3a.secret.key", credentials['aws_secret_access_key'])

        reader = spark.read.format(ingestion.format_type)

        # Apply schema if available
        if ingestion.schema_json:
            from pyspark.sql.types import StructType
            schema_dict = json.loads(ingestion.schema_json) if isinstance(ingestion.schema_json, str) else ingestion.schema_json
            schema = StructType.fromJson(schema_dict)
            reader = reader.schema(schema)

        # Apply format options
        if ingestion.format_options:
            format_options = json.loads(ingestion.format_options) if isinstance(ingestion.format_options, str) else ingestion.format_options
            for key, value in format_options.items():
                reader = reader.option(key, value)

        try:
            return reader.load(file_path)
        except Exception as e:
            raise self._wrap_error(file_path, e)

    def read_file_infer_schema(self, file_path: str, ingestion: Ingestion) -> DataFrame:
        """
        Read file with schema inference.

        Args:
            file_path: Full S3 path
            ingestion: Ingestion configuration

        Returns:
            Spark DataFrame
        """
        spark = self.spark_client.connect()

        # Configure AWS credentials for S3 access
        if ingestion.source_type == "s3" and ingestion.source_credentials:
            credentials = json.loads(ingestion.source_credentials) if isinstance(ingestion.source_credentials, str) else ingestion.source_credentials
            if credentials.get('aws_access_key_id'):
                spark.conf.set("spark.hadoop.fs.s3a.access.key", credentials['aws_access_key_id'])
                spark.conf.set("spark.hadoop.fs.s3a.secret.key", credentials['aws_secret_access_key'])

        reader = spark.read \
            .format(ingestion.format_type) \
            .option("inferSchema", "true")

        # Apply format options
        if ingestion.format_options:
            format_options = json.loads(ingestion.format_options) if isinstance(ingestion.format_options, str) else ingestion.format_options
            for key, value in format_options.items():
                reader = reader.option(key, value)

        df = reader.load(file_path)

        # trigger a very cheap df operation (otherwise df is lazy, it won't throws the exceptions)
        try:
            df.limit(1).collect()
        except Exception as e:
            raise self._wrap_error(file_path, e)

        return df
