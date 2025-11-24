import json

from pyspark.sql import DataFrame

from app.models.domain import Ingestion
from app.services.batch.errors import wrap_error
from app.spark.connect_client import SparkConnectClient


class SparkFileReader:
    """
    Service for reading files into Spark DataFrames.
    Handles format options, credentials, and schema inference.
    """

    def __init__(self, spark_client: SparkConnectClient):
        self.spark_client = spark_client

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
            raise wrap_error(file_path, e)

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
            raise wrap_error(file_path, e)

        return df
