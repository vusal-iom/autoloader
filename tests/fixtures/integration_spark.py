from typing import Generator

import pytest
from pyspark.sql import SparkSession

from app.spark.connect_client import SparkConnectClient
from .ensure_services_ready import ensure_services_ready
from .integration_minio import lakehouse_bucket

@pytest.fixture(scope="session")
def spark_connect_url() -> str:
    return "sc://localhost:15002"


@pytest.fixture(scope="session")
def spark_client(ensure_services_ready, lakehouse_bucket, spark_connect_url: str) -> Generator[SparkConnectClient, None, None]:
    # MinIO credentials for S3 access
    s3_credentials = {
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin"
    }

    try:
        client = SparkConnectClient(
            connect_url=spark_connect_url,
            token="",
            s3_credentials=s3_credentials
        )
        spark_session = client.connect()
        print(f"Connected to Spark: {spark_connect_url}")

        yield client

        client.stop()
        print("Stopped Spark session")
    except Exception as e:
        raise RuntimeError(
            f"Failed to connect to Spark Connect at {spark_connect_url}: {e}\n"
            "Ensure Spark is running: docker-compose -f docker-compose.test.yml up -d"
        )

@pytest.fixture(scope="session")
def spark_session(spark_client) -> Generator[SparkSession, None, None]:
    yield spark_client.connect()