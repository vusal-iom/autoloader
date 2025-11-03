#!/usr/bin/env python3
"""
Quick connectivity test for Spark Connect in Docker Compose.

Usage:
    python scripts/test_spark_connect.py
"""
import os
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.spark.connect_client import SparkConnectClient


def test_basic_connection():
    """Test basic Spark Connect connectivity."""
    print("=" * 60)
    print("Testing Spark Connect Connectivity")
    print("=" * 60)

    connect_url = os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002")
    connect_token = os.getenv("TEST_SPARK_CONNECT_TOKEN", "")

    print(f"\n1. Connecting to: {connect_url}")

    client = SparkConnectClient(connect_url, connect_token)

    try:
        result = client.test_connection()

        if result["status"] == "success":
            print(f"   ✅ Connection successful!")
            print(f"   Spark Version: {result['spark_version']}")
            return True
        else:
            print(f"   ❌ Connection failed: {result['error']}")
            return False

    except Exception as e:
        print(f"   ❌ Connection error: {e}")
        return False
    finally:
        client.stop()


def test_s3_access():
    """Test S3/MinIO access from Spark."""
    print("\n2. Testing S3/MinIO Access")

    connect_url = os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002")
    connect_token = os.getenv("TEST_SPARK_CONNECT_TOKEN", "")

    client = SparkConnectClient(connect_url, connect_token)

    try:
        spark = client.connect()

        # Configure S3 access
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        print("   ✅ S3 configuration set")

        # Try to read from S3 (will fail if bucket doesn't exist, but connection works)
        try:
            df = spark.read.format("binaryFile").load("s3a://test-ingestion-bucket/")
            count = df.count()
            print(f"   ✅ Successfully accessed S3! Found {count} files")
        except Exception as e:
            # Expected if bucket doesn't exist yet
            if "NoSuchBucket" in str(e) or "does not exist" in str(e):
                print("   ⚠️  S3 accessible but test bucket doesn't exist yet (this is OK)")
            else:
                print(f"   ❌ S3 access error: {e}")
                return False

        return True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    finally:
        client.stop()


def test_iceberg_catalog():
    """Test Iceberg catalog configuration."""
    print("\n3. Testing Iceberg Catalog")

    connect_url = os.getenv("TEST_SPARK_CONNECT_URL", "sc://localhost:15002")
    connect_token = os.getenv("TEST_SPARK_CONNECT_TOKEN", "")

    client = SparkConnectClient(connect_url, connect_token)

    try:
        spark = client.connect()

        # Check if test_catalog is configured
        catalogs = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [row[0] for row in catalogs]

        if "test_catalog" in catalog_names:
            print(f"   ✅ test_catalog is available")
            print(f"   Available catalogs: {', '.join(catalog_names)}")
            return True
        else:
            print(f"   ❌ test_catalog not found")
            print(f"   Available catalogs: {', '.join(catalog_names)}")
            return False

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    finally:
        client.stop()


def main():
    """Run all connectivity tests."""
    print("\n🚀 Spark Connect Test Suite\n")

    results = []

    # Test 1: Basic connectivity
    results.append(("Basic Connection", test_basic_connection()))

    # Test 2: S3 access
    results.append(("S3/MinIO Access", test_s3_access()))

    # Test 3: Iceberg catalog
    results.append(("Iceberg Catalog", test_iceberg_catalog()))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {name}")

    all_passed = all(result[1] for result in results)

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All tests passed! Spark Connect is ready for E2E testing.")
        return 0
    else:
        print("❌ Some tests failed. Check the logs above for details.")
        print("\nTroubleshooting:")
        print("1. Ensure docker-compose is running: docker-compose -f docker-compose.test.yml up -d")
        print("2. Check service health: docker-compose -f docker-compose.test.yml ps")
        print("3. View logs: docker logs autoloader-test-spark-connect")
        return 1


if __name__ == "__main__":
    sys.exit(main())
