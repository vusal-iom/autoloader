"""Test script to run Prefect flow directly."""
import asyncio
from app.prefect.flows.run_ingestion import run_ingestion_flow
from app.database import SessionLocal
from app.models.domain import Ingestion, IngestionStatus
from uuid import uuid4
import json
import boto3
from datetime import datetime, timezone

def setup_minio_test_data():
    """Create test bucket and upload sample JSON files to MinIO."""
    print("\n📦 Setting up MinIO test data...")

    # Create MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1"
    )

    bucket_name = "test-bucket"

    # Create bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✓ Created bucket: {bucket_name}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"✓ Bucket already exists: {bucket_name}")
    except Exception as e:
        if "BucketAlreadyExists" not in str(e):
            raise
        print(f"✓ Bucket already exists: {bucket_name}")

    # Upload 3 test JSON files
    num_files = 3
    records_per_file = 100

    for file_idx in range(num_files):
        records = []
        for record_idx in range(records_per_file):
            record_id = file_idx * records_per_file + record_idx
            record = {
                "id": record_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_id": f"user-{record_id % 10}",
                "event_type": ["login", "pageview", "click", "purchase"][record_id % 4],
                "value": record_id * 10.5
            }
            records.append(record)

        # Convert to newline-delimited JSON
        file_content = "\n".join([json.dumps(record) for record in records])

        # Upload to MinIO
        key = f"data/batch_{file_idx}.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=file_content.encode('utf-8')
        )

        print(f"  ✓ Uploaded: {key} ({len(records)} records)")

    print(f"✓ Created {num_files} test files with {num_files * records_per_file} total records\n")
    return bucket_name


def create_test_ingestion():
    """Create a test ingestion in the database."""
    db = SessionLocal()

    # Create a simple test ingestion
    ingestion = Ingestion(
        id=str(uuid4()),
        tenant_id="test-tenant",
        name="Test S3 Ingestion",
        status=IngestionStatus.ACTIVE,
        cluster_id="test-cluster",

        # Source configuration (using MinIO from docker-compose)
        source_type="s3",
        source_path="s3://test-bucket/data/",
        source_file_pattern="*.json",
        source_credentials=json.dumps({
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "endpoint_url": "http://localhost:9000",
            "region": "us-east-1"
        }),

        # Format configuration
        format_type="json",
        format_options=json.dumps({}),

        # Schema configuration
        schema_inference="auto",
        schema_evolution_enabled=True,

        # Destination configuration
        destination_catalog="default",
        destination_database="test_db",
        destination_table="test_table",
        write_mode="append",

        # Schedule configuration (daily at midnight)
        schedule_frequency="daily",
        schedule_time="00:00",
        schedule_timezone="UTC",

        # Other settings
        checkpoint_location=f"s3://checkpoints/test-ingestion-{uuid4()}",
        alerts_enabled=False,
        created_by="test-user",
    )

    db.add(ingestion)
    db.commit()
    db.refresh(ingestion)

    print(f"✓ Created test ingestion: {ingestion.id}")
    print(f"  Name: {ingestion.name}")
    print(f"  Status: {ingestion.status}")

    db.close()
    return ingestion.id


def test_flow_directly(ingestion_id: str):
    """Test the Prefect flow directly (synchronous execution)."""
    print(f"\n🚀 Testing Prefect flow for ingestion: {ingestion_id}")
    print("=" * 60)

    try:
        # Run the flow
        result = run_ingestion_flow(
            ingestion_id=ingestion_id,
            trigger="manual"
        )

        print("\n✓ Flow completed successfully!")
        print(f"Result: {result}")

    except Exception as e:
        print(f"\n✗ Flow failed with error:")
        print(f"  {type(e).__name__}: {e}")


if __name__ == "__main__":
    print("Prefect Flow Test Script")
    print("=" * 60)

    # Setup MinIO test data
    try:
        setup_minio_test_data()
    except Exception as e:
        print(f"\n❌ Failed to setup MinIO test data: {e}")
        print("Make sure Docker Compose is running:")
        print("  docker-compose -f docker-compose.test.yml up -d minio")
        exit(1)

    # Create a test ingestion
    ingestion_id = create_test_ingestion()

    # Test the flow
    test_flow_directly(ingestion_id)

    print("\n" + "=" * 60)
    print("Test complete!")
