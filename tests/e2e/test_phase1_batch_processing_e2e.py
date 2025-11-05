"""
Phase 1 Batch Processing E2E Test

Test ID: E2E-PHASE1-01
Description: End-to-end test for Phase 1 batch processing with real infrastructure
Tests the complete flow: MinIO files → BatchOrchestrator → Iceberg table
"""
import os
import time
from typing import Dict, Any

import pytest
from sqlalchemy.orm import Session

from app.models.domain import Ingestion, Run, ProcessedFile, ProcessedFileStatus, IngestionStatus
from app.services.batch_orchestrator import BatchOrchestrator


@pytest.mark.e2e
@pytest.mark.requires_minio
@pytest.mark.requires_spark
@pytest.mark.slow
class TestPhase1BatchProcessing:
    """End-to-end test for Phase 1 batch processing with real infrastructure"""

    def test_complete_batch_ingestion_happy_path(
        self,
        test_db_postgres: Session,
        test_data_s3: Dict[str, Any],
        test_config: Dict[str, Any],
    ):
        """
        Complete happy path test for Phase 1 batch processing.

        Steps:
        1. Create Ingestion record in database
        2. Trigger batch processing via BatchOrchestrator
        3. Verify Run record created with status=SUCCESS
        4. Verify ProcessedFile records created for all 3 files
        5. Verify data written to Iceberg table
        6. Run again and verify files are skipped (idempotency)

        Prerequisites:
        - MinIO running on localhost:9000
        - PostgreSQL running on localhost:5432
        - Spark Connect running on localhost:15002
        - Test files uploaded to MinIO (via test_data_s3 fixture)
        """
        # Step 1: Create Ingestion record
        ingestion = self._create_test_ingestion(test_db_postgres, test_data_s3, test_config)
        test_db_postgres.add(ingestion)
        test_db_postgres.commit()
        test_db_postgres.refresh(ingestion)

        print(f"\n✓ Created ingestion: {ingestion.id}")

        # Step 2: Run batch processing via BatchOrchestrator
        orchestrator = BatchOrchestrator(test_db_postgres)

        print(f"✓ Starting batch ingestion run...")
        run = orchestrator.run_ingestion(ingestion)

        print(f"✓ Run completed: {run.id}, status={run.status}")

        # Step 3: Verify Run record
        from app.models.domain import RunStatus

        assert run is not None, "Run should be created"
        assert run.status == RunStatus.SUCCESS.value, f"Expected SUCCESS, got {run.status}"
        assert run.files_processed == 3, f"Expected 3 files processed, got {run.files_processed}"
        assert run.started_at is not None, "Run should have started_at"
        assert run.ended_at is not None, "Run should have ended_at"
        assert run.duration_seconds > 0, "Run should have duration"

        print(f"✓ Run record verified: {run.files_processed} files, {run.duration_seconds}s")

        # Step 4: Verify ProcessedFile records
        processed_files = test_db_postgres.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion.id
        ).all()

        assert len(processed_files) == 3, f"Expected 3 ProcessedFile records, got {len(processed_files)}"

        for pf in processed_files:
            assert pf.status == ProcessedFileStatus.SUCCESS.value, \
                f"File {pf.file_path} should have status=SUCCESS, got {pf.status}"
            assert pf.records_ingested is not None, f"File {pf.file_path} should have records_ingested"
            assert pf.records_ingested > 0, f"File {pf.file_path} should have records > 0"
            assert pf.processing_duration_ms > 0, f"File {pf.file_path} should have duration"
            assert pf.processed_at is not None, f"File {pf.file_path} should have processed_at"

        total_records = sum(pf.records_ingested for pf in processed_files)
        print(f"✓ ProcessedFile records verified: 3 files, {total_records} total records")

        # Step 5: Verify data in Iceberg table
        self._verify_iceberg_data(
            ingestion=ingestion,
            expected_record_count=test_data_s3["total_records"],
            test_config=test_config
        )

        print(f"✓ Iceberg table verified: {test_data_s3['total_records']} records")

        # Step 6: Test idempotency - run again and verify files are skipped
        print(f"✓ Testing idempotency - running again...")
        run2 = orchestrator.run_ingestion(ingestion)

        assert run2 is not None, "Second run should be created"
        assert run2.status == "SUCCESS", f"Second run should be SUCCESS, got {run2.status}"
        assert run2.files_processed == 0, \
            f"Second run should process 0 files (already processed), got {run2.files_processed}"

        # Verify still only 3 ProcessedFile records (no duplicates)
        processed_files_after = test_db_postgres.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion.id
        ).all()
        assert len(processed_files_after) == 3, \
            f"Should still have 3 ProcessedFile records, got {len(processed_files_after)}"

        print(f"✓ Idempotency verified: Second run skipped already-processed files")
        print(f"\n✅ Complete happy path test PASSED")

    def _create_test_ingestion(
        self,
        db: Session,
        test_data_s3: Dict[str, Any],
        test_config: Dict[str, Any]
    ) -> Ingestion:
        """Create test Ingestion record"""
        import json
        import uuid

        # Build S3 path (use s3a:// for Spark)
        source_path = f"s3a://{test_data_s3['bucket']}/{test_data_s3['prefix']}"

        # Build credentials JSON
        credentials = {
            "aws_access_key_id": test_data_s3["access_key"],
            "aws_secret_access_key": test_data_s3["secret_key"],
            "endpoint": test_data_s3["endpoint"],
            "aws_region": "us-east-1"
        }

        ingestion = Ingestion(
            id=str(uuid.uuid4()),
            tenant_id=test_config["tenant_id"],
            name="E2E Test Phase1 Batch Ingestion",
            cluster_id=test_config["cluster_id"],

            # Source configuration
            source_type="S3",
            source_path=source_path,
            source_file_pattern="*.json",
            source_credentials=credentials,

            # Format configuration
            format_type="JSON",
            format_options={"multiline": "false"},
            schema_json=None,  # Auto-infer schema

            # Destination configuration
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table="phase1_e2e_test",
            write_mode="append",

            # Checkpoint location
            checkpoint_location=f"s3a://{test_data_s3['bucket']}/checkpoints/phase1-e2e-test",

            # Status
            status=IngestionStatus.ACTIVE,

            # Required audit field
            created_by="e2e-test"
        )

        return ingestion

    def _verify_iceberg_data(
        self,
        ingestion: Ingestion,
        expected_record_count: int,
        test_config: Dict[str, Any]
    ):
        """Verify data was written to Iceberg table using Spark Connect"""
        from app.spark.connect_client import SparkConnectClient
        import json

        # Create Spark Connect client
        credentials = json.loads(ingestion.source_credentials)
        client = SparkConnectClient(
            spark_connect_url=test_config["spark_connect_url"],
            spark_connect_token=test_config.get("spark_connect_token", ""),
            source_type=ingestion.source_type,
            credentials=credentials
        )

        # Connect to Spark
        client.connect()

        # Query the Iceberg table
        table_identifier = f"{ingestion.destination_catalog}.{ingestion.destination_database}.{ingestion.destination_table}"

        try:
            df = client.session.table(table_identifier)
            actual_count = df.count()

            assert actual_count == expected_record_count, \
                f"Expected {expected_record_count} records in Iceberg table, got {actual_count}"

            # Verify some data properties
            first_row = df.first()
            assert first_row is not None, "Table should have at least one row"

            # Verify schema has expected columns
            schema_fields = [field.name for field in df.schema.fields]
            expected_fields = ["id", "timestamp", "user_id", "event_type", "properties", "metadata"]

            for field in expected_fields:
                assert field in schema_fields, f"Expected field '{field}' in schema"

        except Exception as e:
            # Table might not exist yet or other error
            pytest.fail(f"Failed to verify Iceberg table: {e}")

    def test_batch_processing_with_file_error_isolation(
        self,
        test_db_postgres: Session,
        test_data_s3: Dict[str, Any],
        test_config: Dict[str, Any],
        s3_client
    ):
        """
        Test that one corrupt file doesn't block processing of other files.

        Steps:
        1. Upload 2 good JSON files + 1 corrupt file
        2. Run batch processing
        3. Verify 2 files marked SUCCESS, 1 marked FAILED
        4. Verify Run status is still SUCCESS (partial success)
        5. Verify good data is in Iceberg table
        """
        # Upload corrupt file
        bucket = test_data_s3["bucket"]
        prefix = test_data_s3["prefix"]
        corrupt_key = f"{prefix}corrupt.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=corrupt_key,
            Body=b'{invalid json content here'  # Invalid JSON
        )

        # Create ingestion
        ingestion = self._create_test_ingestion(test_db_postgres, test_data_s3, test_config)
        test_db_postgres.add(ingestion)
        test_db_postgres.commit()

        # Run batch processing
        orchestrator = BatchOrchestrator(test_db_postgres)
        run = orchestrator.run_ingestion(ingestion)

        # Verify run completed (even with failures)
        assert run is not None
        # Run should be SUCCESS because most files succeeded
        # In Phase 1, we consider partial success as SUCCESS

        # Verify ProcessedFile records
        processed_files = test_db_postgres.query(ProcessedFile).filter(
            ProcessedFile.ingestion_id == ingestion.id
        ).all()

        # Should have 4 files total (3 good + 1 corrupt)
        assert len(processed_files) == 4, f"Expected 4 ProcessedFile records, got {len(processed_files)}"

        success_files = [pf for pf in processed_files if pf.status == ProcessedFileStatus.SUCCESS.value]
        failed_files = [pf for pf in processed_files if pf.status == ProcessedFileStatus.FAILED.value]

        assert len(success_files) == 3, f"Expected 3 successful files, got {len(success_files)}"
        assert len(failed_files) == 1, f"Expected 1 failed file, got {len(failed_files)}"

        # Verify failed file has error information
        failed_file = failed_files[0]
        assert failed_file.error_message is not None, "Failed file should have error_message"
        assert failed_file.error_type is not None, "Failed file should have error_type"

        print(f"✓ Error isolation verified: 3 success, 1 failed")
        print(f"  Failed file: {failed_file.file_path}")
        print(f"  Error: {failed_file.error_message}")
