"""
Integration tests for BatchFileProcessor._process_single_file.
"""
import pytest
import json
import uuid
from pyspark.sql.types import StructType, StructField, LongType, StringType
from chispa.dataframe_comparer import assert_df_equality

from app.services.batch.processor import BatchFileProcessor
from app.services.file_state_service import FileStateService
from app.repositories.ingestion_repository import IngestionRepository
from app.models.domain import Ingestion, IngestionStatus
from tests.conftest import TestTableMetadata
from tests.helpers.logger import TestLogger

@pytest.mark.integration
class TestBatchFileProcessor:
    """Test BatchFileProcessor._process_single_file method."""

    @pytest.fixture
    def ingestion(self, test_db):
        """Create test ingestion."""
        repo = IngestionRepository(test_db)
        unique_id = str(uuid.uuid4())
        ingestion = Ingestion(
            id=f"test-ingestion-{unique_id}",
            tenant_id="test-tenant",
            name="Test Batch Processor",
            cluster_id="test-cluster-1",
            source_type="s3",
            source_path="s3://test-bucket/data/",
            source_credentials={
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test"
            },
            format_type="json",
            destination_catalog="test_catalog",
            destination_database="test_db",
            destination_table=f"batch_test_{unique_id[:8]}",
            checkpoint_location=f"/tmp/test-checkpoint-{unique_id}",
            status=IngestionStatus.ACTIVE,
            on_schema_change="append_new_columns",
            schema_version=1,
            created_by="test-user"
        )
        test_db.add(ingestion)
        test_db.commit()
        test_db.refresh(ingestion)
        return ingestion

    def test_process_single_file_success_infer_schema(
        self, test_db, spark_client, spark_session, upload_file, ingestion, random_table_name_generator
    ):
        """Test processing a single file with schema inference."""
        logger = TestLogger()
        logger.section("Integration Test: Process Single File (Infer Schema)")

        # ----------------------------------------------------------------------
        # 1. Setup
        # ----------------------------------------------------------------------
        logger.phase("Setup")
        
        # Upload test file
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        s3_path = upload_file(key=f"data/test_infer_{uuid.uuid4()}.json", content=file_data)
        logger.step(f"Uploaded file to {s3_path}")

        # generate random table name
        table: TestTableMetadata = random_table_name_generator(prefix="batch_infer", logger=logger)

        # Configure ingestion with the table name
        ingestion.destination_table = table.table_name
        test_db.commit()
        logger.step(f"Configured ingestion for table {table}")

        # ----------------------------------------------------------------------
        # 2. Action
        # ----------------------------------------------------------------------
        logger.phase("Action")
        
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        result = processor._process_single_file(s3_path)
        logger.step("Processed file")

        # ----------------------------------------------------------------------
        # 3. Verification
        # ----------------------------------------------------------------------
        logger.phase("Verify")
        
        assert result == {'record_count': 2}
        
        # Verify table content
        df_actual = spark_session.table(table.full_name).orderBy("id")
        
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("score", LongType(), True)
        ])
        
        df_expected = spark_session.createDataFrame(file_data, schema=expected_schema)
        
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
            ignore_row_order=True
        )
        logger.success("Table content matches expected data")

    def test_process_single_file_success_predefined_schema(
        self, test_db, spark_client, spark_session, upload_file, ingestion, random_table_name_generator
    ):
        """Test processing a single file with predefined schema."""
        logger = TestLogger()
        logger.section("Integration Test: Process Single File (Predefined Schema)")

        # ----------------------------------------------------------------------
        # 1. Setup
        # ----------------------------------------------------------------------
        logger.phase("Setup")

        # Configure strict schema
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}
            ]
        }
        ingestion.schema_json = json.dumps(schema)
        
        # generate random table name
        table: TestTableMetadata = random_table_name_generator(prefix="batch_schema", logger=logger)
        
        # Configure ingestion with the table name
        ingestion.destination_table = table.table_name
        test_db.commit()

        # Upload file with extra field 'score'
        file_data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88}
        ]
        s3_path = upload_file(key=f"data/test_schema_{uuid.uuid4()}.json", content=file_data)
        logger.step(f"Uploaded file with extra 'score' field to {s3_path}")

        # ----------------------------------------------------------------------
        # 2. Action
        # ----------------------------------------------------------------------
        logger.phase("Action")
        
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        result = processor._process_single_file(s3_path)
        logger.step("Processed file")

        # ----------------------------------------------------------------------
        # 3. Verification
        # ----------------------------------------------------------------------
        logger.phase("Verify")
        
        assert result == {'record_count': 2}
        
        df_actual = spark_session.table(table.full_name).orderBy("id")
        
        # Expected schema should NOT have 'score'
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True)
        ])
        
        expected_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=expected_schema)
        
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
            ignore_row_order=True
        )
        logger.success("Table content adheres to strict schema (extra column ignored)")

    def test_process_single_file_empty_file(
        self, test_db, spark_client, spark_session, upload_file, ingestion, random_table_name_generator
    ):
        """Test processing an empty file."""
        logger = TestLogger()
        logger.section("Integration Test: Process Empty File")

        # ----------------------------------------------------------------------
        # 1. Setup
        # ----------------------------------------------------------------------
        logger.phase("Setup")
        
        # Upload empty file
        s3_path = upload_file(key=f"data/test_empty_{uuid.uuid4()}.json", content=[])
        
        # generate random table name
        table: TestTableMetadata = random_table_name_generator(prefix="batch_empty", logger=logger)
        
        # Configure ingestion with the table name
        ingestion.destination_table = table.table_name
        test_db.commit()

        # ----------------------------------------------------------------------
        # 2. Action
        # ----------------------------------------------------------------------
        logger.phase("Action")
        
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        
        result = processor._process_single_file(s3_path)
        logger.step("Processed file")

        # ----------------------------------------------------------------------
        # 3. Verification
        # ----------------------------------------------------------------------
        logger.phase("Verify")
        
        assert result == {'record_count': 0}
        
        # Table should not exist because no data was written
        table_exists = spark_session.catalog.tableExists(table.full_name)
        assert not table_exists
        logger.success("Table was not created")

    def test_process_single_file_malformed_file(
        self, test_db, spark_client, spark_session, upload_file, ingestion
    ):
        """Test processing a malformed file."""
        logger = TestLogger()
        logger.section("Integration Test: Process Malformed File")

        # ----------------------------------------------------------------------
        # 1. Setup
        # ----------------------------------------------------------------------
        logger.phase("Setup")
        
        # Upload malformed content (not using fixture because fixture expects JSON serializable content)
        # We could enhance fixture to accept bytes, but for now manual upload is fine for this edge case
        content = b"{ this is not valid json }"
        s3_path = upload_file(key=f"data/test_malformed_{uuid.uuid4()}.json", content=content)
        
        ingestion.format_options = json.dumps({"mode": "FAILFAST"})
        test_db.commit()

        # ----------------------------------------------------------------------
        # 2. Action & Verification
        # ----------------------------------------------------------------------
        logger.phase("Action & Verify")
        
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)

        with pytest.raises(Exception) as err_info:
            processor._process_single_file(s3_path)

        err = err_info.value
        err.user_message = "Malformed data encountered. Fix the source file or switch to PERMISSIVE mode."

        logger.success("Exception raised as expected")

    def test_process_single_file_schema_evolution(
        self, test_db, spark_client, spark_session, upload_file, ingestion, random_table_name_generator
    ):
        """Test schema evolution across two files."""
        logger = TestLogger()
        logger.section("Integration Test: Schema Evolution")

        # ----------------------------------------------------------------------
        # 1. Setup & Action (File 1)
        # ----------------------------------------------------------------------
        logger.phase("Phase 1: Process first file")
        
        # Upload File 1
        file1_data = [{"id": 1, "name": "Alice"}]
        s3_path1 = upload_file(key=f"data/test_evolve_1_{uuid.uuid4()}.json", content=file1_data)
        
        # generate random table name
        table: TestTableMetadata = random_table_name_generator(prefix="batch_evolve", logger=logger)
        
        # Configure ingestion with the table name
        ingestion.destination_table = table.table_name
        test_db.commit()
        
        # Process File 1
        state_service = FileStateService(test_db)
        processor = BatchFileProcessor(spark_client, state_service, ingestion, test_db)
        processor._process_single_file(s3_path1)
        logger.step("Processed file 1")
        
        # ----------------------------------------------------------------------
        # 2. Setup & Action (File 2)
        # ----------------------------------------------------------------------
        logger.phase("Phase 2: Process second file with new column")
        
        # Upload File 2 (New column 'email')
        file2_data = [{"id": 2, "name": "Bob", "email": "bob@example.com"}]
        s3_path2 = upload_file(key=f"data/test_evolve_2_{uuid.uuid4()}.json", content=file2_data)
        
        # Process File 2
        processor._process_single_file(s3_path2)
        logger.step("Processed file 2")
        
        # ----------------------------------------------------------------------
        # 3. Verification
        # ----------------------------------------------------------------------
        logger.phase("Verify")
        
        df_actual = spark_session.table(table.full_name).orderBy("id")
        
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        expected_data = [
            {"id": 1, "name": "Alice", "email": None},
            {"id": 2, "name": "Bob", "email": "bob@example.com"}
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=expected_schema)
        
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
            ignore_row_order=True
        )
        logger.success("Table evolved correctly and contains all data")
