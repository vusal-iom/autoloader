"""
Integration tests for SchemaEvolutionService.apply_schema_evolution with real Iceberg tables.

Tests validate that DDL operations actually work with real Spark and Iceberg.
"""
import pytest
import uuid
from typing import Any, Dict, List

from app.services.schema_evolution_service import SchemaEvolutionService
from tests.e2e.helpers.assertions import verify_table_content
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestSchemaEvolutionApply:
    """Integration tests for apply_schema_evolution with real Iceberg tables."""

    def test_append_new_columns_happy_path(self, spark_session):
        """
        Test 1: Append New Columns Strategy - Happy Path

        Validates the most common evolution scenario with real DDL:
        - Create table with base schema
        - Insert initial data
        - Evolve schema with new columns
        - Verify DDL works and data integrity is maintained
        """
        logger = TestLogger()
        logger.section("Integration Test: Append New Columns Happy Path")

        table_name = f"users_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            logger.phase("Setup: Create table with base schema")
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    name STRING
                ) USING iceberg
            """)
            logger.step(f"Created table: {table_id}", always=True)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES (1, 'Alice'), (2, 'Bob')
            """)
            logger.step("Inserted 2 initial records", always=True)

            logger.phase("Action: Create evolved schema and apply evolution")

            # Type-annotated data to fix diagnostic warning
            json_data: List[Dict[str, Any]] = [
                {
                    "id": 3,
                    "name": "Charlie",
                    "email": "charlie@example.com",
                    "created_at": "2024-01-15T10:30:00"
                }
            ]
            df = spark_session.createDataFrame(json_data)
            logger.step("Created DataFrame with evolved schema", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect schema changes"
            assert len(comparison.added_columns) == 2, "Should detect 2 new columns (email, created_at)"
            logger.step(f"Schema comparison detected {len(comparison.added_columns)} new columns", always=True)

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns"
            )
            logger.step("Applied schema evolution (append_new_columns)", always=True)

            logger.phase("Verify: Schema updated correctly")

            # 1. Verify table schema has new columns
            updated_table = spark_session.table(table_id)
            field_names = {f.name for f in updated_table.schema.fields}

            assert field_names == {"id", "name", "email", "created_at"}
            logger.step(f"Table schema has new columns: {field_names}", always=True)

            # 2. Verify old records have NULL for new columns
            verify_table_content(
                df_or_table=updated_table,
                expected_data=[
                    {"id": 1, "name": "Alice", "email": None, "created_at": None},
                    {"id": 2, "name": "Bob", "email": None, "created_at": None},
                ],
                spark_session=spark_session,
                logger=logger
            )
            logger.step("Old records have NULL for new columns", always=True)

            # 3. Insert DataFrame with evolved schema and verify
            df.writeTo(table_id).append()

            # 4. Verify no data loss - all records present with correct values
            verify_table_content(
                df_or_table=table_id,
                expected_data=[
                    {"id": 1, "name": "Alice", "email": None, "created_at": None},
                    {"id": 2, "name": "Bob", "email": None, "created_at": None},
                    {"id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2024-01-15T10:30:00"},
                ],
                spark_session=spark_session,
                logger=logger
            )
            logger.success(f"All verifications passed for {table_id}", always=True)

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            logger.step(f"Cleaned up table: {table_id}", always=True)
