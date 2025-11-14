"""
Integration tests for SchemaEvolutionService.apply_schema_evolution with real Iceberg tables.

Tests validate that DDL operations actually work with real Spark and Iceberg.
"""
import pytest
import uuid

from app.services.schema_evolution_service import SchemaEvolutionService


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
        # Generate unique table name
        table_name = f"users_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            # ============================================================
            # SETUP: Create table with base schema
            # ============================================================
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    name STRING
                ) USING iceberg
            """)
            print(f"Created table: {table_id}")

            # Insert initial data
            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES (1, 'Alice'), (2, 'Bob')
            """)
            print(f"Inserted 2 initial records")

            # ============================================================
            # ACTION: Create evolved schema and apply evolution
            # ============================================================
            # Create DataFrame with evolved schema (new columns: email, created_at)
            json_data = [
                {
                    "id": 3,
                    "name": "Charlie",
                    "email": "charlie@example.com",
                    "created_at": "2024-01-15T10:30:00"
                }
            ]
            df = spark_session.createDataFrame(json_data)
            print(f"Created DataFrame with evolved schema")

            # Get target table schema
            target_schema = spark_session.table(table_id).schema

            # Compare schemas
            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect schema changes"
            assert len(comparison.added_columns) == 2, "Should detect 2 new columns (email, created_at)"
            print(f"Schema comparison detected {len(comparison.added_columns)} new columns")

            # Apply evolution with append_new_columns strategy
            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns"
            )
            print(f"Applied schema evolution (append_new_columns)")

            # ============================================================
            # VERIFY: Schema updated correctly
            # ============================================================
            # 1. Table schema has new columns
            updated_table = spark_session.table(table_id)
            field_names = [f.name for f in updated_table.schema.fields]

            assert "id" in field_names, "id column should exist"
            assert "name" in field_names, "name column should exist"
            assert "email" in field_names, "email column should be added"
            assert "created_at" in field_names, "created_at column should be added"
            print(f"VERIFY 1: Table schema has new columns: {field_names}")

            # 2. Old records have NULL for new columns
            old_records = updated_table.filter("id <= 2").collect()
            assert len(old_records) == 2, "Should have 2 old records"
            assert all(row["email"] is None for row in old_records), "Old records should have NULL for email"
            assert all(row["created_at"] is None for row in old_records), "Old records should have NULL for created_at"
            print(f"VERIFY 2: Old records have NULL for new columns")

            # 3. Can insert new records with all columns
            # NOTE: Must specify column names explicitly because column order may differ
            spark_session.sql(f"""
                INSERT INTO {table_id} (id, name, email, created_at)
                VALUES (4, 'David', 'david@example.com', '2024-01-16T14:00:00')
            """)
            new_record = updated_table.filter("id = 4").collect()[0]
            assert new_record["name"] == "David"
            assert new_record["email"] == "david@example.com"
            assert new_record["created_at"] == "2024-01-16T14:00:00"
            print(f"VERIFY 3: Can insert new records with all columns")

            # 4. No data loss (should have 3 records total: 2 initial + 1 new)
            total_count = updated_table.count()
            assert total_count == 3, f"Should have 3 records total, got {total_count}"
            print(f"VERIFY 4: No data loss ({total_count} records)")

            print(f"Test 1 PASSED: append_new_columns happy path works on {table_id}")

        finally:
            # Cleanup
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            print(f"Cleaned up table: {table_id}")
