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

    def test_append_new_nested_field_in_struct(self, spark_session):
        """
        Test 2: Append New Nested Field in Struct - Happy Path

        Scenario:
        - Base table has a nested `profile` struct with (name, age)
        - New data introduces `profile.email`
        - We expect the evolution to add `email` inside `profile`
        - Old records should have profile.email = NULL
        """
        logger = TestLogger()
        logger.section("Integration Test: Append New Nested Field in Struct")

        table_name = f"users_nested_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            logger.phase("Setup: Create table with nested struct schema")
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    profile STRUCT<
                        name: STRING,
                        age: INT
                    >
                ) USING iceberg
            """)
            logger.step(f"Created table with nested struct: {table_id}", always=True)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES
                    (1, named_struct('name', 'Alice', 'age', 30)),
                    (2, named_struct('name', 'Bob',   'age', 40))
            """)
            logger.step("Inserted 2 initial records with profile struct", always=True)

            logger.phase("Action: Create evolved schema with extra nested field and apply evolution")

            # JSON-like data with new nested field profile.email
            json_data: List[Dict[str, Any]] = [
                {
                    "id": 3,
                    "profile": {
                        "name": "Charlie",
                        "age": 25,
                        "email": "charlie@example.com",
                    },
                }
            ]
            df = spark_session.createDataFrame(json_data)
            logger.step("Created DataFrame with evolved nested schema (profile.email)", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect schema changes for nested struct"
            assert len(comparison.added_columns) == 1, "Should detect 1 new nested column (profile.email)"
            logger.step(
                f"Schema comparison detected {len(comparison.added_columns)} new nested columns",
                always=True,
            )

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns",
            )
            logger.step("Applied schema evolution (append_new_columns) for nested struct", always=True)

            logger.phase("Verify: Nested schema updated correctly")

            updated_table = spark_session.table(table_id)
            table_schema = updated_table.schema

            # Get nested 'profile' struct and its fields
            profile_field = table_schema["profile"]
            profile_struct = profile_field.dataType
            nested_field_names = {f.name for f in profile_struct.fields}

            assert nested_field_names == {"name", "age", "email"}
            logger.step(
                f"Profile struct has expected nested fields: {nested_field_names}",
                always=True,
            )

            # Verify old records have NULL for new nested field
            verify_table_content(
                df_or_table=updated_table,
                expected_data=[
                    {
                        "id": 1,
                        "profile": {"name": "Alice", "age": 30, "email": None},
                    },
                    {
                        "id": 2,
                        "profile": {"name": "Bob", "age": 40, "email": None},
                    },
                    {
                        "id": 3,
                        "profile": {
                            "name": "Charlie",
                            "age": 25,
                            "email": "charlie@example.com",
                        },
                    },
                ],
                spark_session=spark_session,
                logger=logger,
            )
            logger.success(f"All nested verifications passed for {table_id}", always=True)

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            logger.error(f"Cleaned up table: {table_id}")

    def test_append_nested_struct_inside_struct(self, spark_session):
        """
        Test 3: Append Nested Struct Inside Existing Struct

        Scenario:
        - Base table has `profile` struct with only `name`
        - New data introduces `profile.details` as a nested struct with (age, country)
        - We expect evolution to add the nested struct
        - Old records should have profile.details = NULL (or all fields NULL)
        """
        logger = TestLogger()
        logger.section("Integration Test: Append Nested Struct Inside Struct")

        table_name = f"users_double_nested_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            logger.phase("Setup: Create table with simple nested struct")
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    profile STRUCT<
                        name: STRING
                    >
                ) USING iceberg
            """)
            logger.step(f"Created table: {table_id}", always=True)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES
                    (1, named_struct('name', 'Alice')),
                    (2, named_struct('name', 'Bob'))
            """)
            logger.step("Inserted 2 records with simple profile struct", always=True)

            logger.phase("Action: Create evolved schema with doubly-nested struct")

            json_data: List[Dict[str, Any]] = [
                {
                    "id": 3,
                    "profile": {
                        "name": "Charlie",
                        "details": {
                            "age": 30,
                            "country": "NL"
                        }
                    },
                }
            ]
            df = spark_session.createDataFrame(json_data)
            logger.step("Created DataFrame with nested struct inside profile", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect nested struct addition"
            assert len(comparison.added_columns) >= 1, "Should detect at least 1 new nested field/struct"
            logger.step(
                f"Schema comparison detected {len(comparison.added_columns)} new nested fields",
                always=True,
            )

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns",
            )
            logger.step("Applied schema evolution for doubly-nested struct", always=True)

            logger.phase("Verify: Doubly-nested schema updated correctly")

            updated_table = spark_session.table(table_id)

            # Verify full table content including nested NULLs
            verify_table_content(
                df_or_table=updated_table,
                expected_data=[
                    {
                        "id": 1,
                        "profile": {"name": "Alice", "details": None},
                    },
                    {
                        "id": 2,
                        "profile": {"name": "Bob", "details": None},
                    },
                    {
                        "id": 3,
                        "profile": {
                            "name": "Charlie",
                            "details": {"age": 30, "country": "NL"}
                        },
                    },
                ],
                spark_session=spark_session,
                logger=logger,
            )
            logger.success(f"Doubly-nested struct verifications passed for {table_id}", always=True)

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            logger.step(f"Cleaned up table: {table_id}", always=True)

    def test_append_field_in_array_of_structs(self, spark_session):
        """
        Test 4: Append Field Inside Array of Structs

        Scenario:
        - Base table has `events` as ARRAY<STRUCT<type: STRING, ts: TIMESTAMP>>
        - New data introduces `events.element.metadata` as a new field in the struct
        - We expect evolution to add metadata field to the array's struct type
        - Old records should have metadata = NULL in each array element
        """
        logger = TestLogger()
        logger.section("Integration Test: Append Field in Array of Structs")

        table_name = f"users_array_struct_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            logger.phase("Setup: Create table with array of structs")
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    events ARRAY<STRUCT<
                        type: STRING,
                        ts: STRING
                    >>
                ) USING iceberg
            """)
            logger.step(f"Created table with array of structs: {table_id}", always=True)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES
                    (1, array(named_struct('type', 'click', 'ts', '2024-01-01T10:00:00'))),
                    (2, array(
                        named_struct('type', 'view', 'ts', '2024-01-01T10:01:00'),
                        named_struct('type', 'click', 'ts', '2024-01-01T10:02:00')
                    ))
            """)
            logger.step("Inserted 2 records with event arrays", always=True)

            logger.phase("Action: Create evolved schema with extra field in array struct")

            json_data: List[Dict[str, Any]] = [
                {
                    "id": 3,
                    "events": [
                        {
                            "type": "click",
                            "ts": "2024-01-01T10:03:00",
                            "metadata": {"source": "web"}
                        }
                    ],
                }
            ]
            df = spark_session.createDataFrame(json_data)
            logger.step("Created DataFrame with evolved array struct (events.element.metadata)", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect array struct field addition"
            assert len(comparison.added_columns) >= 1, "Should detect at least 1 new field in array struct"
            logger.step(
                f"Schema comparison detected {len(comparison.added_columns)} new fields in array struct",
                always=True,
            )

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns",
            )
            logger.step("Applied schema evolution for array of structs", always=True)

            logger.phase("Verify: Array struct schema updated correctly")

            updated_table = spark_session.table(table_id)

            # Verify full table content - old arrays should have metadata=NULL in elements
            verify_table_content(
                df_or_table=updated_table,
                expected_data=[
                    {
                        "id": 1,
                        "events": [{"type": "click", "ts": "2024-01-01T10:00:00", "metadata": None}],
                    },
                    {
                        "id": 2,
                        "events": [
                            {"type": "view", "ts": "2024-01-01T10:01:00", "metadata": None},
                            {"type": "click", "ts": "2024-01-01T10:02:00", "metadata": None},
                        ],
                    },
                    {
                        "id": 3,
                        "events": [
                            {"type": "click", "ts": "2024-01-01T10:03:00", "metadata": {"source": "web"}},
                        ],
                    },
                ],
                spark_session=spark_session,
                logger=logger,
            )
            logger.success(f"Array of structs verifications passed for {table_id}", always=True)

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            logger.step(f"Cleaned up table: {table_id}", always=True)

    def test_nested_field_order_changes_no_evolution(self, spark_session):
        """
        Test 5: Nested Field Order Changes - Should NOT Trigger Evolution

        Scenario:
        - Base table has `profile` struct with (name, age, email)
        - New data has the same fields but in different order (email, name, age)
        - We expect NO schema changes detected (order shouldn't matter)
        - Data should be written successfully without evolution
        """
        logger = TestLogger()
        logger.section("Integration Test: Nested Field Order Changes - No Evolution")

        table_name = f"users_field_order_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            logger.phase("Setup: Create table with ordered nested fields")
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    profile STRUCT<
                        name: STRING,
                        age: INT,
                        email: STRING
                    >
                ) USING iceberg
            """)
            logger.step(f"Created table: {table_id}", always=True)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES (1, named_struct('name', 'Alice', 'age', 30, 'email', 'alice@example.com'))
            """)
            logger.step("Inserted 1 record with ordered profile fields", always=True)

            logger.phase("Action: Create DataFrame with same fields in different order")

            # Note: JSON naturally creates fields in the order specified
            # Spark should normalize this to match the table schema
            json_data: List[Dict[str, Any]] = [
                {
                    "id": 2,
                    "profile": {
                        "email": "bob@example.com",  # Different order
                        "name": "Bob",
                        "age": 40
                    },
                }
            ]
            df = spark_session.createDataFrame(json_data)
            logger.step("Created DataFrame with reordered nested fields", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert not comparison.has_changes, "Field order changes should NOT trigger schema evolution"
            assert len(comparison.added_columns) == 0, "Should detect NO new columns"
            assert len(comparison.removed_columns) == 0, "Should detect NO removed columns"
            logger.step("Confirmed: Field order difference detected as NO schema change", always=True)

            logger.phase("Verify: Data can be written without evolution")

            # Write directly without evolution
            df.writeTo(table_id).append()
            logger.step("Successfully wrote reordered data without evolution", always=True)

            updated_table = spark_session.table(table_id)

            # Verify both records are present with correct values
            verify_table_content(
                df_or_table=updated_table,
                expected_data=[
                    {
                        "id": 1,
                        "profile": {"name": "Alice", "age": 30, "email": "alice@example.com"},
                    },
                    {
                        "id": 2,
                        "profile": {"name": "Bob", "age": 40, "email": "bob@example.com"},
                    },
                ],
                spark_session=spark_session,
                logger=logger,
            )
            logger.success(f"Field order independence verified for {table_id}", always=True)

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            logger.step(f"Cleaned up table: {table_id}", always=True)
