"""
Integration tests for SchemaEvolutionService.apply_schema_evolution with real Iceberg tables.

Tests validate that DDL operations actually work with real Spark and Iceberg.
"""
import pytest
import uuid
from typing import Any, Dict, List

from app.services.schema_evolution_service import SchemaEvolutionService
from tests.helpers.assertions import verify_table_content, verify_table_schema
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
            verify_table_schema(
                df_or_table=updated_table,
                expected_schema=[
                    ("id", "bigint"),
                    ("name", "string"),
                    ("email", "string"),
                    ("created_at", "string"),
                ],
                logger=logger,
            )

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
        from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

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

            # Explicitly define schema to avoid Spark inferring profile as MapType
            schema = StructType([
                StructField("id", LongType(), True),
                StructField("profile", StructType([
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("email", StringType(), True)
                ]), True)
            ])

            df = spark_session.createDataFrame(json_data, schema=schema)
            logger.step("Created DataFrame with evolved nested schema (profile.email)", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect schema changes for nested struct"

            nested_fields = [path for path, _ in comparison.nested_field_additions]
            assert nested_fields == ["profile.email"], "Should detect nested field addition"
            assert not comparison.modified_columns, "Nested additions should not be treated as type changes"
            logger.step(
                f"Schema comparison detected nested addition(s): {nested_fields}",
                always=True,
            )

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns",
            )
            logger.step("Applied nested field evolution via SchemaEvolutionService", always=True)

            # Insert the new data
            df.writeTo(table_id).append()
            logger.step("Inserted new record with evolved schema", always=True)

            logger.phase("Verify: Nested schema updated correctly")

            updated_table = spark_session.table(table_id)
            verify_table_schema(
                df_or_table=updated_table,
                expected_schema=[
                    ("id", "bigint"),
                    ("profile", "struct<name:string,age:int,email:string>"),
                ],
                logger=logger,
            )

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
            logger.step(f"Cleaned up table: {table_id}", always=True)

    def test_append_nested_struct_inside_struct(self, spark_session):
        """
        Test 3: Append Nested Struct Inside Existing Struct

        Scenario:
        - Base table has `profile` struct with only `name`
        - New data introduces `profile.details` as a nested struct with (age, country)
        - We expect evolution to add the nested struct
        - Old records should have profile.details = NULL (or all fields NULL)
        """
        from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

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

            # Explicitly define schema to avoid Spark inferring profile as MapType
            schema = StructType([
                StructField("id", LongType(), True),
                StructField("profile", StructType([
                    StructField("name", StringType(), True),
                    StructField("details", StructType([
                        StructField("age", IntegerType(), True),
                        StructField("country", StringType(), True)
                    ]), True)
                ]), True)
            ])

            df = spark_session.createDataFrame(json_data, schema=schema)
            logger.step("Created DataFrame with nested struct inside profile", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect nested struct addition"

            nested_fields = [path for path, _ in comparison.nested_field_additions]
            assert nested_fields == ["profile.details"], "Should capture nested struct addition"
            assert not comparison.modified_columns
            logger.step(
                f"Schema comparison detected nested addition(s): {nested_fields}",
                always=True,
            )

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns",
            )
            logger.step("Applied nested struct evolution via SchemaEvolutionService", always=True)

            # Insert the new data with the evolved schema
            df.writeTo(table_id).append()
            logger.step("Inserted new record with nested struct", always=True)

            logger.phase("Verify: Doubly-nested schema updated correctly")

            updated_table = spark_session.table(table_id)
            verify_table_schema(
                df_or_table=updated_table,
                expected_schema=[
                    ("id", "bigint"),
                    ("profile", "struct<name:string,details:struct<age:int,country:string>>"),
                ],
                logger=logger,
            )

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
        from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, MapType

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

            # Explicitly define schema to avoid Spark inferring metadata as MapType
            schema = StructType([
                StructField("id", LongType(), True),
                StructField("events", ArrayType(
                    StructType([
                        StructField("type", StringType(), True),
                        StructField("ts", StringType(), True),
                        StructField("metadata", MapType(StringType(), StringType(), True), True)
                    ]), True
                ), True)
            ])

            df = spark_session.createDataFrame(json_data, schema=schema)
            logger.step("Created DataFrame with evolved array struct (events.element.metadata)", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect array struct field addition"

            nested_fields = [path for path, _ in comparison.nested_field_additions]
            assert nested_fields == ["events.element.metadata"], "Should detect nested array path"
            assert not comparison.modified_columns
            logger.step(
                f"Schema comparison detected array nested addition(s): {nested_fields}",
                always=True,
            )

            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="append_new_columns",
            )
            logger.step("Applied array struct field evolution via SchemaEvolutionService", always=True)

            # Insert the new data
            df.writeTo(table_id).append()
            logger.step("Inserted new record with evolved array struct", always=True)

            logger.phase("Verify: Array struct schema updated correctly")

            updated_table = spark_session.table(table_id)
            verify_table_schema(
                df_or_table=updated_table,
                expected_schema=[
                    ("id", "bigint"),
                    ("events", "array<struct<type:string,ts:string,metadata:map<string,string>>>")
                ],
                logger=logger,
            )

            # Verify full table content - old arrays should have metadata=NULL in elements
            # NOTE: Cannot use verify_table_content here because it tries to sort by arrays
            # which Spark doesn't support. Manually verify instead.
            result = updated_table.orderBy("id").collect()
            assert len(result) == 3, f"Expected 3 rows, got {len(result)}"

            # Verify row 1
            assert result[0]["id"] == 1
            assert len(result[0]["events"]) == 1
            assert result[0]["events"][0]["type"] == "click"
            assert result[0]["events"][0]["ts"] == "2024-01-01T10:00:00"
            assert result[0]["events"][0]["metadata"] is None

            # Verify row 2
            assert result[1]["id"] == 2
            assert len(result[1]["events"]) == 2
            assert result[1]["events"][0]["type"] == "view"
            assert result[1]["events"][0]["metadata"] is None
            assert result[1]["events"][1]["type"] == "click"
            assert result[1]["events"][1]["metadata"] is None

            # Verify row 3
            assert result[2]["id"] == 3
            assert len(result[2]["events"]) == 1
            assert result[2]["events"][0]["type"] == "click"
            assert result[2]["events"][0]["metadata"] == {"source": "web"}

            logger.step("Verified all 3 rows with correct array struct values", always=True)
            logger.success(f"Array of structs verifications passed for {table_id}", always=True)

        finally:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_id}")
            logger.step(f"Cleaned up table: {table_id}", always=True)

    def test_struct_to_map_type_change(self, spark_session):
        """
        Test: Struct to Map Type Change - Edge Case

        Scenario:
        - Base table has `profile` as a STRUCT<name: STRING, age: INT>
        - New data has `profile` as a MAP<STRING, STRING>
        - This is a structural type change that should be detected as incompatible
        - Tests that the comparison correctly identifies this as a TYPE_CHANGE
        """
        from pyspark.sql.types import StructType, StructField, LongType, StringType, MapType

        logger = TestLogger()
        logger.section("Integration Test: Struct to Map Type Change")

        table_name = f"users_struct_map_{uuid.uuid4().hex[:8]}"
        table_id = f"test_catalog.test_db.{table_name}"

        try:
            logger.phase("Setup: Create table with struct field")
            spark_session.sql(f"""
                CREATE TABLE {table_id} (
                    id BIGINT,
                    profile STRUCT<
                        name: STRING,
                        age: INT
                    >
                ) USING iceberg
            """)
            logger.step(f"Created table: {table_id}", always=True)

            spark_session.sql(f"""
                INSERT INTO {table_id}
                VALUES (1, named_struct('name', 'Alice', 'age', 30))
            """)
            logger.step("Inserted 1 record with profile struct", always=True)

            logger.phase("Action: Create DataFrame with map type for profile field")

            json_data: List[Dict[str, Any]] = [
                {
                    "id": 2,
                    "profile": {
                        "city": "Amsterdam",
                        "country": "NL"
                    },
                }
            ]

            # Explicitly define schema with profile as MapType
            schema = StructType([
                StructField("id", LongType(), True),
                StructField("profile", MapType(StringType(), StringType(), True), True)
            ])

            df = spark_session.createDataFrame(json_data, schema=schema)
            logger.step("Created DataFrame with profile as map<string,string>", always=True)

            target_schema = spark_session.table(table_id).schema

            logger.phase("Verify: Type change detection")

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes, "Should detect type change"
            assert len(comparison.modified_columns) == 1, "Should detect 1 modified column"

            col_name, old_type, new_type = comparison.modified_columns[0]
            assert col_name == "profile"
            assert old_type == "struct<name:string,age:int>"
            assert new_type == "map<string,string>"

            logger.step(
                f"Correctly detected structural type change: {col_name} from {old_type} to {new_type}",
                always=True,
            )

            # Verify this is recognized as incompatible
            is_compatible = SchemaEvolutionService.is_type_change_compatible(old_type, new_type)
            assert not is_compatible, "Struct to Map should be incompatible"
            logger.step("Correctly identified as incompatible type change", always=True)

            logger.success(f"Struct to Map edge case validated for {table_id}", always=True)

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
        from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

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

            # Explicitly define schema with fields in DIFFERENT order from table
            # but same fields to test that field order doesn't matter
            schema = StructType([
                StructField("id", LongType(), True),
                StructField("profile", StructType([
                    StructField("email", StringType(), True),  # Different order
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True)
                ]), True)
            ])

            df = spark_session.createDataFrame(json_data, schema=schema)
            logger.step("Created DataFrame with reordered nested fields", always=True)

            target_schema = spark_session.table(table_id).schema

            comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)
            assert comparison.has_changes is False, "Field order-only differences should be ignored"
            assert comparison.modified_columns == []
            assert comparison.nested_field_additions == []
            logger.step("Field order correctly treated as no-op", always=True)

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
