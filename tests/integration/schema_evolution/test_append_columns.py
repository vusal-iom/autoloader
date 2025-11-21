"""
Integration tests for appending new columns and nested fields.
"""
from typing import Any, Dict, List

import pytest
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from chispa.dataframe_comparer import assert_df_equality

from app.services.schema_evolution_service import SchemaEvolutionService
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestSchemaEvolutionAppendColumns:
    """Integration tests for append_new_columns strategy."""

    def test_append_new_columns_happy_path(self, spark_session, temporary_table):
        """
        Append New Columns Strategy - Happy Path

        Validates the most common evolution scenario with real DDL:
        - Create table with base schema
        - Insert initial data
        - Evolve schema with new columns
        - Verify DDL works and data integrity is maintained
        """
        logger = TestLogger()
        logger.section("Integration Test: Append New Columns Happy Path")

        table_id = temporary_table(prefix="users", logger=logger)

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

        # Explicitly define schema to avoid Spark inferring wrong schema
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_at", StringType(), True)
        ])

        df = spark_session.createDataFrame(json_data, schema=new_schema)
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

        # Insert the new data and verify, matching the flow used in other tests
        df.writeTo(table_id).append()
        logger.step("Inserted new record with evolved schema", always=True)


        logger.phase("Verify: Schema and data updated correctly")
        df_actual = spark_session.table(table_id).orderBy("id")
        expected_data: List[Dict[str, Any]] = [
            {"id": 1, "name": "Alice", "email": None, "created_at": None},
            {"id": 2, "name": "Bob", "email": None, "created_at": None},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2024-01-15T10:30:00"},
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success(f"All verifications passed for {table_id}", always=True)

    def test_append_new_nested_field_in_struct(self, spark_session, temporary_table):
        """
        Append New Nested Field in Struct - Happy Path

        Scenario:
        - Base table has a nested `profile` struct with (name, age)
        - New data introduces `profile.email`
        - We expect the evolution to add `email` inside `profile`
        - Old records should have profile.email = NULL
        """
        logger = TestLogger()
        logger.section("Integration Test: Append New Nested Field in Struct")

        table_id = temporary_table(prefix="users_nested", logger=logger)

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
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("profile", StructType([
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("email", StringType(), True)
            ]), True)
        ])

        df = spark_session.createDataFrame(json_data, schema=new_schema)
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

        df_actual = spark_session.table(table_id).orderBy("id")
        expected_data: List[Dict[str, Any]] = [
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
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success(f"All nested verifications passed for {table_id}", always=True)

    def test_append_nested_struct_inside_struct(self, spark_session, temporary_table):
        """
        Append Nested Struct Inside Existing Struct

        Scenario:
        - Base table has `profile` struct with only `name`
        - New data introduces `profile.details` as a nested struct with (age, country)
        - We expect evolution to add the nested struct
        - Old records should have profile.details = NULL (or all fields NULL)
        """
        logger = TestLogger()
        logger.section("Integration Test: Append Nested Struct Inside Struct")

        table_id = temporary_table(prefix="users_double_nested", logger=logger)

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
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("profile", StructType([
                StructField("name", StringType(), True),
                StructField("details", StructType([
                    StructField("age", IntegerType(), True),
                    StructField("country", StringType(), True)
                ]), True)
            ]), True)
        ])

        df = spark_session.createDataFrame(json_data, schema=new_schema)
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

        df_actual = spark_session.table(table_id).orderBy("id")
        expected_data: List[Dict[str, Any]] = [
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
                    "details": {"age": 30, "country": "NL"},
                },
            },
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success(f"Doubly-nested struct verifications passed for {table_id}", always=True)

    def test_append_field_in_array_of_structs(self, spark_session, temporary_table):
        """
        Append Field Inside Array of Structs

        Scenario:
        - Base table has `events` as ARRAY<STRUCT<type: STRING, ts: TIMESTAMP>>
        - New data introduces `events.element.metadata` as a new field in the struct
        - We expect evolution to add metadata field to the array's struct type
        - Old records should have metadata = NULL in each array element
        """
        logger = TestLogger()
        logger.section("Integration Test: Append Field in Array of Structs")

        table_id = temporary_table(prefix="users_array_struct", logger=logger)

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
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("events", ArrayType(
                StructType([
                    StructField("type", StringType(), True),
                    StructField("ts", StringType(), True),
                    StructField("metadata", MapType(StringType(), StringType(), True), True)
                ]), True
            ), True)
        ])

        df = spark_session.createDataFrame(json_data, schema=new_schema)
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

        df_actual = spark_session.table(table_id).orderBy("id")
        expected_data: List[Dict[str, Any]] = [
            {
                "id": 1,
                "events": [
                    {"type": "click", "ts": "2024-01-01T10:00:00", "metadata": None},
                ],
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
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success(f"Array of structs verifications passed for {table_id}", always=True)
