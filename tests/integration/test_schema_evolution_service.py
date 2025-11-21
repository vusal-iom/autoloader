"""
Integration tests for SchemaEvolutionService.apply_schema_evolution with real Iceberg tables.

Tests validate that DDL operations actually work with real Spark and Iceberg.
"""
from typing import Any, Dict, List

import pytest
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType, FloatType,
)

from chispa.dataframe_comparer import *

from app.services.schema_evolution_service import (
    IncompatibleTypeChangeError,
    SchemaEvolutionService,
)
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestSchemaEvolutionApply:
    """Integration tests for apply_schema_evolution with real Iceberg tables."""

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

    def test_struct_to_map_type_change(self, spark_session, temporary_table):
        """
        Struct to Map Type Change - Edge Case

        Scenario:
        - Base table has `profile` as a STRUCT<name: STRING, age: INT>
        - New data has `profile` as a MAP<STRING, STRING>
        - This is a structural type change that should be detected as incompatible
        - Tests that the comparison correctly identifies this as a TYPE_CHANGE
        """
        logger = TestLogger()
        logger.section("Integration Test: Struct to Map Type Change")

        table_id = temporary_table(prefix="users_struct_map", logger=logger)

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

        # Applying evolution should raise an incompatibility error
        with pytest.raises(IncompatibleTypeChangeError) as exc_info:
            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="sync_all_columns",
            )

        assert exc_info.value.message == """Cannot change column 'profile' from struct<name:string,age:int> to map<string,string>"""
        logger.step("apply_schema_evolution correctly raised IncompatibleTypeChangeError", always=True)

    def test_nested_field_order_changes_no_evolution(self, spark_session, temporary_table):
        """
        Nested Field Order Changes - Should NOT Trigger Evolution

        Scenario:
        - Base table has `profile` struct with (name, age, email)
        - New data has the same fields but in different order (email, name, age)
        - We expect NO schema changes detected (order shouldn't matter)
        - Data should be written successfully without evolution
        """
        logger = TestLogger()
        logger.section("Integration Test: Nested Field Order Changes - No Evolution")

        table_id = temporary_table(prefix="users_field_order", logger=logger)

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
        incoming_schema = StructType([
            StructField("id", LongType(), True),
            StructField("profile", StructType([
                StructField("email", StringType(), True),  # Different order
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True)
        ])

        df = spark_session.createDataFrame(json_data, schema=incoming_schema)
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

        df_actual = spark_session.table(table_id).orderBy("id")
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("profile", StructType([
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("email", StringType(), True),
            ]), True)
        ])
        expected_data: List[Dict[str, Any]] = [
            {
                "id": 1,
                "profile": {"name": "Alice", "age": 30, "email": "alice@example.com"},
            },
            {
                "id": 2,
                "profile": {"name": "Bob", "age": 40, "email": "bob@example.com"},
            },
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)
        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success(f"Field order independence verified for {table_id}", always=True)

    def test_type_promotions(self, spark_session, temporary_table):
        """
        Type Promotions (Int -> Long, Float -> Double)

        Scenario:
        - Base table has `count` (INT) and `score` (FLOAT)
        - New data has `count` (LONG) and `score` (DOUBLE)
        - We expect evolution to promote both columns
        """
        logger = TestLogger()
        logger.section("Integration Test: Type Promotions")

        table_id = temporary_table(prefix="users_promotions", logger=logger)

        logger.phase("Setup: Create table with INT and FLOAT columns")
        spark_session.sql(f"CREATE TABLE {table_id} (id BIGINT, count INT, score FLOAT) USING iceberg")
        spark_session.sql(f"INSERT INTO {table_id} VALUES (1, 100, 1.5)")
        logger.step("Created table with INT and FLOAT columns", always=True)

        logger.phase("Action: Create DataFrame with LONG and DOUBLE columns")
        # Use values that require LongType and DoubleType
        json_data = [{"id": 2, "count": 2147483648, "score": 2.123456789012345}]
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("count", LongType(), True),
            StructField("score", DoubleType(), True)  # Spark defaults python float to Double
        ])
        df = spark_session.createDataFrame(json_data, schema=new_schema)

        target_schema = spark_session.table(table_id).schema
        comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.modified_columns) == 2

        # Verify specific changes
        changes = {(col, old, new) for col, old, new in comparison.modified_columns}
        # Spark simpleString() returns 'int' for IntegerType and 'bigint' for LongType
        assert ("count", "int", "bigint") in changes
        assert ("score", "float", "double") in changes
        logger.step("Detected type changes: int->long and float->double", always=True)

        SchemaEvolutionService.apply_schema_evolution(
            spark=spark_session,
            table_identifier=table_id,
            comparison=comparison,
            strategy="sync_all_columns"
        )
        logger.step("Applied schema evolution (sync_all_columns)", always=True)

        df.writeTo(table_id).append()
        logger.step("Inserted new record", always=True)

        logger.phase("Verify: Column types and data updated correctly")
        df_actual = spark_session.table(table_id).orderBy("id")

        expected_data = [
            {"id": 1, "count": 100, "score": 1.5},
            {"id": 2, "count": 2147483648, "score": 2.123456789012345}
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)

        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success("Type promotions and data verification passed", always=True)

    def test_sync_strategy_remove_column(self, spark_session, temporary_table):
        """
        Sync Strategy - Remove Column
        """
        logger = TestLogger()
        logger.section("Integration Test: Sync Strategy - Remove Column")

        table_id = temporary_table(prefix="users_remove", logger=logger)

        spark_session.sql(f"CREATE TABLE {table_id} (id BIGINT, extra_col STRING) USING iceberg")
        spark_session.sql(f"INSERT INTO {table_id} VALUES (1, 'remove_me')")

        # New data missing 'extra_col'
        json_data = [{"id": 2}]
        new_schema = StructType([StructField("id", LongType(), True)])
        df = spark_session.createDataFrame(json_data, schema=new_schema)

        target_schema = spark_session.table(table_id).schema
        comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.removed_columns) == 1
        assert comparison.removed_columns[0].name == "extra_col"

        SchemaEvolutionService.apply_schema_evolution(
            spark=spark_session,
            table_identifier=table_id,
            comparison=comparison,
            strategy="sync_all_columns"
        )

        df.writeTo(table_id).append()

        df_actual = spark_session.table(table_id).orderBy("id")
        expected_data = [
            {"id": 1},
            {"id": 2}
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)

        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success("Column removed and data verified successfully", always=True)

    def test_sync_strategy_mixed_changes(self, spark_session, temporary_table):
        """
        Sync Strategy - Mixed Changes (Add, Remove, Type Change)
        """
        logger = TestLogger()
        logger.section("Integration Test: Sync Strategy - Mixed Changes")

        table_id = temporary_table(prefix="users_mixed", logger=logger)

        # Base: id (long), old_col (string), count (int)
        spark_session.sql(f"""
            CREATE TABLE {table_id} (
                id BIGINT,
                old_col STRING,
                count INT
            ) USING iceberg
        """)
        spark_session.sql(f"INSERT INTO {table_id} VALUES (1, 'keep', 100)")

        # New: id (long), count (long) [promoted], new_col (string) [added], old_col [removed]
        json_data = [{"id": 2, "count": 2147483648, "new_col": "fresh"}]
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("count", LongType(), True),
            StructField("new_col", StringType(), True)
        ])
        df = spark_session.createDataFrame(json_data, schema=new_schema)

        target_schema = spark_session.table(table_id).schema
        comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)

        assert comparison.has_changes
        assert len(comparison.added_columns) == 1
        assert len(comparison.removed_columns) == 1
        assert len(comparison.modified_columns) == 1

        SchemaEvolutionService.apply_schema_evolution(
            spark=spark_session,
            table_identifier=table_id,
            comparison=comparison,
            strategy="sync_all_columns"
        )

        df.writeTo(table_id).append()

        df_actual = spark_session.table(table_id).orderBy("id")

        expected_data = [
            {"id": 1, "count": 100, "old_col": "keep"},
            {"id": 2, "count": 2147483648, "new_col": "fresh"}
        ]
        df_expected = spark_session.createDataFrame(expected_data, schema=new_schema)

        assert_df_equality(
            df1=df_actual,
            df2=df_expected,
            ignore_column_order=True,
        )
        logger.success("Mixed changes applied successfully", always=True)

    def test_incompatible_type_change_long_to_int(self, spark_session, temporary_table):
        """
        Incompatible Type Change - Long to Int
        """
        logger = TestLogger()
        logger.section("Integration Test: Incompatible Change - Long to Int")

        table_id = temporary_table(prefix="users_bad_change", logger=logger)

        spark_session.sql(f"CREATE TABLE {table_id} (id BIGINT, count BIGINT) USING iceberg")

        # New data tries to change count to INT
        json_data = [{"id": 1, "count": 100}]
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("count", IntegerType(), True)
        ])
        df = spark_session.createDataFrame(json_data, schema=new_schema)

        target_schema = spark_session.table(table_id).schema
        comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)

        assert comparison.has_changes

        with pytest.raises(IncompatibleTypeChangeError):
            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="sync_all_columns"
            )
        logger.success("Incompatible change correctly rejected", always=True)