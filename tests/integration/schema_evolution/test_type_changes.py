"""
Integration tests for type changes and promotions.
"""
from typing import Any, Dict, List

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from chispa.dataframe_comparer import assert_df_equality

from app.services.schema_evolution_service import (
    IncompatibleTypeChangeError,
    SchemaEvolutionService,
)
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestSchemaEvolutionTypeChanges:
    """Integration tests for type promotions and changes."""

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
