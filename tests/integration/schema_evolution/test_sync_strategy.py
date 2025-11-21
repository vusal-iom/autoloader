"""
Integration tests for sync strategy and complex scenarios.
"""
import pytest
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from chispa.dataframe_comparer import assert_df_equality

from app.services.schema_evolution_service import SchemaEvolutionService
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestSchemaEvolutionSyncStrategy:
    """Integration tests for sync_all_columns strategy."""

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
