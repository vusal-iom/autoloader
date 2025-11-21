"""
Integration tests for 'fail' and 'ignore' strategies.
"""
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import LongType, StringType, StructField, StructType

from app.services.schema_evolution_service import (SchemaEvolutionService,
                                                   SchemaMismatchError)
from tests.helpers.logger import TestLogger


@pytest.mark.integration
class TestSchemaEvolutionOtherStrategies:
    """Integration tests for fail and ignore strategies."""

    def test_fail_strategy_raises_error(self, spark_session, temporary_table):
        """
        Fail Strategy - Should raise SchemaMismatchError on changes
        """
        logger = TestLogger()
        logger.section("Integration Test: Fail Strategy")

        table_id = temporary_table(prefix="users_fail", logger=logger)

        # Base schema
        spark_session.sql(f"CREATE TABLE {table_id} (id BIGINT, name STRING) USING iceberg")
        spark_session.sql(f"INSERT INTO {table_id} VALUES (1, 'Alice')")

        # New schema with added column
        json_data = [{"id": 2, "name": "Bob", "age": 30}]
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("age", LongType(), True)
        ])
        df = spark_session.createDataFrame(json_data, schema=new_schema)

        target_schema = spark_session.table(table_id).schema
        comparison = SchemaEvolutionService.compare_schemas(df.schema, target_schema)

        assert comparison.has_changes

        # Should raise error
        with pytest.raises(SchemaMismatchError) as excinfo:
            SchemaEvolutionService.apply_schema_evolution(
                spark=spark_session,
                table_identifier=table_id,
                comparison=comparison,
                strategy="fail"
            )

        error = excinfo.value
        assert "Schema Mismatch Detected!" in error.message

        logger.success("SchemaMismatchError raised as expected", always=True)

    def test_ignore_strategy_does_nothing(self, spark_session, temporary_table):
        """
        Ignore Strategy - Should not apply any changes
        """
        logger = TestLogger()
        logger.section("Integration Test: Ignore Strategy")

        table_id = temporary_table(prefix="users_ignore", logger=logger)

        # Base schema
        spark_session.sql(f"CREATE TABLE {table_id} (id BIGINT, name STRING) USING iceberg")
        spark_session.sql(f"INSERT INTO {table_id} VALUES (1, 'Alice')")

        # New schema with added column
        json_data = [{"id": 2, "name": "Bob", "age": 30}]
        new_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("age", LongType(), True)
        ])
        new_data_df = spark_session.createDataFrame(json_data, schema=new_schema)

        target_schema = spark_session.table(table_id).schema
        comparison = SchemaEvolutionService.compare_schemas(new_data_df.schema, target_schema)

        assert comparison.has_changes

        # Apply ignore strategy
        SchemaEvolutionService.apply_schema_evolution(
            spark=spark_session,
            table_identifier=table_id,
            comparison=comparison,
            strategy="ignore"
        )

        # assert that table schema and data are unchanged
        assert_df_equality(
            df1=spark_session.table(table_id).orderBy("id"),
            df2=spark_session.createDataFrame(data=[{"id": 1, "name": "Alice"}], schema="id BIGINT, name STRING"),
            ignore_column_order=True,
        )

        # Append data while ignoring extra columns (age) by aligning to target schema
        df_aligned = SchemaEvolutionService.align_dataframe_to_target_schema(new_data_df, target_schema)
        df_aligned.writeTo(table_id).append()

        # assert table schema stays while we have also the second row (appended)
        assert_df_equality(
            df1=spark_session.table(table_id).orderBy("id"),
            df2=spark_session.createDataFrame(
                data=[
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"}
                ],
                schema="id BIGINT, name STRING"
            ),
            ignore_column_order=True,
        )
