"""Shared test assertion helpers for schema and content verification."""

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from pyspark.sql import SparkSession

from tests.helpers.logger import TestLogger

__all__ = ["verify_table_content", "verify_table_schema"]


def _resolve_dataframe(df_or_table: Any, spark_session: Optional[SparkSession]):
    """Return a DataFrame, Spark session, and name from df_or_table input."""
    if isinstance(df_or_table, str):
        if spark_session is None:
            raise ValueError("spark_session is required when df_or_table is a table identifier")
        df = spark_session.table(df_or_table)
        table_name = df_or_table
        return df, spark_session, table_name

    if hasattr(df_or_table, "schema") and hasattr(df_or_table, "collect"):
        df = df_or_table
        if spark_session is None:
            spark_session = df.sparkSession
        table_name = "DataFrame"
        return df, spark_session, table_name

    raise ValueError(f"df_or_table must be DataFrame or string, got {type(df_or_table)}")


def _schema_fields(schema) -> List[Dict[str, str]]:
    return [{"name": field.name, "type": field.dataType.simpleString()} for field in schema.fields]


def _normalize_schema_fields(
    fields: List[Dict[str, str]],
    ignore_case: bool,
    ignore_field_order: bool,
) -> List[Dict[str, str]]:
    normalized = []
    for field in fields:
        normalized.append({
            "name": field["name"].lower() if ignore_case else field["name"],
            "type": field["type"].lower() if ignore_case else field["type"],
        })

    if ignore_field_order:
        normalized.sort(key=lambda f: f["name"])

    return normalized


def _format_schema_for_logging(fields: List[Dict[str, str]]) -> str:
    if not fields:
        return "(empty schema)"
    return "\n".join(f"- {field['name']}: {field['type']}" for field in fields)


def _coerce_expected_schema(
    expected_schema: Sequence[Union[Dict[str, str], Tuple[str, str]]]
) -> List[Dict[str, str]]:
    coerced: List[Dict[str, str]] = []
    for field in expected_schema:
        if isinstance(field, dict):
            name = field.get("name")
            type_str = field.get("type")
        elif isinstance(field, (list, tuple)) and len(field) == 2:
            name, type_str = field
        else:
            raise ValueError(
                "expected_schema must be a sequence of dicts with 'name'/'type' or (name, type) tuples"
            )

        if name is None or type_str is None:
            raise ValueError("expected_schema entries must include both name and type")

        coerced.append({"name": str(name), "type": str(type_str)})

    return coerced


def _generate_detailed_diff(actual_rows, expected_rows, columns, table_name, note: Optional[str] = None):
    """Generate detailed diff output showing mismatches."""
    actual_count = len(actual_rows)
    expected_count = len(expected_rows)
    table_label = table_name or "DataFrame"

    def row_to_dict(row, cols):
        return {col: row[col] for col in cols}

    actual_dicts = [row_to_dict(row, columns) for row in actual_rows]
    expected_dicts = [row_to_dict(row, columns) for row in expected_rows]

    error_lines = [
        f"\nContent verification failed for {table_name}",
        f"Expected {expected_count} rows, got {actual_count} rows",
        "",
    ]

    missing_rows = []
    for exp_dict in expected_dicts:
        if exp_dict not in actual_dicts:
            missing_rows.append(exp_dict)

    extra_rows = []
    for act_dict in actual_dicts:
        if act_dict not in expected_dicts:
            extra_rows.append(act_dict)

    if missing_rows:
        error_lines.append(f"MISSING ROWS ({len(missing_rows)} rows in expected but not in actual):")
        for i, row in enumerate(missing_rows[:5], 1):
            error_lines.append(f"  {i}. {row}")
        if len(missing_rows) > 5:
            error_lines.append(f"  ... and {len(missing_rows) - 5} more")
        error_lines.append("")

    if extra_rows:
        error_lines.append(f"EXTRA ROWS ({len(extra_rows)} rows in actual but not in expected):")
        for i, row in enumerate(extra_rows[:5], 1):
            error_lines.append(f"  {i}. {row}")
        if len(extra_rows) > 5:
            error_lines.append(f"  ... and {len(extra_rows) - 5} more")
        error_lines.append("")

    if actual_count > 0 and expected_count > 0:
        error_lines.append("FIRST 3 ROWS COMPARISON:")
        max_rows = min(3, actual_count, expected_count)
        for i in range(max_rows):
            exp_dict = expected_dicts[i] if i < len(expected_dicts) else {}
            act_dict = actual_dicts[i] if i < len(actual_dicts) else {}

            error_lines.append(f"  Row {i + 1}:")
            error_lines.append(f"    Expected: {exp_dict}")
            error_lines.append(f"    Actual:   {act_dict}")

            if exp_dict != act_dict:
                diffs = []
                all_keys = set(exp_dict.keys()) | set(act_dict.keys())
                for key in sorted(all_keys):
                    exp_val = exp_dict.get(key, "<missing>")
                    act_val = act_dict.get(key, "<missing>")
                    if exp_val != act_val:
                        diffs.append(f"{key}: {exp_val} != {act_val}")
                if diffs:
                    error_lines.append(f"    Diff:     {', '.join(diffs)}")
            error_lines.append("")

    if expected_count <= 10:
        error_lines.append("EXPECTED DATA (all rows):")
        for i, row in enumerate(expected_dicts, 1):
            error_lines.append(f"  {i}. {row}")
        error_lines.append("")

    if actual_count <= 10:
        error_lines.append("ACTUAL DATA (all rows):")
        for i, row in enumerate(actual_dicts, 1):
            error_lines.append(f"  {i}. {row}")
        error_lines.append("")

    print("\n".join(error_lines))
    suffix = f" ({note})" if note else ""
    raise AssertionError(f"Content verification failed for {table_label}; see printed diff above{suffix}")


def verify_table_content(
    df_or_table: Any,
    expected_data: List[Dict[str, Any]],
    spark_session: Optional[SparkSession] = None,
    columns: Optional[List[str]] = None,
    ignore_column_order: bool = True,
    ignore_row_order: bool = True,
    logger: Optional[TestLogger] = None,
):
    """Verify that a DataFrame or table contains exactly the expected rows."""
    from pyspark.sql.types import StructType

    df, spark_session, table_name = _resolve_dataframe(df_or_table, spark_session)

    if logger:
        logger.step(f"Verifying content of {table_name}")

    table_columns = df.columns

    if columns is None:
        display_columns = table_columns
    else:
        missing_cols = set(columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
        display_columns = columns

    compare_columns = sorted(display_columns) if ignore_column_order else display_columns

    if expected_data:
        expected_columns_set = set(compare_columns)
        for idx, row in enumerate(expected_data, 1):
            if isinstance(row, dict):
                row_dict = row
            elif hasattr(row, "asDict"):
                row_dict = row.asDict(recursive=True)
            else:
                raise ValueError("expected_data entries must be dict-like or Row instances")

            missing_in_row = expected_columns_set - set(row_dict.keys())
            extra_in_row = set(row_dict.keys()) - expected_columns_set
            if missing_in_row or extra_in_row:
                table_label = table_name or "DataFrame"
                error_lines = [
                    "\nverify_table_content expected_data column mismatch for "
                    f"{table_label} (row {idx}):",
                    f"- table columns: {list(table_columns)}",
                    f"- expected columns: {list(display_columns)}",
                    f"- columns used for compare: {list(compare_columns)}",
                    f"- row columns: {list(row_dict.keys())}",
                ]
                if missing_in_row:
                    error_lines.append(f"- missing columns: {sorted(missing_in_row)}")
                if extra_in_row:
                    error_lines.append(f"- extra columns: {sorted(extra_in_row)}")
                error_lines.append(f"- offending row data: {row_dict}")
                error_lines.append("- hint: include all table columns in expected_data, or pass columns=[...] to verify a subset")
                print("\n".join(error_lines))
                raise AssertionError(
                    f"verify_table_content expected_data columns do not align for {table_label} (row {idx}); see printed diff above"
                )

    df_to_compare = df.select(*compare_columns)

    missing_note: Optional[str] = None

    if not expected_data:
        expected_df = spark_session.createDataFrame([], df_to_compare.schema)
    else:
        from pyspark.sql.types import StructType, ArrayType

        missing_marker = "<missing_in_expected_data>"
        missing_inserted = False

        def _fill_missing_struct_fields(value, data_type):
            if value is None or data_type is None:
                return value

            if isinstance(data_type, StructType):
                if not isinstance(value, dict):
                    return value
                filled = dict(value)
                for field in data_type.fields:
                    if field.name not in filled:
                        filled[field.name] = missing_marker
                        nonlocal_missing_inserted[0] = True
                    else:
                        filled[field.name] = _fill_missing_struct_fields(filled[field.name], field.dataType)
                return filled

            if isinstance(data_type, ArrayType) and isinstance(data_type.elementType, StructType):
                if not isinstance(value, list):
                    return value
                return [_fill_missing_struct_fields(elem, data_type.elementType) for elem in value]

            return value

        schema_by_name = {field.name: field.dataType for field in df_to_compare.schema.fields}
        filled_expected = []
        nonlocal_missing_inserted = [False]
        for row in expected_data:
            row_dict = row if isinstance(row, dict) else row.asDict(recursive=True)
            filled_row = {}
            for col in compare_columns:
                filled_row[col] = _fill_missing_struct_fields(row_dict.get(col), schema_by_name.get(col))
            filled_expected.append(filled_row)

        if nonlocal_missing_inserted[0]:
            missing_note = f"'{missing_marker}' denotes fields not provided in expected_data"

        expected_df = spark_session.createDataFrame(filled_expected, schema=df_to_compare.schema)
        if ignore_column_order:
            expected_df = expected_df.select(*sorted(expected_df.columns))

    if ignore_row_order and compare_columns:
        df_to_compare = df_to_compare.sort(*compare_columns)
        expected_df = expected_df.sort(*compare_columns)

    actual_rows = df_to_compare.collect()
    expected_rows = expected_df.collect()

    actual_count = len(actual_rows)
    expected_count = len(expected_rows)

    if logger:
        logger.metric("Expected Rows", expected_count)
        logger.metric("Actual Rows", actual_count)

    if actual_count != expected_count or actual_rows != expected_rows:
        _generate_detailed_diff(actual_rows, expected_rows, compare_columns, table_name, note=missing_note)

    if logger:
        logger.success(f"Content verification passed: {actual_count} rows match exactly")

    return df


def verify_table_schema(
    df_or_table: Any,
    expected_schema: Sequence[Union[Dict[str, str], Tuple[str, str]]],
    spark_session: Optional[SparkSession] = None,
    ignore_field_order: bool = True,
    ignore_case: bool = False,
    logger: Optional[TestLogger] = None,
):
    """Verify that a DataFrame or table schema matches the expected definition."""
    df, _, table_name = _resolve_dataframe(df_or_table, spark_session)

    actual_fields = _schema_fields(df.schema)
    expected_fields = _coerce_expected_schema(expected_schema)

    normalized_actual = _normalize_schema_fields(actual_fields, ignore_case, ignore_field_order)
    normalized_expected = _normalize_schema_fields(expected_fields, ignore_case, ignore_field_order)

    if logger:
        logger.step(f"Verifying schema of {table_name}")
        logger.metric("Actual Schema", _format_schema_for_logging(actual_fields))
        logger.metric("Expected Schema", _format_schema_for_logging(expected_fields))

    if normalized_actual != normalized_expected:
        error_lines = [
            f"\nSchema verification failed for {table_name}",
            "Expected Schema:",
            _format_schema_for_logging(expected_fields),
            "",
            "Actual Schema:",
            _format_schema_for_logging(actual_fields),
        ]
        print("\n".join(error_lines))
        raise AssertionError(f"Schema verification failed for {table_name}; see printed diff above")

    if logger:
        logger.success("Schema verification passed", always=True)

    return df.schema
