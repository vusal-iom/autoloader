# Test Suite Guidelines

Central guidance for all tests in this repo. Contains common rules and notes specific to unit, integration, and e2e suites.

## Common (all tests)
- **Logging tone:** No emojis, no decorative separators. Use `TestLogger` (`section/phase/step/success`) for clarity.
- **Data verification:** Prefer `chispa.assert_df_equality` with an explicit `StructType` to validate both data and schema. Avoid separate schema assertions. Use `ignore_row_order=True` and `ignore_column_order=True` when ordering is irrelevant.
- **Building expected DataFrames:** Define schemas up-front to prevent Spark inference errors (e.g., maps vs structs). Fill expected NULLs explicitly for newly added fields before comparing.
- **Helpers:** Reuse shared helpers instead of re-implementing. Keep messages concise and professional.
- **Running tests:** `pytest tests/unit -q`, `pytest tests/integration -q`, `pytest tests/e2e -q` (ensure required services are up per suite below).

Example data check:
```python
df_expected = spark.createDataFrame(expected_rows, schema=expected_schema)
assert_df_equality(df_actual, df_expected, ignore_column_order=True, ignore_row_order=True)
```

## Unit Tests
- Target pure logic; mock external systems where appropriate.
- Fast and deterministic; no network or Docker dependencies.
- Preferred command: `pytest tests/unit -q`.

## Integration Tests
- Purpose: Verify components with **real Spark + Iceberg + MinIO**, but **without Prefect**. Database is PG.
- Start services: `docker-compose -f docker-compose.test.yml up -d` (wait for Spark Connect at `sc://localhost:15002` and MinIO at `http://localhost:9000`).
- Catalog: `test_catalog.test_db`. Use real Spark Connect, never local/mocked sessions.
- Data verification: Use `assert_df_equality` with explicit schemas for schema-evolution scenarios (covers schema + data). Populate expected NULLs for legacy rows and use `ignore_row_order/ignore_column_order` when needed.
- Run: `pytest tests/integration/ -v` or a single file, e.g., `pytest tests/integration/test_schema_evolution_service.py -v`.

## E2E Tests
- Purpose: Full stack, product-like flow with **API-only interactions**. Uses Docker services (Spark, MinIO, Postgres) **and Prefect**; never touch the DB directly.
- Rules: One test at a time, propose structure before large additions, use helpers from `tests.e2e.helpers` (e.g., `generate_unique_table_name`, `create_standard_ingestion`, `wait_for_run_completion`, `verify_table_data`, `assert_run_metrics`).
- Data verification: Validate in Spark after API actions. Use `verify_table_data` for counts/fields/duplicates and `assert_df_equality` for exact dataset comparisons when manageable.
- Quick start: `docker-compose -f docker-compose.test.yml up -d` (wait ~1–2 min), then `pytest tests/e2e/ -v`.
- Critical don'ts: No direct DB queries, no hardcoded table names, no custom polling (use helpers), no mocks.
