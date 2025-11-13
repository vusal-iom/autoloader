# Schema Evolution Module - Unit Test Strategy

## Executive Summary

The schema evolution module is critical for maintaining data integrity during automatic ingestion from cloud storage. This strategy document outlines comprehensive unit tests for the `SchemaEvolutionService`, related repositories, and integration points to ensure reliable schema change detection, validation, and evolution handling.

## Test Coverage Goals

- **Target Coverage**: 95%+ line coverage, 100% branch coverage for critical paths
- **Priority**: Schema evolution is P0 - any failure can corrupt data or break pipelines
- **Risk Areas**: Type compatibility validation, DDL generation, error handling
- **Test Types**: Unit tests (isolated), integration tests (with Spark), E2E tests (full pipeline)

## Module Components to Test

1. **SchemaEvolutionService** (`app/services/schema_evolution_service.py`)
2. **SchemaVersionRepository** (`app/repositories/schema_version_repository.py`)
3. **Domain Models** (SchemaVersion, SchemaChange, SchemaComparison)
4. **Integration Points** (BatchFileProcessor schema evolution flow)

## Test Case Prioritization

### P0 - Critical (Must have before production)

#### 1. Schema Comparison Tests

**Test: `test_compare_schemas_identical`**
- **Why**: Validates baseline - no changes should trigger no actions
- **What**: Ensures identical schemas return no changes
- **Business Value**: Prevents unnecessary schema operations that could impact performance
- **Edge Cases**: Case sensitivity, nullable flags, metadata differences

**Test: `test_compare_schemas_new_columns`**
- **Why**: Most common evolution scenario in data lakes
- **What**: Detects columns present in source but not target
- **Business Value**: Enables automatic schema expansion as data sources evolve
- **Edge Cases**: Complex types (arrays, structs), special characters in names

**Test: `test_compare_schemas_removed_columns`**
- **Why**: Detects data loss scenarios that need careful handling
- **What**: Identifies columns missing from new data
- **Business Value**: Prevents silent data drops, enables backward compatibility
- **Edge Cases**: Nested fields, columns used in partitioning

**Test: `test_compare_schemas_type_changes`**
- **Why**: Type mismatches are the #1 cause of ingestion failures
- **What**: Detects when column types differ between schemas
- **Business Value**: Prevents runtime casting errors and data corruption
- **Edge Cases**: Nullable changes, precision changes, complex type variations

#### 2. Type Compatibility Tests

**Test: `test_is_type_change_compatible_safe_widening`**
- **Why**: Certain type changes are safe (int→long, float→double)
- **What**: Validates safe type widening conversions
- **Business Value**: Allows automatic evolution for compatible changes
- **Test Cases**:
  - int → long/bigint ✓
  - float → double ✓
  - date → timestamp ✓
  - string ↔ varchar ✓

**Test: `test_is_type_change_compatible_unsafe_narrowing`**
- **Why**: Narrowing conversions cause data loss
- **What**: Rejects unsafe type changes
- **Business Value**: Prevents data truncation and loss
- **Test Cases**:
  - long → int ✗
  - double → float ✗
  - timestamp → date ✗ (loses time component)
  - string → int ✗

**Test: `test_is_type_change_compatible_special_cases`**
- **Why**: Some conversions have special rules
- **What**: Handles edge cases in type compatibility
- **Business Value**: Ensures data integrity for complex scenarios
- **Test Cases**:
  - decimal precision changes
  - array element type changes
  - struct field evolution
  - map key/value type changes

#### 3. Evolution Strategy Tests

**Test: `test_strategy_fail_raises_exception`**
- **Why**: "Fail" strategy must halt processing to prevent corruption
- **What**: Verifies SchemaMismatchError with detailed change information
- **Business Value**: Gives users control over schema changes in strict environments
- **Validation**:
  - Error message includes all changes
  - Suggested remediation steps
  - No partial application

**Test: `test_strategy_ignore_no_changes`**
- **Why**: "Ignore" allows reading subset of columns
- **What**: Ensures no DDL executed, DataFrame columns adjusted
- **Business Value**: Enables reading new files with extra columns without table changes
- **Edge Cases**: Column order differences, case sensitivity

**Test: `test_strategy_append_new_columns`**
- **Why**: Most common evolution strategy - additive only
- **What**: Adds new columns, preserves existing and removed columns
- **Business Value**: Safe evolution that never loses data
- **DDL Validation**:
  - ALTER TABLE ADD COLUMN statements
  - Column order preservation
  - Default values for new columns

**Test: `test_strategy_sync_all_columns`**
- **Why**: Full schema synchronization for complete alignment
- **What**: Adds new, removes old, updates compatible types
- **Business Value**: Keeps table schema in sync with source files
- **DDL Validation**:
  - ADD COLUMN for new fields
  - DROP COLUMN for removed fields
  - ALTER COLUMN TYPE for compatible changes
  - Transaction/rollback handling

### P1 - Important (Should have for reliability)

#### 4. Error Handling Tests

**Test: `test_incompatible_type_change_error`**
- **Why**: Must catch and report incompatible changes clearly
- **What**: IncompatibleTypeChangeError with details
- **Business Value**: Clear error messages reduce debugging time
- **Scenarios**: string→int, timestamp→boolean, struct→string

**Test: `test_ddl_execution_failure_handling`**
- **Why**: DDL can fail due to locks, permissions, constraints
- **What**: Proper error propagation and cleanup
- **Business Value**: Prevents partial schema updates
- **Scenarios**: Permission denied, table locked, constraint violations

**Test: `test_null_and_empty_schema_handling`**
- **Why**: Edge cases in schema comparison
- **What**: Graceful handling of null/empty schemas
- **Business Value**: Robustness for edge cases
- **Scenarios**: New table creation, empty DataFrames

#### 5. Schema Version Tracking Tests

**Test: `test_schema_version_creation`**
- **Why**: Audit trail for all schema changes
- **What**: SchemaVersionRepository creates accurate records
- **Business Value**: Compliance and debugging capability
- **Fields**: version number, schema JSON, changes, timestamp

**Test: `test_schema_version_retrieval`**
- **Why**: Need to query schema history
- **What**: Repository methods for version queries
- **Business Value**: Schema evolution analysis and rollback
- **Queries**: Latest version, version by number, versions by date range

**Test: `test_concurrent_schema_evolution`**
- **Why**: Multiple files might trigger evolution simultaneously
- **What**: Prevents race conditions in version tracking
- **Business Value**: Data consistency in parallel processing
- **Scenarios**: Version number conflicts, concurrent DDL

### P2 - Nice to Have (Enhance user experience)

#### 6. Performance Tests

**Test: `test_large_schema_comparison_performance`**
- **Why**: Tables can have 100s of columns
- **What**: Performance benchmarks for large schemas
- **Business Value**: Ensures scalability
- **Metrics**: Comparison time vs column count

**Test: `test_schema_caching_effectiveness`**
- **Why**: Repeated schema operations should be fast
- **What**: Schema caching and invalidation
- **Business Value**: Reduced latency for frequent operations
- **Scenarios**: Cache hits, invalidation triggers

#### 7. Diagnostic and Debugging Tests

**Test: `test_schema_change_detailed_reporting`**
- **Why**: Users need clear understanding of changes
- **What**: Human-readable change descriptions
- **Business Value**: Reduced support tickets
- **Format**: Column names, old/new types, change categories

**Test: `test_schema_evolution_dry_run`**
- **Why**: Preview changes before applying
- **What**: Simulation mode without DDL execution
- **Business Value**: Safe testing of evolution strategies
- **Output**: Planned DDL statements, impact analysis

## Test Data Strategy

### Schema Test Fixtures

```python
# Fixture: Simple schemas for basic testing
@pytest.fixture
def simple_source_schema():
    """Source schema with basic types"""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("created_at", TimestampType(), True)
    ])

@pytest.fixture
def simple_target_schema():
    """Target schema with slight differences"""
    return StructType([
        StructField("id", LongType(), False),  # Type evolved
        StructField("name", StringType(), True),
        StructField("deleted", BooleanType(), True)  # Removed from source
    ])

# Fixture: Complex schemas for advanced testing
@pytest.fixture
def complex_schema_with_nested():
    """Schema with nested structures"""
    return StructType([
        StructField("id", LongType(), False),
        StructField("metadata", StructType([
            StructField("version", IntegerType()),
            StructField("tags", ArrayType(StringType()))
        ])),
        StructField("mappings", MapType(StringType(), DoubleType()))
    ])
```

## Mock Strategy

### Spark Session Mocking

```python
@pytest.fixture
def mock_spark():
    """Mock Spark session for DDL testing"""
    spark = MagicMock()
    spark.sql = MagicMock()
    spark.table = MagicMock()
    return spark

@pytest.fixture
def mock_table_with_schema(simple_target_schema):
    """Mock table with predefined schema"""
    df = MagicMock()
    df.schema = simple_target_schema
    return df
```

### Repository Mocking

```python
@pytest.fixture
def mock_schema_version_repo():
    """Mock schema version repository"""
    repo = MagicMock(spec=SchemaVersionRepository)
    repo.create_version.return_value = SchemaVersion(
        id="test-version-1",
        version=1,
        schema_json={},
        changes_json=[]
    )
    return repo
```

## Test Execution Plan

### Phase 1: Core Functionality (Week 1)
- Schema comparison logic (P0 tests 1-3)
- Type compatibility validation
- Basic strategy application

### Phase 2: Error Handling (Week 2)
- Exception scenarios (P1 tests 4-5)
- DDL failure handling
- Concurrent access patterns

### Phase 3: Integration (Week 3)
- End-to-end with BatchFileProcessor
- Repository integration
- Performance benchmarks

## Success Metrics

1. **Code Coverage**
   - Line coverage: ≥95%
   - Branch coverage: 100% for critical paths
   - Exception coverage: All error paths tested

2. **Test Quality**
   - All P0 tests passing
   - Test execution time < 30 seconds for unit tests
   - Zero flaky tests

3. **Business Impact**
   - Zero data corruption incidents
   - 90% reduction in schema-related support tickets
   - Automatic handling of 95% of schema changes

## Risk Mitigation

### High Risk Areas

1. **Concurrent Schema Evolution**
   - Risk: Race conditions causing duplicate versions
   - Mitigation: Database constraints, locking mechanisms
   - Test: Parallel execution scenarios

2. **Incompatible Type Changes**
   - Risk: Data loss from incorrect type conversion
   - Mitigation: Strict compatibility matrix, validation
   - Test: Comprehensive type combination matrix

3. **DDL Rollback**
   - Risk: Partial schema updates on failure
   - Mitigation: Transaction support or compensation logic
   - Test: Failure injection during DDL execution

## Test Maintenance

### Regular Review
- Monthly: Review test coverage metrics
- Quarterly: Update test cases for new Spark/Iceberg versions
- On-demand: Add tests for production issues

### Documentation
- Each test must have clear docstring
- Complex scenarios need inline comments
- Maintain test-to-requirement traceability

## Appendix: Test File Structure

```
tests/unit/
├── test_schema_evolution_service.py       # Core service tests
├── test_schema_comparison.py              # Comparison logic isolation
├── test_type_compatibility.py             # Type system tests
├── test_schema_strategies.py              # Strategy pattern tests
└── fixtures/
    ├── schema_fixtures.py                 # Reusable schema definitions
    └── mock_fixtures.py                   # Mock objects
```

## Next Steps

1. Review and approve test strategy with team
2. Set up test infrastructure and fixtures
3. Implement P0 tests first
4. Establish CI/CD integration with coverage gates
5. Create test data generators for complex scenarios