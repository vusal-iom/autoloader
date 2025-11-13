# Example: Using Testing Strategy Skills

## Quick Start Guide

### 1. Activate a Skill

In Claude Code, you can now use these commands:
- `/plan-unit-tests` - Create a unit test strategy
- `/implement-test` - Implement tests one by one

### 2. Example Usage for Schema Evolution Tests

Let's create a test strategy for your schema evolution features:

```bash
# Step 1: Plan the tests
/plan-unit-tests
> Target: app/services/schema_evolution_service.py
```

This will generate a document like:

---

# Unit Test Strategy: Schema Evolution Service

## Module Overview
- **File Path**: app/services/schema_evolution_service.py
- **Purpose**: Handles detection and management of schema changes in ingested files
- **Dependencies**: Spark, Iceberg catalog, File discovery service
- **Test File Location**: tests/unit/services/test_schema_evolution_service.py

## Test Coverage Goals
- Target Coverage: 90%
- Critical Functions: detect_schema_change, apply_evolution, validate_compatibility
- Mock Strategy: Mock Spark operations, use test schemas for validation

## Test Cases

### Function: `detect_schema_change`

#### Test 1: No Schema Change - Identical Schemas
- **Why This Test**: Verify system correctly identifies when schemas haven't changed
- **Input Setup**: Two identical Parquet schemas
- **Expected Behavior**: Return False, no evolution triggered
- **Assertions**: No schema change detected, no warnings logged
- **Mocks Required**: Mock file reader, schema comparison

#### Test 2: New Column Added
- **Why This Test**: Most common evolution scenario in production
- **Input Setup**: Existing schema + new schema with additional column
- **Expected Behavior**: Detect change, return evolution plan
- **Assertions**: Change detected, evolution plan includes ADD COLUMN
- **Mocks Required**: Mock Iceberg catalog

#### Test 3: Column Type Change - Compatible
- **Why This Test**: Ensure safe type promotions are handled (int → long)
- **Input Setup**: Schema with int column → schema with long column
- **Expected Behavior**: Detect change, allow compatible evolution
- **Assertions**: Evolution allowed, type promotion noted

#### Test 4: Column Type Change - Incompatible
- **Why This Test**: Prevent data corruption from incompatible changes
- **Input Setup**: Schema with string column → schema with int column
- **Expected Behavior**: Detect change, reject evolution, raise error
- **Assertions**: IncompatibleSchemaError raised, clear error message

#### Test 5: Column Dropped
- **Why This Test**: Handle column removal scenarios
- **Input Setup**: Schema with column removed
- **Expected Behavior**: Detect change, handle based on policy
- **Assertions**: Appropriate warning or error based on strict mode

### Function: `apply_evolution`

#### Test 1: Apply ADD COLUMN Evolution
- **Why This Test**: Verify Iceberg table alteration works correctly
- **Input Setup**: Evolution plan with ADD COLUMN operation
- **Expected Behavior**: Column added to Iceberg table
- **Assertions**: Table schema updated, column present
- **Mocks Required**: Mock Iceberg catalog operations

#### Test 2: Apply Multiple Changes
- **Why This Test**: Real-world scenarios often have multiple changes
- **Input Setup**: Plan with ADD and ALTER operations
- **Expected Behavior**: All changes applied in correct order
- **Assertions**: All changes reflected in final schema

#### Test 3: Rollback on Failure
- **Why This Test**: Ensure atomicity of schema changes
- **Input Setup**: Evolution plan with failing operation
- **Expected Behavior**: Rollback all changes on failure
- **Assertions**: Original schema preserved, error logged

## Test Prioritization

### P0 - Critical (Must Have)
- [ ] detect_schema_change - No change scenario
- [ ] detect_schema_change - New column added
- [ ] detect_schema_change - Incompatible type change
- [ ] apply_evolution - ADD COLUMN
- [ ] apply_evolution - Rollback on failure

### P1 - Important (Should Have)
- [ ] detect_schema_change - Compatible type change
- [ ] apply_evolution - Multiple changes
- [ ] validate_compatibility - All scenarios

### P2 - Nice to Have
- [ ] Performance tests for large schemas
- [ ] Edge cases with nested structures

## Special Considerations
- Schema comparison must handle nullable differences
- Order of columns shouldn't trigger false positives
- Consider Parquet vs Iceberg schema representations
- Test with real-world schema examples from production

## Validation Criteria
- All P0 tests must pass
- Schema evolution must be backward compatible
- Error messages must be actionable
- No data loss scenarios allowed

---

### 3. Implement Tests One by One

```bash
# Step 2: Start implementation
/implement-test
> Strategy document: docs/test-strategies/unit/schema_evolution_test_strategy.md
> Select test: detect_schema_change - No change scenario

# The skill will write ONE test, run it, then ask if you want to continue
```

### 4. Sample Test Output

The implementation skill will generate something like:

```python
def test_detect_schema_change_no_change():
    """
    Test Strategy Reference: No Schema Change - Identical Schemas
    Purpose: Verify system correctly identifies when schemas haven't changed
    Priority: P0
    """
    # Arrange - Set up identical schemas
    service = SchemaEvolutionService()
    existing_schema = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False)
    ])
    new_schema = existing_schema  # Identical schema

    # Act - Check for schema changes
    result = service.detect_schema_change(
        existing_schema=existing_schema,
        new_schema=new_schema
    )

    # Assert - No change should be detected
    assert result.has_changes is False, "Should not detect changes in identical schemas"
    assert result.evolution_plan is None, "No evolution plan should be generated"
    assert len(result.warnings) == 0, "No warnings for identical schemas"
```

Then run: `pytest tests/unit/services/test_schema_evolution_service.py::test_detect_schema_change_no_change -xvs`

## Benefits You'll See

1. **Consistent Structure**: Every test follows the same pattern
2. **Clear Documentation**: The strategy document explains WHY each test exists
3. **Manageable Reviews**: One test at a time = easier code reviews
4. **Progress Tracking**: Always know what's done and what's next
5. **Knowledge Preservation**: New team members understand test rationale

## Tips for Your Workflow

1. **Start Small**: Begin with one service or module
2. **Iterate on Strategy**: The strategy doc is living - update it as you learn
3. **Use P0/P1/P2**: Prioritization helps focus on what matters
4. **One PR Per Module**: Keep pull requests focused
5. **Run Tests Immediately**: Don't accumulate untested code

## Next Steps

1. Try the planning skill on one of your services
2. Review and refine the generated strategy
3. Use the implementation skill to write tests one by one
4. Adjust the skills based on your team's preferences

The skills are in `.claude/skills/` and can be customized for your specific needs!