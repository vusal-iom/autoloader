# Testing Strategy Quick Start Guide

## Your New Testing Workflow

You now have a reproducible, structured approach to testing that creates documentation and implements tests one at a time.

## Available Skills

You've created 4 testing skills in `.claude/skills/`:

1. **`/plan-unit-tests`** - Create unit test strategies
2. **`/plan-integration-tests`** - Create integration test strategies
3. **`/plan-e2e-tests`** - Create E2E test strategies
4. **`/implement-test`** - Implement tests one by one from strategies

## How to Use Your New Workflow

### Phase 1: Planning (Documentation First)

```bash
# For unit tests
/plan-unit-tests
> Which module? app/services/ingestion_service.py
# Creates: docs/test-strategies/unit/ingestion_service_test_strategy.md

# For integration tests
/plan-integration-tests
> Which feature? S3 to Iceberg ingestion flow
# Creates: docs/test-strategies/integration/s3_ingestion_integration_strategy.md

# For E2E tests
/plan-e2e-tests
> Which journey? Complete ingestion setup by new user
# Creates: docs/test-strategies/e2e/new_user_ingestion_e2e_strategy.md
```

### Phase 2: Implementation (One Test at a Time)

```bash
/implement-test
> Strategy document? docs/test-strategies/unit/ingestion_service_test_strategy.md
> Which test? test_create_ingestion_with_valid_data

# Claude will:
# 1. Write ONE test
# 2. Run it immediately
# 3. Fix if it fails
# 4. Ask if you want the next test
```

## Key Benefits You Asked For

✅ **Reproducible Steps**: Same workflow every time
✅ **Planning Documents**: Markdown strategies before code
✅ **High-Level Language**: WHY tests exist, WHAT they cover
✅ **One Test Per Iteration**: Easy to review and change
✅ **Different Skills for Different Tests**: Unit vs Integration vs E2E

## Example: Your Schema Evolution Tests

### Step 1: Create the Plan
```bash
/plan-unit-tests
> Target: app/services/schema_evolution_service.py
```

This generates a strategy document with test cases like:
- Test no schema change scenario
- Test adding new columns
- Test type compatibility
- Test incompatible changes
- Test error handling

### Step 2: Implement One by One
```bash
/implement-test
> Strategy: docs/test-strategies/unit/schema_evolution_test_strategy.md
> Test: detect_schema_change_no_change

# Writes and runs one test
# Then asks: Continue with next test?
```

## Customization Tips

The skills are just markdown files in `.claude/skills/`. You can:

1. **Add Domain-Specific Skills**:
   - `plan-spark-tests.md` for Spark-specific testing
   - `plan-performance-tests.md` for load testing
   - `plan-security-tests.md` for security validation

2. **Modify Existing Skills**:
   - Add your team's specific requirements
   - Include company coding standards
   - Add custom test patterns

3. **Create Workflow Chains**:
   ```bash
   # Create a skill that runs multiple skills
   /complete-test-suite
   # Which runs: plan → implement → coverage report
   ```

## Your Testing Strategy Files

All your test strategies will be organized in:
```
docs/
└── test-strategies/
    ├── unit/
    │   ├── ingestion_service_test_strategy.md
    │   ├── schema_evolution_test_strategy.md
    │   └── ...
    ├── integration/
    │   ├── s3_ingestion_integration_strategy.md
    │   └── ...
    └── e2e/
        ├── new_user_journey_e2e_strategy.md
        └── ...
```

## Version Control

Don't forget to commit your skills:
```bash
git add .claude/skills/
git add docs/test-strategies/
git commit -m "Add reproducible testing strategy system"
```

## Next Steps

1. **Try It Out**: Run `/plan-unit-tests` on one of your services
2. **Review**: Check the generated strategy document
3. **Implement**: Use `/implement-test` to write your first test
4. **Iterate**: One test at a time until coverage is complete
5. **Refine**: Update the skills based on what you learn

## Questions?

- The skills are in `.claude/skills/` - you can read and modify them
- The strategies go in `docs/test-strategies/` - they're living documents
- Each test implementation is atomic - easy to review in PRs
- The workflow is reproducible - same process every time

Now you have a systematic, documented approach to testing that creates both planning documentation and incremental implementation!