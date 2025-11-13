# Test Implementation - One Test at a Time

You are implementing tests based on a test strategy document. Follow this disciplined approach:

## Step 1: Load the Strategy
Ask: "Which test strategy document should I use? (Provide the path to the strategy markdown file)"

Once provided:
1. Read the strategy document
2. List all test cases with their priorities
3. Show the user: "Here are the test cases from the strategy: [list]. Which one should I implement first?"

## Step 2: Implement ONE Test

### Before Writing Code:
1. Confirm the selected test case
2. Review the test details from the strategy
3. Say: "I'll now implement: [Test Name]. This test will [purpose from strategy]."

### Writing the Test:
```python
def test_[descriptive_name_from_strategy]():
    """
    Test Strategy Reference: [Test name from doc]
    Purpose: [Why this test exists]
    Priority: [P0/P1/P2]
    """
    # Arrange - Set up test data and mocks
    # [Clear setup with comments]

    # Act - Execute the function being tested
    # [Single action being tested]

    # Assert - Verify the results
    # [Clear assertions with meaningful messages]
```

### Implementation Rules:
1. **ONE test only** - Do not write multiple tests
2. **Self-contained** - Test should not depend on other tests
3. **Clear naming** - Test name should describe what it tests
4. **Meaningful assertions** - Include assertion messages
5. **Proper cleanup** - Use fixtures or teardown as needed

## Step 3: Run and Verify

After writing the test:
1. Run the specific test: `pytest path/to/test.py::test_function_name -xvs`
2. If it fails:
   - Analyze the error
   - Fix the issue
   - Run again
3. If it passes:
   - Confirm: "✓ Test passed successfully"

## Step 4: Update Progress

1. Show current status:
```
Test Implementation Progress:
✓ test_[completed_test_name] - COMPLETED
⏳ test_[next_test_name] - PENDING
⏳ test_[another_test_name] - PENDING
```

2. Ask: "Would you like to implement the next test? If yes, which one?"

## Step 5: Iteration Loop

If user wants to continue:
1. Go back to Step 2 with the next test
2. Maintain the same disciplined approach
3. ONE test at a time

If user wants to stop:
1. Summarize what was completed
2. Save any notes about remaining work
3. Suggest next steps

## Important Reminders

### DO:
- Implement exactly ONE test per iteration
- Run the test immediately after writing
- Fix any failures before moving on
- Keep tests focused and simple
- Follow the strategy document closely

### DON'T:
- Write multiple tests at once
- Skip running the test
- Move on with failing tests
- Deviate from the strategy without discussion
- Make tests overly complex

## Example Interaction Flow

```
User: /implement-test
Assistant: Which test strategy document should I use?
User: docs/test-strategies/unit/ingestion_service_test_strategy.md
Assistant: I've loaded the strategy. Here are the test cases:
  P0: test_create_ingestion_with_valid_data
  P0: test_validate_source_credentials
  P1: test_handle_duplicate_ingestion_name
  P2: test_performance_with_large_file_list

  Which one should I implement first?
User: Start with test_create_ingestion_with_valid_data
Assistant: I'll now implement: test_create_ingestion_with_valid_data. This test will verify that an ingestion can be created with valid input data.

[Writes ONE test]
[Runs the test]
✓ Test passed successfully

Test Implementation Progress:
✓ test_create_ingestion_with_valid_data - COMPLETED
⏳ test_validate_source_credentials - PENDING
⏳ test_handle_duplicate_ingestion_name - PENDING
⏳ test_performance_with_large_file_list - PENDING

Would you like to implement the next test? If yes, which one?
```

## Special Cases

### When Tests Fail
1. Do not proceed to next test
2. Debug and fix the current test
3. Explain what went wrong
4. Show the fix
5. Re-run to confirm

### When Strategy Needs Updates
1. If the implementation reveals issues with the strategy
2. Note what needs to be updated
3. Ask if you should update the strategy document
4. Make updates if approved
5. Continue with implementation

### When Dependencies Are Complex
1. Create helper functions or fixtures if needed
2. But still implement ONE test at a time
3. The helper code doesn't count as a separate test
4. Focus remains on the single test case