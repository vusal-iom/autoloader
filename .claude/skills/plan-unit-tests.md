# Unit Test Planning Strategy

You are creating a comprehensive unit test strategy. Follow these steps systematically:

## Step 1: Gather Information
Ask the user: "Which module, class, or file would you like to create unit tests for?"

## Step 2: Analyze the Code
1. Read the target file completely
2. Identify all public functions/methods
3. List all external dependencies
4. Note any complex logic or edge cases

## Step 3: Create Strategy Document

Generate a markdown document with this structure:

```markdown
# Unit Test Strategy: [Module/Class Name]

## Module Overview
- **File Path**: [path]
- **Purpose**: [What this module does]
- **Dependencies**: [List external dependencies]
- **Test File Location**: tests/unit/[appropriate_path]

## Test Coverage Goals
- Target Coverage: [percentage]%
- Critical Functions: [List must-test functions]
- Mock Strategy: [How to handle dependencies]

## Test Cases

### Function: `[function_name]`

#### Test 1: [Test Name - Happy Path]
- **Why This Test**: [Business reason for this test]
- **Input Setup**: [What inputs to provide]
- **Expected Behavior**: [What should happen]
- **Assertions**: [What to verify]
- **Mocks Required**: [What to mock, if any]

#### Test 2: [Test Name - Edge Case]
- **Why This Test**: [What edge case this covers]
- **Input Setup**: [Boundary conditions]
- **Expected Behavior**: [How it should handle edge case]
- **Assertions**: [What to verify]

#### Test 3: [Test Name - Error Handling]
- **Why This Test**: [What error scenario]
- **Input Setup**: [Invalid inputs]
- **Expected Behavior**: [Error handling expectation]
- **Assertions**: [Error type, message]

### Function: `[next_function]`
[Repeat pattern above]

## Test Prioritization
### P0 - Critical (Must Have)
- [ ] [Test name] - [Reason why critical]

### P1 - Important (Should Have)
- [ ] [Test name] - [What it covers]

### P2 - Nice to Have
- [ ] [Test name] - [Additional coverage]

## Special Considerations
- [Any specific testing challenges]
- [Performance considerations]
- [Complex mock scenarios]

## Validation Criteria
- All P0 tests must pass
- No breaking changes to existing tests
- Code coverage meets target
```

## Step 4: Save and Review
Save the document to: `docs/test-strategies/unit/[module_name]_test_strategy.md`

Ask: "Would you like me to review any specific test case in more detail, or shall we proceed with the implementation plan?"

## Important Notes
- Focus on testing behavior, not implementation
- Each test should have a clear business purpose
- Avoid testing private methods directly
- Keep tests simple and readable
- One assertion per test when possible