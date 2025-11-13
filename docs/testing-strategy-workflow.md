# Testing Strategy Workflow Documentation

## Overview
This document outlines how to create reproducible testing strategies using Claude Code skills. The workflow consists of two phases:
1. **Planning Phase**: Create detailed test strategy documents
2. **Implementation Phase**: Execute tests one at a time based on the plan

## Why Skills Over Agents?
- **Skills** are better for your use case because:
  - They're reusable templates for specific workflows
  - They can be invoked with `/skill-name` commands
  - They maintain consistent structure across projects
  - They're easier to version control and share

- **Agents** are better for:
  - One-off complex investigations
  - Tasks requiring extensive autonomy
  - Exploratory work without defined structure

## Setting Up Your Testing Skills

### 1. Create the Skills Directory Structure
```
.claude/
├── skills/
│   ├── plan-unit-tests.md
│   ├── plan-integration-tests.md
│   ├── plan-e2e-tests.md
│   ├── implement-test.md
│   └── review-test-coverage.md
```

### 2. Skill Templates

#### A. Unit Test Planning Skill (.claude/skills/plan-unit-tests.md)
```markdown
# Unit Test Planning Skill

You are helping plan unit tests for the codebase. Follow these steps:

1. **Analyze the Target Module**
   - Read the module/class to be tested
   - Identify all public methods/functions
   - List dependencies and mock requirements

2. **Create Test Strategy Document**
   Generate a markdown document with:

   ## Unit Test Strategy for [Module Name]

   ### Overview
   - Module purpose: [Brief description]
   - Test scope: [What will be tested]
   - Mock requirements: [External dependencies to mock]

   ### Test Cases

   #### Test Case 1: [Descriptive Name]
   - **Purpose**: Why this test exists
   - **Coverage**: What functionality it validates
   - **Input**: Expected input conditions
   - **Expected Behavior**: What should happen
   - **Edge Cases**: Boundary conditions to consider
   - **Mocks Required**: Dependencies to mock

   #### Test Case 2: [Descriptive Name]
   [Same structure...]

   ### Test Prioritization
   - Critical path tests (P0)
   - Important functionality (P1)
   - Edge cases (P2)

   ### Coverage Goals
   - Target coverage percentage
   - Critical paths that must be covered

3. **Save as**: `docs/test-strategies/unit/[module]_test_strategy.md`
```

#### B. Integration Test Planning Skill (.claude/skills/plan-integration-tests.md)
```markdown
# Integration Test Planning Skill

You are helping plan integration tests. Follow these steps:

1. **Analyze System Boundaries**
   - Identify components that interact
   - Map data flow between services
   - List external systems (databases, APIs, etc.)

2. **Create Integration Test Strategy Document**

   ## Integration Test Strategy for [Feature/Flow]

   ### System Context
   - Components involved: [List components]
   - Data flow: [Describe the flow]
   - External dependencies: [Databases, APIs, etc.]

   ### Test Scenarios

   #### Scenario 1: [Happy Path Name]
   - **Purpose**: Validate normal operation flow
   - **Setup**: Required initial state
   - **Actions**: Steps to execute
   - **Verification Points**: What to check
   - **Cleanup**: Post-test cleanup needed

   #### Scenario 2: [Error Handling]
   - **Purpose**: Validate error conditions
   - **Failure Points**: Where things can go wrong
   - **Expected Recovery**: How system should respond

   ### Test Environment Requirements
   - Database state
   - Service configurations
   - Network conditions

   ### Performance Considerations
   - Expected response times
   - Resource usage limits

3. **Save as**: `docs/test-strategies/integration/[feature]_test_strategy.md`
```

#### C. E2E Test Planning Skill (.claude/skills/plan-e2e-tests.md)
```markdown
# End-to-End Test Planning Skill

You are helping plan E2E tests. Follow these steps:

1. **Map User Journeys**
   - Identify critical user paths
   - Define success criteria
   - Note system touchpoints

2. **Create E2E Test Strategy Document**

   ## E2E Test Strategy for [User Journey]

   ### User Journey Overview
   - User persona: [Who is the user]
   - Goal: [What they're trying to achieve]
   - Success criteria: [How we know it worked]

   ### Test Scenarios

   #### Journey 1: [Complete Flow Name]
   - **Pre-conditions**: Initial system state
   - **User Actions**: Step-by-step actions
   - **System Responses**: Expected behaviors
   - **Validation Points**: Key checkpoints
   - **Data Verification**: End state validation

   ### Test Data Requirements
   - User accounts needed
   - Sample data sets
   - Configuration settings

   ### Environment Setup
   - Services required
   - External integrations
   - Test data reset strategy

3. **Save as**: `docs/test-strategies/e2e/[journey]_test_strategy.md`
```

#### D. Test Implementation Skill (.claude/skills/implement-test.md)
```markdown
# Test Implementation Skill

You are implementing tests based on a strategy document. Follow these rules:

1. **Read the Strategy Document First**
   - Load the test strategy document
   - Pick ONE test case to implement
   - Confirm with user which test to implement

2. **Implementation Guidelines**
   - Write only ONE test at a time
   - Make the test self-contained
   - Include clear assertions
   - Add descriptive comments
   - Use proper test naming conventions

3. **Test Structure**
   ```python
   def test_[descriptive_name]():
       """
       Test Case: [Name from strategy doc]
       Purpose: [Why this test exists]
       Coverage: [What it validates]
       """
       # Arrange
       [Setup code]

       # Act
       [Execute the functionality]

       # Assert
       [Verify the results]
   ```

4. **After Implementation**
   - Run the test
   - Verify it passes
   - Update coverage metrics
   - Ask: "Which test should I implement next?"

5. **Important Rules**
   - ONE test per iteration
   - Always run after writing
   - Fix failures before moving on
   - Update strategy doc if changes needed
```

### 3. Using the Skills

#### Planning Phase
```bash
# Start planning unit tests
/plan-unit-tests

# The skill will:
# 1. Ask which module/class to test
# 2. Analyze the code
# 3. Generate the strategy document
# 4. Save it for review
```

#### Implementation Phase
```bash
# Start implementing from a plan
/implement-test

# The skill will:
# 1. Ask for the strategy document path
# 2. Show available test cases
# 3. Implement one test
# 4. Run it and verify
# 5. Ask if you want to continue
```

## Workflow Example

### Step 1: Plan Your Tests
```bash
# Plan unit tests for ingestion_service.py
/plan-unit-tests
> Target: app/services/ingestion_service.py

# Output: docs/test-strategies/unit/ingestion_service_test_strategy.md
```

### Step 2: Review the Plan
- Open the generated strategy document
- Review test cases and priorities
- Make any adjustments needed

### Step 3: Implement Tests Iteratively
```bash
# Start implementation
/implement-test
> Strategy document: docs/test-strategies/unit/ingestion_service_test_strategy.md
> Select test case: 1 (test_create_ingestion_with_valid_data)

# After first test passes
> Continue? Yes
> Select test case: 2 (test_create_ingestion_with_invalid_source)
```

## Benefits of This Approach

1. **Reproducibility**: Same process every time
2. **Documentation**: Strategy documents serve as test documentation
3. **Incremental Progress**: One test at a time prevents overwhelm
4. **Review-Friendly**: Easy to review individual tests
5. **Knowledge Transfer**: New team members can understand test rationale
6. **Coverage Tracking**: Clear view of what's tested and what's not

## Tips for Success

1. **Start with Critical Paths**: Focus on the most important functionality first
2. **Use the Strategy Docs**: They're living documents - update as needed
3. **Run Tests Immediately**: Don't accumulate untested code
4. **Mock Wisely**: Only mock what you must, test real interactions when possible
5. **Keep Tests Independent**: Each test should run in isolation

## Advanced: Customizing Skills

You can create specialized skills for your specific needs:
- `plan-schema-evolution-tests.md` for Iceberg schema changes
- `plan-spark-tests.md` for Spark-specific testing
- `plan-performance-tests.md` for load testing

Each skill can have domain-specific prompts and structures.

## Version Control

Always commit your skills to version control:
```bash
git add .claude/skills/
git commit -m "Add testing strategy skills"
```

This allows your team to share and evolve the testing workflow together.