# Complete Guide to Using Agents in Claude Code

## Table of Contents
1. [Introduction](#introduction)
2. [What Are Agents?](#what-are-agents)
3. [Why Use Agents?](#why-use-agents)
4. [Types of Agents](#types-of-agents)
5. [How to Use Agents](#how-to-use-agents)
6. [Practical Examples](#practical-examples)
7. [Creating Custom Agents](#creating-custom-agents)
8. [Best Practices](#best-practices)
9. [Advanced Topics](#advanced-topics)

---

## Introduction

Agents (also called "subagents") are one of the most powerful features in Claude Code. They enable you to delegate specialized tasks to AI assistants that are specifically configured for particular purposes. Think of them as expert teammates, each with their own focus area and toolset.

This guide will teach you everything you need to know about leveraging agents to boost your productivity in Claude Code.

---

## What Are Agents?

**Agents are specialized AI assistants that Claude Code can delegate tasks to.** Each agent operates with its own separate context window from the main conversation and includes custom configuration tailored to specific purposes.

### Key Characteristics

- **Separate Context**: Each agent maintains its own conversation history, preventing clutter in your main chat
- **Specialized Configuration**: Agents can have custom system prompts, tool access, and model selection
- **Autonomous Operation**: Once invoked, agents work independently to complete their assigned tasks
- **Resumable**: You can continue previous agent conversations using agent IDs for multi-step workflows

### How They Differ from Regular Claude

While the main Claude Code interface is a general-purpose assistant, agents are focused specialists. When you ask Claude to perform a complex task, it can:
1. Recognize that a specialized agent would handle it better
2. Automatically delegate to that agent
3. Return the results to you while keeping your main conversation clean

---

## Why Use Agents?

### 1. **Context Preservation**
Agents prevent context pollution in your main conversation. When debugging a complex issue or reviewing extensive code, the agent's work stays in its own context window, keeping your main chat focused and efficient.

### 2. **Specialized Expertise**
Each agent can be fine-tuned for specific domains:
- A code reviewer agent focuses on quality, security, and maintainability
- A debugger agent specializes in root cause analysis
- A data analysis agent excels at SQL and BigQuery operations

This specialization leads to higher success rates for complex tasks.

### 3. **Reusability**
Create an agent once, use it across multiple projects:
- Share agents with your team via version control
- Build a library of specialized assistants for common workflows
- Standardize code review criteria across your organization

### 4. **Flexible Permissions & Security**
Grant different tool access levels per agent:
- Limit a documentation agent to read-only operations
- Allow a deployment agent to execute bash commands
- Restrict database agents to specific query tools

### 5. **Improved Efficiency**
Instead of repeatedly explaining the same context or requirements, agents remember their configuration:
- No need to remind Claude about your code review standards
- Automated task execution with consistent quality
- Parallel execution of independent tasks

---

## Types of Agents

Claude Code supports several types of agents, each with different scopes and use cases:

### 1. Built-in Agents

Claude Code includes specialized built-in agents like:

- **Plan Agent**: Assists with codebase research during plan mode, helping explore and understand code structure
- **Explore Agent**: Fast agent specialized for exploring codebases, finding files by patterns, searching code for keywords
- **General-purpose Agent**: For researching complex questions and executing multi-step tasks

These are always available and don't require any setup.

### 2. Project Agents

**Location**: `.claude/agents/` in your project directory

**Scope**: Available only within the specific project

**Priority**: Highest - these override user and plugin agents with the same name

**Use Case**: Project-specific workflows like:
- Custom code review standards for your codebase
- Project-specific testing strategies
- Domain-specific refactoring patterns

**Benefit**: Version controlled with your project, shared with all team members

### 3. User Agents

**Location**: `~/.claude/agents/` in your home directory

**Scope**: Available across all projects

**Priority**: Lower than project agents

**Use Case**: Personal workflows and preferences:
- Your preferred code style reviewer
- Personal productivity agents
- Cross-project utilities

**Benefit**: Consistent across all your work, customized to your preferences

### 4. Plugin Agents

**Location**: Provided by installed plugins

**Scope**: Available when the plugin is active

**Use Case**: Domain-specific capabilities from the community:
- Language-specific analyzers
- Framework-specific helpers
- Integration with external tools

**Benefit**: Extend Claude Code with community expertise

### 5. CLI-Defined Agents

**Definition**: Created via `--agents` flag when starting Claude Code

**Scope**: Session-specific, temporary

**Use Case**: One-off specialized tasks or experimentation

**Benefit**: Quick agent creation without file management

---

## How to Use Agents

There are two primary ways to use agents in Claude Code:

### 1. Automatic Delegation (Recommended)

Claude intelligently recognizes when a task matches an agent's description and automatically delegates to it.

**Example:**
```
You: "Review my authentication changes for security issues"
```

If you have a code-reviewer agent configured, Claude will automatically use it without you needing to explicitly request it.

**How it works:**
- Claude reads the `description` field in your agent's configuration
- Matches your request against available agents
- Selects and invokes the most appropriate agent
- Returns results to your main conversation

### 2. Explicit Invocation

You can specifically request an agent by name when you want direct control.

**Example:**
```
You: "Use the debugger subagent to investigate why users can't log in"
```

**When to use:**
- You know exactly which agent you want
- Multiple agents might match, but you have a preference
- You want to ensure a specific agent handles the task

### 3. Task Tool (Programmatic)

In your own agents or workflows, you can use the Task tool to invoke agents:

```
Use the Task tool with subagent_type="Explore" to find authentication handlers
```

---

## Practical Examples

### Example 1: Code Review Workflow

**Scenario**: You've just finished implementing a new authentication feature and want it reviewed.

**Without Agents:**
```
You: "Can you review my changes for security issues, coding standards, test coverage, and performance?"
Claude: [Reads files, provides lengthy analysis in main chat]
```

**With Agents:**
```
You: "Review my authentication changes"
Claude: [Automatically delegates to code-reviewer agent]
Code-reviewer agent: [Analyzes security, standards, coverage, performance]
Claude: [Returns concise summary of findings]
```

**Benefits**:
- Cleaner main conversation
- Specialized review criteria
- Consistent review standards
- Separate context for detailed analysis

### Example 2: Bug Investigation

**Scenario**: Users report login failures in production.

**Command:**
```
You: "Use the debugger subagent to investigate why users can't log in"
```

**What happens:**
1. Debugger agent is invoked with the problem description
2. Agent searches logs, reviews error handling code
3. Agent analyzes recent changes related to authentication
4. Agent identifies root cause and suggests fixes
5. Results returned to your main conversation

**Why it works better:**
- Debugger agent knows to check common failure patterns
- Configured with specific debugging tools and permissions
- Follows systematic debugging methodology
- Keeps verbose investigation details out of main chat

### Example 3: Test Coverage Expansion

**Scenario**: You want to improve test coverage for a service.

**Workflow:**
```
You: "Find functions in UserService that aren't covered by tests"

Claude: [Uses Explore agent to analyze codebase]
Explore agent: [Identifies 5 untested functions]

You: "Generate tests for those functions"

Claude: [Uses test-generator agent if available]
Test-generator agent: [Creates comprehensive test cases]
```

**Value**:
- Systematic coverage analysis
- Consistent test patterns
- Automated test scaffolding
- Edge case identification

### Example 4: Data Analysis

**Scenario**: You need to analyze user engagement metrics.

**Custom Agent**: data-analyst (configured for SQL and BigQuery)

```
You: "Analyze user engagement trends over the last 30 days"

Claude: [Delegates to data-analyst agent]
Data-analyst agent:
  - Queries database for engagement metrics
  - Calculates trends and patterns
  - Generates visualization queries
  - Summarizes insights

Claude: [Presents findings with recommendations]
```

**Agent Configuration** enables:
- Pre-configured database connections
- Standard metric definitions
- Preferred analysis frameworks
- Visualization templates

### Example 5: Refactoring Legacy Code

**Scenario**: Modernize deprecated API usage across your project.

```
You: "Find and update all uses of the deprecated AuthManager to use AuthService"

Claude: [Uses general-purpose agent for complex multi-step task]
Agent:
  1. Searches codebase for AuthManager usage
  2. Analyzes each usage context
  3. Generates safe refactoring changes
  4. Runs tests after each change
  5. Tracks progress with todo list

Claude: [Reports completion with file-by-file summary]
```

**Multi-step benefit**: Agent maintains context and state across the entire refactoring operation.

---

## Creating Custom Agents

### Basic Agent Structure

Agents are defined as Markdown files with YAML frontmatter:

```markdown
---
name: code-reviewer
description: Reviews code for quality, security, and best practices. Use this agent after completing feature development.
tools: Read, Grep, Bash
model: sonnet
---

# Code Reviewer Agent

You are a specialized code review assistant focused on:

## Review Criteria
1. **Security**: Check for common vulnerabilities (SQL injection, XSS, CSRF, auth issues)
2. **Code Quality**: Assess readability, maintainability, and adherence to SOLID principles
3. **Testing**: Verify test coverage and quality
4. **Performance**: Identify potential bottlenecks
5. **Standards**: Ensure compliance with project coding standards

## Review Process
1. Use git diff to see what changed
2. Read relevant files for context
3. Analyze against each criterion
4. Provide specific, actionable feedback
5. Prioritize findings (Critical, High, Medium, Low)

## Output Format
Provide a structured review with:
- Summary (2-3 sentences)
- Findings by priority
- Recommended actions
- Positive observations

Be constructive and educational in your feedback.
```

### Configuration Fields

#### Required Fields

- **name**: Unique identifier (lowercase, hyphens for spaces)
  - Example: `code-reviewer`, `data-analyst`, `test-generator`

- **description**: What the agent does and when Claude should invoke it
  - Be specific about triggering scenarios
  - Example: "Reviews code for quality, security, and best practices. Use this agent after completing feature development or when explicitly requested to review code."

#### Optional Fields

- **tools**: Comma-separated list of allowed tools
  - If omitted, agent inherits all tools from parent
  - Examples: `Read, Grep, Bash`, `Read, WebFetch`, `Bash, Edit, Write`
  - Use for security: limit agents to only necessary tools

- **model**: Which Claude model to use
  - Options: `sonnet` (default), `opus` (most capable), `haiku` (fastest)
  - If omitted, inherits from parent conversation
  - Choose based on task complexity vs speed needs

### System Prompt Best Practices

The Markdown content after the frontmatter is your agent's system prompt. Make it effective:

#### 1. Define Clear Scope
```markdown
# Debugger Agent

You are a debugging specialist focused exclusively on identifying root causes of errors,
test failures, and unexpected behavior. You do NOT implement fixes - only diagnose issues.
```

#### 2. Provide Specific Instructions
```markdown
## Debugging Process
1. Reproduce the error if possible
2. Examine error messages and stack traces
3. Review recent code changes (git log, git diff)
4. Check relevant configuration files
5. Trace execution flow
6. Identify the precise failure point
7. Explain the root cause clearly
```

#### 3. Include Examples
```markdown
## Output Example

**Issue**: NullPointerException in UserService.login()

**Root Cause**: The validateCredentials() method returns null when user is not found,
but login() calls .getRole() on the result without null checking.

**Location**: UserService.java:142

**Recommendation**: Add null check before accessing user properties or throw
appropriate exception from validateCredentials().
```

#### 4. Set Standards and Constraints
```markdown
## Constraints
- Never make changes without explicit approval
- Always cite specific line numbers when referencing code
- Prioritize actionable findings over theoretical improvements
- Limit reviews to files changed in the current branch
```

### Creating Your First Agent

Let's create a practical agent for your IOMETE Autoloader project:

**File**: `.claude/agents/spark-expert.md`

```markdown
---
name: spark-expert
description: Specializes in Apache Spark, PySpark, and Spark Connect issues. Use for debugging Spark jobs, optimizing queries, or reviewing Spark-related code.
tools: Read, Grep, Bash, Edit
model: sonnet
---

# Spark Expert Agent

You are a specialist in Apache Spark and PySpark, with deep knowledge of:
- Spark Connect architecture
- DataFrame and SQL optimizations
- Streaming and batch processing
- Iceberg table operations
- Common Spark pitfalls and best practices

## Focus Areas

### 1. Performance
- Identify inefficient operations (collect, cartesian joins, skew)
- Suggest partitioning strategies
- Recommend caching opportunities
- Analyze execution plans

### 2. Correctness
- Verify schema handling
- Check checkpoint management
- Validate write modes (append, overwrite, merge)
- Review error handling

### 3. Best Practices
- Session management
- Resource configuration
- Monitoring and observability
- Testing strategies for Spark code

## Project Context
This project uses:
- Spark Connect (port 15002)
- Apache Iceberg for destination tables
- Schema inference and evolution
- Checkpoint-based processing

## Response Format
1. Identify the issue/opportunity
2. Explain the impact
3. Provide specific code recommendations
4. Reference Spark documentation when relevant
```

**Usage:**
```
You: "Review the batch_orchestrator for Spark efficiency issues"
You: "Use the spark-expert to debug why schema inference is failing"
```

### Agent Development Workflow

1. **Start Simple**: Let Claude generate an initial agent
   ```
   You: "Create a code-reviewer agent for this project"
   ```

2. **Test and Iterate**: Use the agent, observe results, refine
   ```
   You: "Use the code-reviewer agent on my latest changes"
   [Review results]
   [Edit agent configuration based on what worked/didn't]
   ```

3. **Specialize Gradually**: Add project-specific knowledge over time
   ```markdown
   ## Project-Specific Standards
   - All API endpoints must return Pydantic schemas
   - Database access only through repository layer
   - Services must not directly import SQLAlchemy models
   ```

4. **Share and Standardize**: Version control project agents
   ```bash
   git add .claude/agents/
   git commit -m "Add code-reviewer agent with project standards"
   ```

---

## Best Practices

### 1. Design Single-Purpose Agents

**Good**: Separate agents for reviewing, debugging, testing
```
code-reviewer.md    # Reviews code quality
debugger.md         # Diagnoses issues
test-generator.md   # Creates tests
```

**Avoid**: One mega-agent that does everything
```
super-agent.md      # Reviews, debugs, tests, deploys, makes coffee
```

**Why**: Focused agents are more reliable, maintainable, and reusable.

### 2. Write Descriptive Trigger Conditions

**Good**:
```yaml
description: Reviews Python code for PEP 8 compliance, type hints, and docstring quality. Use when explicitly asked to review code or after implementing new Python functions.
```

**Avoid**:
```yaml
description: Reviews code
```

**Why**: Clear descriptions enable better automatic delegation.

### 3. Limit Tool Access Appropriately

**For read-only analysis**:
```yaml
tools: Read, Grep
```

**For code modification**:
```yaml
tools: Read, Grep, Edit
```

**For deployment tasks**:
```yaml
tools: Read, Bash
```

**Why**: Principle of least privilege - agents only access what they need.

### 4. Choose the Right Model

**Use `haiku` for**:
- Simple, repetitive tasks
- Fast iterations
- Cost-sensitive operations
- Pattern matching

**Use `sonnet` for**:
- Most development tasks (default)
- Balanced capability and speed
- General-purpose agents

**Use `opus` for**:
- Complex reasoning tasks
- Novel problem-solving
- High-stakes code review
- Architecture decisions

### 5. Provide Rich Context in System Prompts

Include:
- **Project-specific conventions**: Naming patterns, file organization
- **Technology stack details**: Versions, key libraries, frameworks
- **Common pitfalls**: Known issues in your codebase
- **Examples**: Template outputs, sample analyses

### 6. Version Control Project Agents

```bash
# Include in your repository
.claude/
  agents/
    code-reviewer.md
    api-designer.md
    database-migrator.md
```

**Benefits**:
- Team alignment
- Consistent standards
- Evolution tracking
- Onboarding new developers

### 7. Iterate Based on Results

Agents improve with feedback:

1. **Monitor Performance**: Which agents produce good results?
2. **Refine Instructions**: Update prompts based on common mistakes
3. **Adjust Scope**: Narrow or expand responsibilities
4. **Update Triggers**: Improve description for better auto-delegation

### 8. Combine with Other Claude Code Features

**Agents + Hooks**: Automatically run code review on pre-commit
**Agents + Skills**: Package agent invocation as reusable skills
**Agents + Slash Commands**: Create shortcuts for common agent tasks

### 9. Document Your Agents

Add a README in your agents directory:

```markdown
# Project Agents

## code-reviewer
Reviews all code changes for quality, security, and standards compliance.
Use after feature development or explicitly request review.

## spark-expert
Specializes in Spark and PySpark optimization and debugging.
Use for Spark-related issues or performance reviews.

## api-designer
Ensures API endpoints follow REST principles and project patterns.
Use when designing new endpoints or refactoring APIs.
```

### 10. Start with Built-in Agents

Before creating custom agents, leverage built-ins:
- **Explore**: Codebase navigation and understanding
- **Plan**: Task breakdown and research
- **General-purpose**: Complex multi-step tasks

Only create custom agents when you have specific, recurring needs.

---

## Advanced Topics

### Agent Chaining

Agents can invoke other agents for complex workflows:

**Example: Complete Feature Development**
```
You: "Implement user notification preferences feature"

Claude: [Uses Plan agent to break down task]
Plan agent:
  1. Design database schema
  2. Create API endpoints
  3. Implement business logic
  4. Add tests
  5. Review and deploy

Claude: [Executes each step, using specialized agents]
  - database-expert for schema design
  - api-designer for endpoint structure
  - test-generator for test creation
  - code-reviewer for final review
```

### Resumable Agents

Continue multi-turn conversations with agents:

```
You: "Start analyzing performance bottlenecks"
[Agent begins analysis, returns initial findings]

You: "Resume that analysis and focus on the database queries"
[Agent continues from where it left off]
```

**Use Cases**:
- Long-running investigations
- Iterative refinement
- Multi-session debugging

### Dynamic Agent Selection

Claude can choose between multiple similar agents:

**Example**: Different review levels
- `quick-reviewer.md`: Fast, high-level check
- `thorough-reviewer.md`: Deep, comprehensive analysis
- `security-reviewer.md`: Security-focused only

Claude selects based on your request:
```
"Quick review of my changes" → quick-reviewer
"Comprehensive security audit" → security-reviewer
"Full code review before production" → thorough-reviewer
```

### Integrating Agents with Skills

Skills can leverage agents for specialized capabilities:

**Example Skill**: `api-review` skill that invokes api-designer agent

```markdown
# API Review Skill

When reviewing API endpoints, I will:
1. Use the api-designer agent to analyze endpoint structure
2. Check against REST principles
3. Validate request/response schemas
4. Verify error handling patterns
```

### Custom Agent Tools

While agents inherit Claude Code's built-in tools, you can guide their tool usage:

```markdown
## Tool Usage Guidelines

- **Read**: Always read the full file before suggesting changes
- **Grep**: Search with context (-C 3) for better understanding
- **Bash**: Use for running tests, checking git history
- **Edit**: Make surgical, minimal changes
- **Never use Write**: Only edit existing files, never create new ones
```

### Metrics and Monitoring

Track agent effectiveness:

1. **Success Rate**: Do agents complete tasks correctly?
2. **Efficiency**: Are they faster than manual work?
3. **Consistency**: Do they apply standards uniformly?
4. **User Satisfaction**: Do they provide value to the team?

Refine agents based on these metrics.

### Multi-Agent Patterns

**Parallel Agents**: Independent simultaneous tasks
```
You: "Review the API module and run the full test suite"
Claude: [Invokes code-reviewer AND test-runner in parallel]
```

**Sequential Agents**: Dependent pipeline
```
You: "Fix the failing tests and then deploy"
Claude:
  1. debugger agent fixes tests
  2. test-runner agent verifies fixes
  3. deployment agent deploys
```

**Nested Agents**: Agents invoking agents
```
integration-tester agent:
  1. Invokes api-designer to verify endpoint structure
  2. Invokes test-generator to create integration tests
  3. Invokes test-runner to execute tests
  4. Reports results
```

---

## Conclusion

Agents are a powerful productivity multiplier in Claude Code. They enable:

- **Specialization**: Expert assistance for specific domains
- **Automation**: Consistent, repeatable task execution
- **Efficiency**: Faster workflows with better results
- **Collaboration**: Shared standards and practices across teams

### Getting Started

1. **Experiment with Built-ins**: Use Explore and Plan agents to understand the concept
2. **Create Your First Agent**: Start with a simple code-reviewer
3. **Iterate and Refine**: Improve based on real usage
4. **Build a Library**: Develop agents for your common workflows
5. **Share with Team**: Version control and distribute project agents

### Next Steps

- Explore the [Claude Code documentation](https://code.claude.com/docs) for more details
- Check out [Skills](https://code.claude.com/docs/en/skills.md) for complementary capabilities
- Learn about [Hooks](https://code.claude.com/docs/en/hooks.md) for automation
- Join the community to share and discover agents

---

**Happy coding with agents!** 🚀
