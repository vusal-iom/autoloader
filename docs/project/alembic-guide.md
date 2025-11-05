# Alembic Database Migrations Guide

A comprehensive guide to understanding Alembic and how it's used in the IOMETE Autoloader project.

## Table of Contents

1. [What is Alembic?](#what-is-alembic)
2. [Why Use Alembic?](#why-use-alembic)
3. [Core Concepts](#core-concepts)
4. [How Alembic Works](#how-alembic-works)
5. [Alembic in This Project](#alembic-in-this-project)
6. [Common Workflows](#common-workflows)
7. [Migration File Anatomy](#migration-file-anatomy)
8. [Best Practices](#best-practices)
9. [Advanced Topics](#advanced-topics)
10. [Troubleshooting](#troubleshooting)
11. [Reference Commands](#reference-commands)

---

## What is Alembic?

**Alembic** is a lightweight database migration tool for SQLAlchemy. It allows you to:

- **Track database schema changes** over time
- **Version control your database schema** alongside your code
- **Apply schema changes** in a controlled, repeatable way
- **Roll back changes** if needed
- **Share schema changes** with team members

Think of Alembic as **Git for your database schema**. Just like Git tracks changes to your code, Alembic tracks changes to your database structure.

### The Problem Alembic Solves

**Without Alembic:**
```python
# app/database.py
from sqlalchemy import create_engine
from app.models import Base

engine = create_engine("postgresql://...")

# This creates ALL tables based on current code
# But what if the database already has old schema?
# What if columns were renamed?
# What if you need to preserve existing data?
Base.metadata.create_all(engine)  # ❌ Too simplistic
```

**Issues:**
- ❌ No history of schema changes
- ❌ Can't track what changed when
- ❌ Can't safely update production databases
- ❌ Can't roll back changes
- ❌ Hard to coordinate changes across team/environments

**With Alembic:**
```bash
# Create a migration for new changes
alembic revision --autogenerate -m "add processed_files table"

# Review the generated migration
# Edit if needed (add data migrations, custom logic)

# Apply to database
alembic upgrade head

# Roll back if needed
alembic downgrade -1
```

**Benefits:**
- ✅ Every schema change is versioned
- ✅ Changes are repeatable and testable
- ✅ Can upgrade/downgrade any environment
- ✅ Team members get same schema
- ✅ Safe for production deployments

---

## Why Use Alembic?

### 1. **Version Control for Database Schema**

Your database schema evolves with your application. Alembic creates a migration file for each change:

```
alembic/versions/
├── 001_initial_schema.py          # First version
├── 002_add_user_email.py           # Added email column
├── 003_add_processed_files.py      # Added new table
└── 004_add_index_to_status.py      # Performance improvement
```

Each file is versioned and tracked in Git with your code.

### 2. **Repeatability**

The same migration files work on:
- Your local development database
- Your teammate's database
- CI/CD test databases
- Staging environment
- Production environment

### 3. **Safety**

Alembic keeps track of which migrations have been applied:

```sql
-- Alembic creates this table automatically
SELECT * FROM alembic_version;

 version_num
-------------
 67371aeb5ae0  -- Current schema version
```

It won't re-apply migrations that are already applied, preventing duplicate column errors.

### 4. **Data Preservation**

Unlike `Base.metadata.create_all()`, Alembic can:
- Add columns with default values
- Rename columns while preserving data
- Migrate data between columns
- Perform complex schema changes safely

---

## Core Concepts

### 1. **Migration (Revision)**

A **migration** (also called a **revision**) is a Python file that describes:
- **What changed** in the database schema
- **How to upgrade** (apply the change)
- **How to downgrade** (undo the change)

Example migration file name:
```
67371aeb5ae0_initial_migration_with_processed_files.py
    ↑           ↑
    |           └─ Human-readable description
    └─ Unique revision ID (auto-generated)
```

### 2. **Upgrade**

An **upgrade** function applies the schema change:

```python
def upgrade() -> None:
    # Add new table
    op.create_table(
        'processed_files',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('file_path', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
```

When you run `alembic upgrade head`, Alembic executes all pending upgrade functions.

### 3. **Downgrade**

A **downgrade** function reverses the change:

```python
def downgrade() -> None:
    # Remove the table we added
    op.drop_table('processed_files')
```

When you run `alembic downgrade -1`, Alembic executes the downgrade function.

### 4. **Revision Chain**

Migrations form a linked list (chain):

```
None → [001] → [002] → [003] → [004] → HEAD
        ↑       ↑       ↑       ↑       ↑
      Initial  Add     Add    Add    Current
      schema   email   table  index  version
```

Each migration knows:
- `revision`: Its own ID
- `down_revision`: The previous migration's ID

### 5. **Autogenerate**

Alembic can **automatically detect** schema changes by comparing:
- **Your SQLAlchemy models** (the desired schema)
- **The current database** (the actual schema)

```bash
alembic revision --autogenerate -m "add user email"
```

Alembic generates migration code for the differences it finds.

### 6. **Alembic Version Table**

Alembic tracks which migrations have been applied in a special table:

```sql
-- This table is created automatically
CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL PRIMARY KEY
);

-- Current version is stored here
INSERT INTO alembic_version VALUES ('67371aeb5ae0');
```

This ensures migrations are applied exactly once.

---

## How Alembic Works

### Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   Your Application                      │
│                                                          │
│  app/models/domain.py                                   │
│  ┌──────────────────────────────────────────┐           │
│  │ class ProcessedFile(Base):               │           │
│  │     __tablename__ = "processed_files"    │           │
│  │     id = Column(String, primary_key=True)│           │
│  │     file_path = Column(String, ...)      │           │
│  └──────────────────────────────────────────┘           │
│                        ↓                                 │
│                  SQLAlchemy Base.metadata               │
│                  (desired schema)                        │
└─────────────────────────────────────────────────────────┘
                           ↓
                           ↓ Alembic compares
                           ↓
┌─────────────────────────────────────────────────────────┐
│                   Alembic                                │
│                                                          │
│  alembic/env.py                                         │
│  ┌──────────────────────────────────────────┐           │
│  │ target_metadata = Base.metadata          │←─ Models  │
│  │                                           │           │
│  │ config.set_main_option(                  │           │
│  │     "sqlalchemy.url",                    │           │
│  │     settings.database_url                │←─ Config  │
│  │ )                                         │           │
│  └──────────────────────────────────────────┘           │
│                        ↓                                 │
│  alembic revision --autogenerate                        │
│                        ↓                                 │
│  alembic/versions/67371aeb5ae0_....py                   │
│  ┌──────────────────────────────────────────┐           │
│  │ def upgrade():                            │           │
│  │     op.create_table('processed_files',..│           │
│  │                                           │           │
│  │ def downgrade():                          │           │
│  │     op.drop_table('processed_files')     │           │
│  └──────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
                           ↓
                           ↓ alembic upgrade head
                           ↓
┌─────────────────────────────────────────────────────────┐
│                   Database                               │
│                                                          │
│  ┌──────────────────────────────────────────┐           │
│  │ Table: processed_files                   │           │
│  │  - id (varchar)                          │           │
│  │  - file_path (varchar)                   │           │
│  │  - ...                                    │           │
│  └──────────────────────────────────────────┘           │
│  ┌──────────────────────────────────────────┐           │
│  │ Table: alembic_version                   │           │
│  │  - version_num = '67371aeb5ae0'         │           │
│  └──────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

### Step-by-Step Process

#### When You Add a New Model:

1. **You modify code:**
   ```python
   # app/models/domain.py
   class ProcessedFile(Base):
       __tablename__ = "processed_files"
       id = Column(String, primary_key=True)
       file_path = Column(String, nullable=False)
   ```

2. **You run autogenerate:**
   ```bash
   alembic revision --autogenerate -m "add processed_files table"
   ```

3. **Alembic compares:**
   - **Models** (Python code): Has `ProcessedFile` table
   - **Database** (actual schema): Doesn't have this table yet
   - **Difference detected:** Need to create table

4. **Alembic generates migration:**
   ```python
   # alembic/versions/abc123_add_processed_files_table.py
   def upgrade():
       op.create_table(
           'processed_files',
           sa.Column('id', sa.String(), nullable=False),
           sa.Column('file_path', sa.String(), nullable=False),
           sa.PrimaryKeyConstraint('id')
       )
   ```

5. **You review and edit if needed:**
   - Check the generated code
   - Add data migrations if needed
   - Adjust column types/constraints

6. **You apply migration:**
   ```bash
   alembic upgrade head
   ```

7. **Alembic executes:**
   - Runs `upgrade()` function
   - Creates the `processed_files` table
   - Updates `alembic_version` table

#### When Applying to Another Environment:

On a teammate's machine or staging server:

1. **Pull latest code** (includes migration files)
   ```bash
   git pull origin main
   ```

2. **Apply migrations:**
   ```bash
   alembic upgrade head
   ```

3. **Alembic checks:**
   - Current version in `alembic_version`: `001_initial`
   - Available migrations: `001`, `002`, `003`, `004`
   - Migrations to apply: `002`, `003`, `004`

4. **Alembic applies** each upgrade function in order

---

## Alembic in This Project

### Project Structure

```
autoloader/
├── alembic/                          # Alembic configuration
│   ├── versions/                     # Migration files
│   │   └── 67371aeb5ae0_initial_migration_with_processed_files.py
│   ├── env.py                        # Alembic environment setup
│   ├── README                        # Alembic documentation
│   └── script.py.mako               # Template for new migrations
├── alembic.ini                       # Alembic configuration file
├── app/
│   ├── models/
│   │   └── domain.py                # SQLAlchemy models (source of truth)
│   ├── database.py                  # Database setup
│   └── config.py                    # Application configuration
└── .env                             # Environment variables
```

### Configuration Files

#### 1. `alembic.ini` - Main Configuration

```ini
[alembic]
# Where migration scripts are stored
script_location = alembic

# Database URL (overridden by env.py)
sqlalchemy.url = driver://user:pass@localhost/dbname

# Logging configuration
[loggers]
keys = root,sqlalchemy,alembic

[logger_alembic]
level = INFO
```

**Key Settings:**
- `script_location`: Where to store migration files (`alembic/` directory)
- `sqlalchemy.url`: Database connection string (we override this in `env.py`)

#### 2. `alembic/env.py` - Environment Configuration

This is the **most important file**. It tells Alembic how to connect to your database and what models to track.

```python
# Import app configuration and models
from app.config import get_settings
from app.database import Base
from app.models.domain import Ingestion, Run, SchemaVersion, ProcessedFile

# Get database URL from app configuration
settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.database_url)

# Tell Alembic about your models
target_metadata = Base.metadata
```

**What This Does:**

1. **Imports all SQLAlchemy models** so Alembic can see their structure
2. **Gets database URL from app settings** (`config.py`) instead of hardcoded `alembic.ini`
3. **Sets `target_metadata`** to `Base.metadata` - this is the source of truth for autogenerate

**Why This Matters:**

- ✅ Uses same database URL as your application
- ✅ Respects environment variables (`DATABASE_URL`)
- ✅ Automatically discovers all models that inherit from `Base`
- ✅ Works with different environments (dev, test, prod)

### How Database URL is Resolved

```
1. Environment Variable
   ↓
   DATABASE_URL="postgresql://user:pass@localhost/mydb"
   ↓
2. app/config.py
   ↓
   class Settings:
       database_url: str = "sqlite:///./autoloader.db"  # default
   ↓
3. alembic/env.py
   ↓
   settings = get_settings()  # Reads from env or defaults
   config.set_main_option("sqlalchemy.url", settings.database_url)
   ↓
4. Alembic uses this URL for all operations
```

**Examples:**

```bash
# Development (default)
DATABASE_URL=sqlite:///./autoloader.db

# Testing
DATABASE_URL=postgresql://test_user:test_password@localhost:5432/autoloader_test

# Production
DATABASE_URL=postgresql://prod_user:prod_password@prod-db.example.com:5432/autoloader_prod
```

### Model Registration

**Important:** All models must be imported in `alembic/env.py`:

```python
# alembic/env.py
from app.models.domain import Ingestion, Run, SchemaVersion, ProcessedFile
```

**Why?** Alembic uses `Base.metadata` to discover tables. If you don't import a model, SQLAlchemy doesn't know it exists, so Alembic won't track it.

**What Happens If You Forget?**

```python
# ❌ New model created but not imported in env.py
class NewModel(Base):
    __tablename__ = "new_table"
    id = Column(String, primary_key=True)

# Running autogenerate
$ alembic revision --autogenerate -m "add new table"
INFO  [alembic.autogenerate] Detected no changes
# ❌ No migration created!
```

**Solution:** Always import new models in `alembic/env.py`:

```python
# ✅ Import the new model
from app.models.domain import Ingestion, Run, SchemaVersion, ProcessedFile, NewModel
```

---

## Common Workflows

### Workflow 1: Creating Your First Migration

**Scenario:** You just cloned the project and need to set up the database.

```bash
# 1. Set database URL
export DATABASE_URL="postgresql://user:pass@localhost/mydb"

# 2. Apply all migrations
alembic upgrade head

# 3. Verify
alembic current
# Output: 67371aeb5ae0 (head)
```

### Workflow 2: Adding a New Column

**Scenario:** You need to add an `error_count` column to the `runs` table.

**Step 1: Modify the model**

```python
# app/models/domain.py
class Run(Base):
    __tablename__ = "runs"

    id = Column(String, primary_key=True)
    # ... existing columns ...

    # Add new column
    error_count = Column(Integer, nullable=True, default=0)
```

**Step 2: Generate migration**

```bash
alembic revision --autogenerate -m "add error_count to runs"
```

Output:
```
INFO  [alembic.autogenerate.compare] Detected added column 'runs.error_count'
Generating /path/to/alembic/versions/abc123_add_error_count_to_runs.py ... done
```

**Step 3: Review the generated migration**

```python
# alembic/versions/abc123_add_error_count_to_runs.py
def upgrade() -> None:
    op.add_column('runs', sa.Column('error_count', sa.Integer(), nullable=True))

def downgrade() -> None:
    op.drop_column('runs', 'error_count')
```

**Step 4: Apply migration**

```bash
alembic upgrade head
```

Output:
```
INFO  [alembic.runtime.migration] Running upgrade 67371aeb5ae0 -> abc123, add error_count to runs
```

**Step 5: Verify**

```bash
# Check current version
alembic current

# Or check database directly
psql -d mydb -c "\d runs"
# Should show error_count column
```

### Workflow 3: Adding a New Table

**Scenario:** You implemented Phase 2 and need to add `retry_policies` table.

**Step 1: Create the model**

```python
# app/models/domain.py
class RetryPolicy(Base):
    __tablename__ = "retry_policies"

    id = Column(String, primary_key=True)
    ingestion_id = Column(String, ForeignKey("ingestions.id"))
    max_retries = Column(Integer, nullable=False, default=3)
    backoff_multiplier = Column(Float, nullable=False, default=2.0)
```

**Step 2: Import in env.py**

```python
# alembic/env.py
from app.models.domain import (
    Ingestion, Run, SchemaVersion, ProcessedFile,
    RetryPolicy  # ← Add this
)
```

**Step 3: Generate migration**

```bash
alembic revision --autogenerate -m "add retry_policies table"
```

**Step 4: Review and apply**

```bash
# Review the generated file
cat alembic/versions/xyz789_add_retry_policies_table.py

# Apply
alembic upgrade head
```

### Workflow 4: Renaming a Column

**Scenario:** Rename `source_credentials` to `credentials_json` in `ingestions` table.

**⚠️ Warning:** Autogenerate will detect this as drop + add, losing data!

```python
# ❌ Alembic will generate this (WRONG):
def upgrade():
    op.drop_column('ingestions', 'source_credentials')
    op.add_column('ingestions', sa.Column('credentials_json', ...))
    # DATA LOST!
```

**✅ Correct Approach:** Manually edit the migration:

**Step 1: Generate migration**

```bash
alembic revision -m "rename source_credentials to credentials_json"
# Note: NO --autogenerate (we'll write manually)
```

**Step 2: Manually write migration**

```python
# alembic/versions/def456_rename_source_credentials.py
def upgrade() -> None:
    # PostgreSQL
    op.alter_column('ingestions', 'source_credentials',
                    new_column_name='credentials_json')

    # SQLite (doesn't support ALTER COLUMN, need workaround)
    # with op.batch_alter_table('ingestions') as batch_op:
    #     batch_op.alter_column('source_credentials',
    #                          new_column_name='credentials_json')

def downgrade() -> None:
    op.alter_column('ingestions', 'credentials_json',
                    new_column_name='source_credentials')
```

**Step 3: Apply**

```bash
alembic upgrade head
```

### Workflow 5: Data Migration

**Scenario:** Split `name` column into `first_name` and `last_name`.

```python
# alembic/versions/ghi789_split_name_column.py
def upgrade() -> None:
    # Step 1: Add new columns
    op.add_column('users', sa.Column('first_name', sa.String(), nullable=True))
    op.add_column('users', sa.Column('last_name', sa.String(), nullable=True))

    # Step 2: Migrate data
    connection = op.get_bind()
    connection.execute("""
        UPDATE users
        SET first_name = split_part(name, ' ', 1),
            last_name = split_part(name, ' ', 2)
        WHERE name IS NOT NULL
    """)

    # Step 3: Drop old column
    op.drop_column('users', 'name')

def downgrade() -> None:
    # Reverse the process
    op.add_column('users', sa.Column('name', sa.String(), nullable=True))

    connection = op.get_bind()
    connection.execute("""
        UPDATE users
        SET name = first_name || ' ' || last_name
        WHERE first_name IS NOT NULL
    """)

    op.drop_column('users', 'last_name')
    op.drop_column('users', 'first_name')
```

### Workflow 6: Rolling Back a Migration

**Scenario:** You applied a migration but need to undo it.

```bash
# Check current version
alembic current
# Output: abc123 (head)

# See history
alembic history
# Output:
# abc123 -> current (head), add error_count
# 67371aeb5ae0 -> abc123, initial migration

# Rollback one migration
alembic downgrade -1
# Executes downgrade() function of abc123 migration

# Verify
alembic current
# Output: 67371aeb5ae0 (head)

# Re-apply if needed
alembic upgrade head
```

### Workflow 7: Multiple Environments

**Development:**
```bash
DATABASE_URL="sqlite:///./autoloader.db" alembic upgrade head
```

**Testing:**
```bash
DATABASE_URL="postgresql://test_user:test_password@localhost/autoloader_test" alembic upgrade head
```

**Production:**
```bash
DATABASE_URL="postgresql://prod_user:$PASSWORD@prod-db/autoloader" alembic upgrade head
```

---

## Migration File Anatomy

Let's dissect the migration file from this project:

```python
"""initial_migration_with_processed_files

Revision ID: 67371aeb5ae0
Revises:
Create Date: 2025-11-04 13:24:09.349333

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '67371aeb5ae0'           # This migration's ID
down_revision: Union[str, None] = None    # Previous migration (None = first)
branch_labels: Union[str, Sequence[str], None] = None  # For branching
depends_on: Union[str, Sequence[str], None] = None      # Dependencies

def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('ingestions',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('tenant_id', sa.String(), nullable=False),
        # ... more columns ...
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_ingestions_id'), 'ingestions', ['id'], unique=False)
    # ### end Alembic commands ###

def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_ingestions_id'), table_name='ingestions')
    op.drop_table('ingestions')
    # ### end Alembic commands ###
```

### Key Components

#### 1. Header

```python
"""initial_migration_with_processed_files

Revision ID: 67371aeb5ae0
Revises:
Create Date: 2025-11-04 13:24:09.349333
"""
```

- **Description**: Human-readable summary
- **Revision ID**: Unique identifier (auto-generated)
- **Revises**: Previous migration ID (empty = first migration)
- **Create Date**: When migration was generated

#### 2. Revision Identifiers

```python
revision: str = '67371aeb5ae0'
down_revision: Union[str, None] = None
```

- Creates the migration chain
- Alembic uses these to determine order

#### 3. Upgrade Function

```python
def upgrade() -> None:
    op.create_table('ingestions', ...)
    op.create_index(...)
```

**Common operations:**
- `op.create_table()` - Create new table
- `op.drop_table()` - Remove table
- `op.add_column()` - Add column
- `op.drop_column()` - Remove column
- `op.alter_column()` - Modify column
- `op.create_index()` - Add index
- `op.create_foreign_key()` - Add FK
- `op.execute()` - Run raw SQL

#### 4. Downgrade Function

```python
def downgrade() -> None:
    op.drop_index(...)
    op.drop_table('ingestions')
```

Should **reverse** the upgrade operations in **reverse order**:
- Upgrade: Create table → Create index
- Downgrade: Drop index → Drop table

---

## Best Practices

### 1. Always Review Autogenerated Migrations

**Don't blindly trust autogenerate!** Always review before applying.

```bash
# Generate migration
alembic revision --autogenerate -m "add user email"

# ✅ REVIEW the generated file
cat alembic/versions/abc123_add_user_email.py

# Check for:
# - Correct column types
# - Nullable settings
# - Default values
# - Missing indexes
# - Data migrations needed
```

### 2. Test Migrations Before Production

```bash
# 1. Apply to test database
DATABASE_URL="postgresql://...test_db" alembic upgrade head

# 2. Verify schema
psql -d test_db -c "\d table_name"

# 3. Test downgrade
DATABASE_URL="postgresql://...test_db" alembic downgrade -1

# 4. Re-apply
DATABASE_URL="postgresql://...test_db" alembic upgrade head
```

### 3. One Logical Change Per Migration

**❌ Bad:**
```bash
alembic revision -m "add email, rename status, create audit log"
```

**✅ Good:**
```bash
alembic revision -m "add email to users"
alembic revision -m "rename status to state"
alembic revision -m "create audit log table"
```

### 4. Write Reversible Migrations

Always implement `downgrade()`:

```python
def upgrade():
    op.add_column('users', sa.Column('email', sa.String()))

def downgrade():
    op.drop_column('users', 'email')  # ✅ Reversible
```

### 5. Handle Data Migrations Carefully

When modifying columns with data:

```python
def upgrade():
    # 1. Add new column (nullable first)
    op.add_column('users', sa.Column('email', sa.String(), nullable=True))

    # 2. Migrate data
    op.execute("UPDATE users SET email = username || '@example.com' WHERE email IS NULL")

    # 3. Make not nullable
    op.alter_column('users', 'email', nullable=False)

def downgrade():
    op.drop_column('users', 'email')
```

### 6. Use Batch Operations for SQLite

SQLite doesn't support many ALTER operations. Use `batch_alter_table`:

```python
# ❌ Doesn't work with SQLite
op.alter_column('users', 'email', new_column_name='email_address')

# ✅ Works with SQLite
with op.batch_alter_table('users') as batch_op:
    batch_op.alter_column('email', new_column_name='email_address')
```

### 7. Keep Migrations in Version Control

```bash
git add alembic/versions/abc123_add_email.py
git commit -m "Add email column to users table"
git push
```

### 8. Document Complex Migrations

```python
def upgrade() -> None:
    """
    Split the full_name column into first_name and last_name.

    Data migration strategy:
    - Assumes names are in "First Last" format
    - Sets first_name to full name if no space found
    - Preserves original full_name for rollback
    """
    # ... migration code ...
```

### 9. Use Enum Types Correctly

```python
# Define enum
import enum
from sqlalchemy import Enum

class StatusEnum(str, enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

# In migration
def upgrade():
    # Create enum type (PostgreSQL)
    status_enum = sa.Enum('ACTIVE', 'INACTIVE', name='status_type')
    status_enum.create(op.get_bind())

    op.add_column('users', sa.Column('status', status_enum))

def downgrade():
    op.drop_column('users', 'status')
    sa.Enum(name='status_type').drop(op.get_bind())
```

### 10. Avoid Destructive Operations in Upgrade

**Be careful with:**
- `op.drop_table()`
- `op.drop_column()`
- Data transformations that lose information

**Consider:**
- Adding a new column instead of renaming
- Soft deletes instead of hard deletes
- Archiving data before dropping

---

## Advanced Topics

### Branching and Merging

For teams working on multiple features simultaneously:

```bash
# Developer A creates migration
alembic revision -m "add feature A"
# Creates: abc123 (head)

# Developer B (parallel) creates migration
alembic revision -m "add feature B"
# Creates: def456 (head)

# Now you have two heads!
alembic heads
# Output:
# abc123 (feature A)
# def456 (feature B)

# Merge branches
alembic merge abc123 def456 -m "merge A and B"
# Creates: ghi789 (merges abc123 and def456)
```

### Offline SQL Generation

Generate SQL without executing:

```bash
# Generate SQL for all pending migrations
alembic upgrade head --sql > migration.sql

# Review and run manually
cat migration.sql
psql -d mydb -f migration.sql
```

Useful for:
- Production deployments with DBA review
- Environments without direct database access
- Audit trails

### Multiple Databases

If you have separate databases (not just tables):

```bash
# migrations for database 1
DATABASE_URL="postgresql://localhost/db1" alembic upgrade head

# migrations for database 2
DATABASE_URL="postgresql://localhost/db2" alembic upgrade head
```

Or use separate Alembic configurations:

```
alembic-db1.ini → alembic-db1/versions/
alembic-db2.ini → alembic-db2/versions/
```

### Custom Migration Context

Add custom context data in `env.py`:

```python
def run_migrations_online():
    # ... existing code ...

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        # Add custom context
        include_schemas=True,
        compare_type=True,
        compare_server_default=True
    )
```

---

## Troubleshooting

### Problem: "Can't locate revision identified by 'abc123'"

**Cause:** Migration file is missing or not in `alembic/versions/`

**Solution:**
```bash
# Check versions directory
ls alembic/versions/

# Pull missing migrations from git
git pull

# Or regenerate if in development
alembic revision --autogenerate -m "regenerate migration"
```

### Problem: "Multiple head revisions are present"

**Cause:** Two developers created migrations in parallel

**Solution:**
```bash
# See the heads
alembic heads

# Merge them
alembic merge abc123 def456 -m "merge parallel changes"
alembic upgrade head
```

### Problem: "Target database is not up to date"

**Cause:** Database version doesn't match code

**Solution:**
```bash
# Check current version
alembic current

# Check available migrations
alembic history

# Upgrade to latest
alembic upgrade head
```

### Problem: Autogenerate doesn't detect changes

**Causes:**
1. Model not imported in `env.py`
2. Using Base from different import
3. Model doesn't inherit from Base

**Solutions:**
```python
# ✅ Import all models in alembic/env.py
from app.models.domain import Ingestion, Run, ProcessedFile, NewModel

# ✅ Use the correct Base
from app.database import Base

# ✅ Model inherits from Base
class NewModel(Base):
    __tablename__ = "new_table"
```

### Problem: Migration fails halfway

**Scenario:** Migration runs, encounters error, leaves database in inconsistent state

**Solution:**

```bash
# Check current version
alembic current
# May show partial version or old version

# Manually fix database if needed
psql -d mydb

# Mark migration as applied (if you fixed manually)
alembic stamp abc123

# Or rollback and fix migration
alembic downgrade -1
# Fix the migration file
alembic upgrade head
```

### Problem: "Can't drop column 'X' because it doesn't exist"

**Cause:** Trying to downgrade a migration that was never applied

**Solution:**
```bash
# Check what's actually in database
psql -d mydb -c "\d table_name"

# Manually sync alembic_version
alembic stamp head
```

### Problem: SQLite ALTER TABLE limitations

**Cause:** SQLite doesn't support many ALTER operations

**Solution:** Use batch mode:

```python
# Instead of:
op.alter_column('users', 'email', type=sa.String(200))

# Use:
with op.batch_alter_table('users') as batch_op:
    batch_op.alter_column('email', type_=sa.String(200))
```

---

## Reference Commands

### Inspection Commands

```bash
# Show current version
alembic current

# Show migration history
alembic history

# Show history in verbose mode
alembic history --verbose

# Show head(s)
alembic heads

# Show all branches
alembic branches

# Show pending migrations
alembic history --verbose | grep "(head)"
```

### Migration Management

```bash
# Create new migration (manual)
alembic revision -m "description"

# Create migration (autogenerate)
alembic revision --autogenerate -m "description"

# Upgrade to specific version
alembic upgrade abc123

# Upgrade to latest
alembic upgrade head

# Upgrade by N steps
alembic upgrade +2

# Downgrade to specific version
alembic downgrade abc123

# Downgrade by N steps
alembic downgrade -1

# Downgrade to base (empty database)
alembic downgrade base
```

### Advanced Commands

```bash
# Mark current database as version X (without running migrations)
alembic stamp abc123

# Merge two head branches
alembic merge abc123 def456 -m "merge branches"

# Show SQL without executing
alembic upgrade head --sql

# Upgrade with SQL output to file
alembic upgrade head --sql > migration.sql

# Edit existing migration
alembic edit abc123
```

### Environment-Specific

```bash
# Use different config file
alembic -c alembic-prod.ini upgrade head

# Override database URL
DATABASE_URL="postgresql://..." alembic upgrade head

# Use environment variable
export DATABASE_URL="postgresql://user:pass@localhost/db"
alembic upgrade head
```

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Create manual migration | `alembic revision -m "description"` |
| Create auto migration | `alembic revision --autogenerate -m "description"` |
| Apply all migrations | `alembic upgrade head` |
| Rollback one migration | `alembic downgrade -1` |
| Check current version | `alembic current` |
| See history | `alembic history` |
| Mark database as version X | `alembic stamp abc123` |
| Show SQL (don't execute) | `alembic upgrade head --sql` |
| Merge branches | `alembic merge abc123 def456 -m "merge"` |
| Reset to beginning | `alembic downgrade base` |

---

## Summary

**Alembic** is essential for:
- ✅ Tracking database schema changes over time
- ✅ Safely upgrading production databases
- ✅ Collaborating with team on schema changes
- ✅ Rolling back problematic changes
- ✅ Maintaining consistency across environments

**Key Takeaways:**
1. **Models are source of truth** - Define schema in SQLAlchemy models
2. **Autogenerate detects changes** - But always review before applying
3. **Migrations form a chain** - Each migration builds on previous
4. **Test before production** - Apply to test environment first
5. **Reversible is better** - Always implement `downgrade()`

**In This Project:**
- Configuration in `alembic/env.py` uses app settings
- Database URL from environment variables
- All models imported for autogenerate
- Migrations in `alembic/versions/`
- Track version in `alembic_version` table

---

## Further Reading

- **Official Documentation**: https://alembic.sqlalchemy.org/
- **SQLAlchemy Docs**: https://docs.sqlalchemy.org/
- **Migration Tutorial**: https://alembic.sqlalchemy.org/en/latest/tutorial.html
- **Cookbook**: https://alembic.sqlalchemy.org/en/latest/cookbook.html

**Project-Specific Docs:**
- `../running-tests-in-pycharm.md` - Running tests with migrations
- `../batch-processing/phase1-implementation-summary.md` - Phase 1 schema

---

**Happy Migrating!** 🚀
