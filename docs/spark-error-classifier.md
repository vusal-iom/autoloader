# Spark Error Classifier

## Overview

The `SparkErrorClassifier` is a utility class that transforms verbose Spark/Hadoop/Py4J exceptions into user-friendly, categorized error messages. It's designed to be **independent** from the rest of the codebase and can be used from multiple places.

**Location:** `app/spark/spark_error_classifier.py`
**Tests:** `tests/integration/spark/test_spark_error_classifier.py`

## Problem Statement

Spark errors are notoriously verbose and technical. A simple "file not found" error can produce a multi-page stack trace with JVM internals, Hadoop filesystem details, and gRPC transport noise. Users shouldn't need to parse this to understand what went wrong.

**Before (raw Spark error):**
```
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 7665.0
failed 1 times, most recent failure: Lost task 0.0 in stage 7665.0 (TID 7990) (e422649a3213
executor driver): org.apache.spark.SparkException: Malformed records are detected in schema
inference. Parse Mode: FAILFAST.
    at org.apache.spark.sql.errors.QueryExecutionErrors$.malformedRecordsDetectedInSchemaInferenceError...
    [50+ more lines of stack trace]
```

**After (classified error):**
```python
FileProcessingError(
    category=FileErrorCategory.DATA_MALFORMED,
    retryable=False,
    user_message="Malformed data encountered. Fix input file or adjust reader mode.",
    raw_error="<original error preserved for debugging>"
)
```

## Architecture

The classifier uses a **tiered approach**, checking from most reliable to least reliable:

```
┌─────────────────────────────────────────────────────────┐
│ TIER 1: Spark ErrorClass (Most Stable)                  │
│ - Uses getErrorClass() API when available               │
│ - Examples: PATH_NOT_FOUND, MALFORMED_RECORD            │
└─────────────────────────────────────────────────────────┘
                          ↓ if not matched
┌─────────────────────────────────────────────────────────┐
│ TIER 2: JVM Exception Types                             │
│ - AnalysisException, NumberFormatException              │
│ - Python exception isinstance() checks                  │
└─────────────────────────────────────────────────────────┘
                          ↓ if not matched
┌─────────────────────────────────────────────────────────┐
│ TIER 3: Hadoop Canonical Exceptions                     │
│ - NoSuchBucket, UnknownStoreException                   │
│ - FileNotFoundException, "does not exist"               │
└─────────────────────────────────────────────────────────┘
                          ↓ if not matched
┌─────────────────────────────────────────────────────────┐
│ TIER 4: Format-Level Patterns                           │
│ - Malformed markers (JSON/CSV/Parquet)                  │
│ - Format option validation errors                       │
└─────────────────────────────────────────────────────────┘
                          ↓ if not matched
┌─────────────────────────────────────────────────────────┐
│ TIER 5: Fallback (UNKNOWN)                              │
│ - Catch-all for unrecognized errors                     │
└─────────────────────────────────────────────────────────┘
```

## Error Categories (will be extended as we work)

Defined in `app/services/batch/errors.py`:

| Category | Description | Retryable |
|----------|-------------|-----------|
| `BUCKET_NOT_FOUND` | S3/GCS/Azure bucket doesn't exist | No |
| `PATH_NOT_FOUND` | File or directory doesn't exist | No |
| `DATA_MALFORMED` | Corrupt or invalid file content | No |
| `FORMAT_OPTIONS_INVALID` | Bad reader options (e.g., samplingRatio="abc") | No |
| `UNKNOWN` | Unrecognized error (fallback) | No |

## Usage

```python
from app.spark.spark_error_classifier import SparkErrorClassifier

try:
    spark.read.json("s3a://bucket/path.json").collect()
except Exception as exc:
    error = SparkErrorClassifier.classify(exc, file_path="s3a://bucket/path.json")

    # User-friendly message
    print(error.user_message)  # "Source path not found."

    # Category for programmatic handling
    if error.category == FileErrorCategory.PATH_NOT_FOUND:
        # handle missing file

    # Original error preserved for debugging
    logger.debug(error.raw_error)
```

 
## Design Decisions

1. **Independent utility** - No dependencies on other services, can be used anywhere
2. **Preserve raw error** - Always keep original for debugging
3. **Tier priority** - More stable APIs checked first (ErrorClass > isinstance > string matching)
4. **Conservative fallback** - Unknown errors are NOT retryable by default
5. **User-first messages** - Messages actionable, not technical

## Future Improvements

- [ ] Add more Spark ErrorClass mappings as Spark adds them
- [ ] Consider adding `PERMISSION_DENIED` category for auth errors
- [ ] Consider adding `SCHEMA_MISMATCH` category for schema evolution errors
- [ ] Add retryable errors (e.g., transient network failures)
- [ ] Unit tests for classifier logic (mock exceptions, no Spark needed)
