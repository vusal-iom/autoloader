# Overwrite Mode Alternative - Manual Full Refresh Guide

**Date:** 2025-01-05
**Status:** Official Workaround
**Decision:** Overwrite mode is NOT implemented in IOMETE Autoloader

---

## Executive Summary

IOMETE Autoloader **does not support automatic overwrite mode**. This document explains why and provides manual alternatives for achieving full table refresh behavior.

**TL;DR:**
- **Append mode** is the only supported write mode (incremental ingestion)
- **Overwrite-like behavior** can be achieved manually:
  - **Option A**: Delete table only → processes NEW files
  - **Option B**: Delete table + call API → reprocesses ALL files

---

## Why Overwrite Mode Is Not Implemented

### Decision Rationale

We decided **NOT to implement automatic overwrite mode** for the following reasons:

1. **Safety: Prevents Dangerous Hidden Operations**
   - A simple `write_mode: "overwrite"` flag could silently reprocess ALL files
   - For large datasets (thousands of files, terabytes of data), this is a dangerous operation
   - Hidden behavior could trigger massive costs without user awareness
   - No accidental full reprocesses from configuration mistakes

2. **Explicit Control and Transparency**
   - Manual approach forces users to consciously decide: "Do I want to reprocess ALL files?"
   - Two explicit steps (drop table + clear processed files) make intentions clear
   - Users see exactly what will happen before triggering the operation
   - No surprises or unexpected behavior

3. **Easy Manual Alternative**
   - The manual workaround is straightforward and well-documented
   - Two API calls achieve the same result with full visibility
   - Can be easily automated via scripts for recurring snapshot refreshes
   - Gives users flexibility to choose when and how to reprocess

4. **Cost Transparency**
   - Re-processing ALL files on every run is expensive
   - For large datasets, this could be prohibitively costly
   - Manual approach makes users aware of the cost implications
   - Incremental (append) mode is more cost-effective for 95% of use cases

5. **Semantic Clarity**
   - Files in cloud storage are immutable
   - "Overwrite" is confusing: "I added new files but overwrite deleted old data?"
   - Manual approach eliminates ambiguity
   - Clear separation: file tracking vs. table data

6. **Merge Mode Is Better for Most Use Cases**
   - Most "overwrite" needs are actually upserts
   - "Update existing records, insert new ones"
   - Merge mode (when implemented) handles this better
   - True snapshot refreshes are rare and should be explicit

---

## Manual Alternatives

### Option A: Delete Table, Process New Files Only

**Use Case:** You want a fresh start but only process newly added files going forward.

**Steps:**

1. **Delete the target table** (using your data platform or SQL):
   ```sql
   DROP TABLE your_catalog.your_database.your_table;
   ```

2. **Trigger the next ingestion run** (manual or scheduled):
   ```bash
   curl -X POST "https://your-api.com/api/v1/ingestions/{ingestion_id}/run"
   ```

**Result:**
- Autoloader creates a new empty table
- Only processes files that were **added since the last processed file** (incremental)
- Existing files remain "processed" and are skipped

**When to Use:**
- Starting fresh with a new table structure
- You only want to process new data going forward
- Historical data is no longer needed

---

### Option B: Delete Table + Clear Processed Files (Full Reprocess)

**Use Case:** You want to reprocess ALL files from scratch (true "overwrite" behavior).

**Steps:**

1. **Delete the target table**:
   ```sql
   DROP TABLE your_catalog.your_database.your_table;
   ```

2. **Clear processed file history** (via API):
   ```bash
   curl -X DELETE "https://your-api.com/api/v1/ingestions/{ingestion_id}/processed-files" \
     -H "Authorization: Bearer YOUR_TOKEN"
   ```

   **Response:**
   ```json
   {
     "message": "Cleared 127 processed file records",
     "files_cleared": 127,
     "ingestion_id": "ingestion-abc123"
   }
   ```

3. **Trigger the next ingestion run**:
   ```bash
   curl -X POST "https://your-api.com/api/v1/ingestions/{ingestion_id}/run"
   ```

**Result:**
- Autoloader creates a new empty table
- Processes **ALL files** in the source (s3://bucket/path/*)
- Full refresh from scratch

**When to Use:**
- Data quality issues: need to reprocess everything with corrected logic
- Source files were updated/corrected retroactively
- You want a complete snapshot refresh from all source files
- Testing: want to replay ingestion from the beginning

---

## Comparison Table

| Scenario | Delete Table? | Clear Processed Files? | Files Processed | Use Case |
|----------|---------------|------------------------|-----------------|----------|
| **Fresh start, new files only** | ✅ Yes | ❌ No | Only NEW files since last run | Fresh start, skip old data |
| **Full reprocess (true overwrite)** | ✅ Yes | ✅ Yes | ALL files in source | Data fixes, complete refresh |
| **Keep table, reprocess files** | ❌ No | ✅ Yes | ALL files (appends to table) | Backfill missing data |
| **Normal incremental (default)** | ❌ No | ❌ No | Only NEW files | Standard incremental ingestion |

---

## API Reference

### Clear Processed Files Endpoint

**Endpoint:** `DELETE /api/v1/ingestions/{ingestion_id}/processed-files`

**Description:** Clears all processed file tracking records for an ingestion, allowing reprocessing from scratch.

**Authentication:** Required

**Path Parameters:**
- `ingestion_id` (string, required): The ingestion ID

**Response:**
```json
{
  "message": "Cleared 127 processed file records",
  "files_cleared": 127,
  "ingestion_id": "ingestion-abc123"
}
```

**Error Responses:**
- `404 Not Found`: Ingestion does not exist
- `401 Unauthorized`: Missing or invalid authentication
- `403 Forbidden`: User does not have permission to modify this ingestion

**Notes:**
- Does NOT delete the ingestion configuration
- Does NOT delete the target table
- Does NOT affect currently running ingestions
- Safe to call multiple times (idempotent)

---

## Step-by-Step Examples

### Example 1: Full Snapshot Refresh (Daily Dimension Table)

**Scenario:** You have a daily snapshot of users from an external system. Every day, you want to replace the entire table with the latest snapshot.

**Solution:**

```bash
#!/bin/bash
# Daily snapshot refresh script

INGESTION_ID="users-snapshot-ingestion"
API_BASE="https://your-api.com/api/v1"
TOKEN="your_auth_token"

# Step 1: Drop the table
psql -c "DROP TABLE IF EXISTS analytics.dim_users;"

# Step 2: Clear processed files
curl -X DELETE "$API_BASE/ingestions/$INGESTION_ID/processed-files" \
  -H "Authorization: Bearer $TOKEN"

# Step 3: Trigger ingestion (processes all files)
curl -X POST "$API_BASE/ingestions/$INGESTION_ID/run" \
  -H "Authorization: Bearer $TOKEN"

echo "Snapshot refresh initiated"
```

**Cron schedule:** `0 2 * * *` (every day at 2 AM)

---

### Example 2: Data Quality Fix

**Scenario:** You discovered bad data due to a bug. Source files have been corrected. You need to reprocess everything.

**Solution:**

```bash
#!/bin/bash
# One-time reprocessing after data quality fix

INGESTION_ID="sales-data-ingestion"
API_BASE="https://your-api.com/api/v1"
TOKEN="your_auth_token"

echo "WARNING: This will reprocess ALL files. Continue? (y/n)"
read -r response

if [ "$response" = "y" ]; then
  # Step 1: Drop the table
  echo "Dropping table..."
  psql -c "DROP TABLE IF EXISTS raw.sales_transactions;"

  # Step 2: Clear processed files
  echo "Clearing processed file history..."
  curl -X DELETE "$API_BASE/ingestions/$INGESTION_ID/processed-files" \
    -H "Authorization: Bearer $TOKEN"

  # Step 3: Trigger ingestion
  echo "Triggering full reprocessing..."
  curl -X POST "$API_BASE/ingestions/$INGESTION_ID/run" \
    -H "Authorization: Bearer $TOKEN"

  echo "Full reprocessing initiated. Monitor at: https://your-platform.com/ingestions/$INGESTION_ID/runs"
else
  echo "Aborted."
fi
```

---

### Example 3: Fresh Start (New Files Only)

**Scenario:** You want to start fresh but don't need to reprocess historical files (only new files going forward).

**Solution:**

```bash
#!/bin/bash
# Fresh start - process only new files

INGESTION_ID="logs-ingestion"

# Step 1: Drop the table
psql -c "DROP TABLE IF EXISTS logs.application_logs;"

# Step 2: Trigger ingestion (skips already-processed files)
curl -X POST "https://your-api.com/api/v1/ingestions/$INGESTION_ID/run" \
  -H "Authorization: Bearer $TOKEN"

echo "Fresh table created. Only new files will be processed going forward."
```

**Note:** No need to clear processed files - autoloader will skip them.

---

## Cost Implications

### Append Mode (Default) - Low Cost
- Processes only NEW files each run
- Efficient: reads only incremental data
- Cost: $X per run (scales with new data)

### Full Reprocess (Option B) - High Cost
- Processes ALL files each run
- Expensive: reads entire source dataset
- Cost: $X per run (scales with total dataset)

**Example Cost Comparison:**

| Scenario | Files in Source | New Files per Day | Append Cost | Reprocess Cost |
|----------|-----------------|-------------------|-------------|----------------|
| Small dataset | 100 files (10 GB) | 5 files (0.5 GB) | $0.10 | $2.00 |
| Medium dataset | 1,000 files (100 GB) | 50 files (5 GB) | $1.00 | $20.00 |
| Large dataset | 10,000 files (1 TB) | 100 files (10 GB) | $2.00 | $200.00 |

**Recommendation:** Use full reprocess sparingly. For most use cases, append mode is sufficient.

---

## Best Practices

### ✅ Do

1. **Use append mode by default** - Incremental ingestion is cost-effective and efficient
2. **Clear processed files only when necessary** - Full reprocessing is expensive
3. **Automate snapshot refreshes** - Use cron/scheduler for daily/weekly snapshots
4. **Test before production** - Verify reprocessing behavior in dev environment first
5. **Monitor costs** - Track ingestion costs, especially for full reprocesses

### ❌ Don't

1. **Don't reprocess on every run** - Defeats the purpose of incremental ingestion
2. **Don't clear processed files by accident** - Could trigger expensive reprocessing
3. **Don't assume overwrite is free** - Re-reading all files costs money
4. **Don't forget to drop the table** - Clearing files without dropping table = appending all files again

---

## FAQ

### Q: Why can't I just set `write_mode: "overwrite"`?

**A:** Overwrite mode is not supported. The write_mode field in the schema only accepts `"append"`. If you specify `"overwrite"`, the API will reject your request with a validation error.

We decided not to implement automatic overwrite mode due to complexity, cost, and unclear semantics. Manual control provides better flexibility and cost transparency.

---

### Q: What if I want daily snapshot refreshes?

**A:** Use **Option B** (delete table + clear processed files) on a daily schedule. This gives you snapshot refresh behavior.

See **Example 1: Full Snapshot Refresh** above for a complete script.

---

### Q: Will clearing processed files delete my table?

**A:** No. Clearing processed files only removes file tracking records. It does NOT delete:
- The ingestion configuration
- The target Iceberg table
- Any data

You must manually drop the table if you want it deleted.

---

### Q: Can I reprocess just FAILED files?

**A:** Yes, but you don't need to clear all processed files. The autoloader automatically retries failed files (up to max_retries). If you want to force retry:

1. Don't clear processed files
2. Just trigger a new run
3. Autoloader will retry eligible failed files

---

### Q: What happens if I clear processed files but DON'T delete the table?

**A:** Autoloader will:
1. See all files as "new" (since you cleared the history)
2. Process ALL files
3. **Append** all data to the existing table
4. Result: **Duplicate data**

**This is almost never what you want.** Always delete the table first if you're clearing processed files.

---

### Q: Is there a way to automate this?

**A:** Yes, you can create a scheduled script (cron, Airflow, etc.) that:
1. Drops the table
2. Calls the clear processed files API
3. Triggers the ingestion run

See **Example 1: Full Snapshot Refresh** for a sample script.

---

### Q: What about merge/upsert mode?

**A:** Merge mode is **planned but not yet implemented**. When available, it will allow:
- Update existing records based on a primary key
- Insert new records
- Efficient incremental updates

This is a better solution than overwrite for most use cases.

---

### Q: How do I know if reprocessing is complete?

**A:** Monitor the run status via:

1. **API**: `GET /api/v1/runs/{run_id}`
   ```bash
   curl "https://your-api.com/api/v1/runs/{run_id}"
   ```

2. **UI**: Navigate to Ingestions → Your Ingestion → Runs tab

3. **Webhook**: Configure alerts for run completion

Run statuses: `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL_SUCCESS`

---

## Migration Guide

### If You Were Using `write_mode: "overwrite"` (Unsupported)

**Before (broken):**
```json
{
  "name": "Users Snapshot",
  "source": { ... },
  "destination": {
    "table": "analytics.dim_users",
    "write_mode": "overwrite"  // ❌ Not supported
  }
}
```

**After (manual workaround):**
```json
{
  "name": "Users Snapshot",
  "source": { ... },
  "destination": {
    "table": "analytics.dim_users",
    "write_mode": "append"  // ✅ Always use append
  }
}
```

**Plus daily script:**
```bash
# Schedule this daily via cron
psql -c "DROP TABLE IF EXISTS analytics.dim_users;"
curl -X DELETE "https://api.com/api/v1/ingestions/{id}/processed-files"
curl -X POST "https://api.com/api/v1/ingestions/{id}/run"
```

---

## Support

If you have questions or need help with manual overwrite workflows:

- **Documentation**: https://docs.iomete.com/autoloader
- **Support**: support@iomete.com
- **Community**: https://community.iomete.com

---

## Changelog

| Date | Change |
|------|--------|
| 2025-01-05 | Initial documentation - Overwrite mode not implemented |

---

**Document Version:** 1.0
**Author:** IOMETE Engineering
**Last Updated:** 2025-01-05
