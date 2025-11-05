# Backfill Guide for IOMETE Autoloader

## What is Backfill?

**Backfill** is the process of loading historical data files that exist in your cloud storage but were not processed during normal scheduled ingestion runs. This typically happens when you:

- Set up a new ingestion for a bucket that already contains historical data
- Want to reprocess files after fixing data quality issues
- Need to load data from before your ingestion was created
- Experienced downtime and missed processing files during that period

## Current Implementation Status

⚠️ **Note:** Full automatic backfill functionality is **not yet implemented**. The configuration fields exist in the system, but the processing logic does not currently use them.

### What Currently Works

1. **First Run Behavior:** When you create and activate a new ingestion, it will process ALL files in the configured location (respecting any file patterns you specified)
2. **Incremental Processing:** After the first run, only new or modified files are processed
3. **Modified File Detection:** Files that have changed (based on ETag/modification time) are automatically reprocessed

### What Doesn't Work Yet

1. **Date-Range Backfill:** Cannot specify "process all files modified since January 1, 2024"
2. **Selective Reprocessing:** Cannot easily reprocess specific historical files without clearing all history
3. **Backfill Configuration UI:** The `backfill_enabled` and `backfill_start_date` fields exist but are not functional

---

## Common Backfill Scenarios (User Perspective)

### Scenario 1: New Ingestion for Existing Data

**Situation:**
> "I have 3 years of AWS billing data already in my S3 bucket. I just set up IOMETE Autoloader to start ingesting this data. What happens?"

**What Happens:**
- ✅ **First Run:** All existing files matching your pattern will be discovered and processed
- ✅ **Subsequent Runs:** Only new files (or modified files) will be processed

**User Action Required:**
- None - this works automatically on first activation

**Considerations:**
- Large backfills can take significant time and cluster resources
- Preview mode helps you estimate the scope before activation
- Consider file patterns to limit scope if needed

---

### Scenario 2: Reprocessing After Schema Changes

**Situation:**
> "I updated my table schema and need to reprocess all historical files with the new schema definition."

**Current Workaround:**
1. Pause the ingestion
2. Use the refresh API (if implemented) with `reprocess_all: true`
3. OR: Delete processed file records via database (not recommended for production)
4. Resume the ingestion

**Future Solution (Planned):**
- Backfill configuration with date ranges
- Selective reprocessing options

---

### Scenario 3: Data Quality Issues

**Situation:**
> "We discovered that files from Q1 2024 were corrupted. The corrupted files have been replaced in S3 with corrected versions. I need to reprocess only these files."

**Current Behavior:**
- ✅ If file modification timestamp changed → File will be automatically reprocessed
- ✅ If file ETag changed → File will be automatically reprocessed
- ❌ If file was replaced with identical metadata → File will NOT be reprocessed

**Workaround:**
1. Identify the specific file paths that need reprocessing
2. Delete those specific records from `processed_files` table
3. Next scheduled run will pick them up as "new" files

---

### Scenario 4: Missed Processing During Downtime

**Situation:**
> "My ingestion was paused for 2 weeks for maintenance. Files were still being uploaded to S3 during this time. What happens when I resume?"

**What Happens:**
- ✅ All files uploaded during the pause will be processed on next run
- ✅ The system tracks processed vs. unprocessed files, not time windows

**User Action Required:**
- Simply resume the ingestion
- Monitor the first run to ensure all missed files are caught up

---

### Scenario 5: Late-Arriving Historical Data

**Situation:**
> "I've been running ingestion for 6 months. A partner just sent me 2 years of historical data they forgot to upload. These files have old modification dates (from when they were originally created)."

**What Happens:**
- ✅ Files will be discovered as "new" (not in processed files table)
- ✅ They will be processed regardless of their modification date
- ⚠️ **Limitation:** Cannot filter by date range if you only want specific months

**User Action Required:**
- Place files in the configured S3 path
- Optionally adjust file patterns if you want to process them separately
- Monitor next scheduled run

---

### Scenario 6: Testing Before Production

**Situation:**
> "I want to test my ingestion configuration on a small subset of historical data before processing millions of files."

**Recommended Approach:**
1. **Use Preview Mode:**
   - Configure ingestion in DRAFT status
   - Use preview API to see what files would be processed
   - Review schema inference and cost estimates

2. **Use File Patterns:**
   - Start with specific date pattern: `data/2024/01/*.parquet`
   - Verify results in destination table
   - Expand pattern to full dataset: `data/**/*.parquet`

3. **Use Separate Ingestion:**
   - Create test ingestion with limited pattern
   - Create production ingestion with full pattern
   - Delete test ingestion when satisfied

---

### Scenario 7: Incremental Backfill by Date Range

**Situation:**
> "I have 10 years of data. I want to backfill it gradually - one year per week - to avoid overwhelming the system."

**Current Status:** ⚠️ Not Directly Supported

**Workaround:**
Create multiple ingestions with date-based file patterns:

```
Ingestion 1: prefix=data/2015/, pattern=**/*.json
Ingestion 2: prefix=data/2016/, pattern=**/*.json
...
Ingestion 3: prefix=data/2024/, pattern=**/*.json
```

Activate them sequentially, pausing previous ones as you move forward.

**Future Solution (Planned):**
- Configure `backfill_start_date` and `backfill_end_date`
- System processes only files in that date range
- After backfill completes, switches to normal incremental mode

---

### Scenario 8: Partition-Aware Backfill

**Situation:**
> "My S3 data is organized by date: `s3://bucket/data/year=2024/month=01/day=15/*.parquet`. I only want to backfill specific partitions."

**Current Solution:**
Use file path patterns to target specific partitions:

- Single day: `prefix=data/year=2024/month=01/day=15/`
- Single month: `prefix=data/year=2024/month=01/`, `pattern=**/day=*/`
- Date range: Not directly supported - use multiple ingestions or process all

---

### Scenario 9: Cost-Conscious Backfill

**Situation:**
> "I have 5TB of historical data. Backfilling all at once would be expensive. How can I control costs?"

**Strategies:**

1. **Use Preview Mode:**
   - Get cost estimates before activating
   - Review file count and size estimates

2. **Adjust Schedule:**
   - Use less frequent schedule for backfill period
   - Process in batches during off-peak hours

3. **Use File Limits (if implemented):**
   - Process N files per run
   - Gradually work through backlog

4. **Partition Strategy:**
   - Backfill by partition/time period
   - Use separate ingestions for historical vs. current data

5. **Cluster Sizing:**
   - Use smaller cluster for backfill if time is not critical
   - Scale up for catch-up processing

---

### Scenario 10: Selective Column Backfill

**Situation:**
> "I want to add new columns to my table and reprocess all historical files to populate those columns."

**Current Behavior:**
- Schema evolution detection exists but reprocessing is manual
- Would require full reprocessing of all files

**Current Approach:**
1. Update schema definition in ingestion configuration
2. Use refresh API (when implemented) or clear processed files
3. Reprocess all historical files
4. OR: Use Spark SQL to backfill new columns from existing data without reprocessing files

---

## How to Identify When You Need Backfill

Ask yourself these questions:

1. **Is this a new ingestion on existing data?**
   - ✅ Auto-handled on first run

2. **Did I change my data/schema and need to reprocess?**
   - ⚠️ Requires manual intervention (refresh API or database cleanup)

3. **Do I have a specific date range to process?**
   - ⚠️ Use file patterns or multiple ingestions (not ideal)

4. **Do I need to reprocess specific files?**
   - ⚠️ Requires database intervention

5. **Am I resuming after downtime?**
   - ✅ Auto-handled - missed files will be processed

---

## Best Practices

### For New Ingestions

1. **Start Small:** Test with a subset using file patterns
2. **Use Preview:** Always preview before activating large backfills
3. **Monitor First Run:** Watch the first scheduled run closely
4. **Check Costs:** Review cost estimates for historical data volume

### For Ongoing Operations

1. **Don't Clear History Unnecessarily:** Processed file tracking prevents duplicate processing
2. **Use Modified Detection:** Let the system auto-detect changed files
3. **Plan for Late Data:** Understand that old-dated files will still be processed if new to the system
4. **Monitor Run History:** Track which files were processed in each run

### For Reprocessing

1. **Pause Before Modifying:** Always pause ingestion before making changes
2. **Backup Metadata:** Consider backing up processed files table before clearing
3. **Use Separate Ingestions for Testing:** Don't experiment with production ingestions
4. **Document Changes:** Keep track of when and why you reprocessed data

---

## Future Backfill Features (Planned)

The following features are planned but not yet implemented:

### 1. Date-Range Backfill Configuration
```yaml
backfill:
  enabled: true
  start_date: "2024-01-01T00:00:00Z"
  end_date: "2024-12-31T23:59:59Z"
  mode: "before_incremental"  # or "parallel"
```

### 2. Refresh Operations API
- Reprocess all files
- Reprocess by date range
- Reprocess by file pattern
- Clear processed history

### 3. Backfill Monitoring
- Separate backfill run history
- Progress tracking for large backfills
- Estimated completion time

### 4. Throttled Backfill
- Max files per run during backfill
- Automatic throttling based on cluster capacity
- Backfill priority vs. incremental priority

---

## Troubleshooting Common Backfill Issues

### Files Not Being Processed

**Symptoms:** Expected files are not showing up in ingestion runs

**Check:**
1. File pattern matches the file paths
2. Files are in the configured prefix
3. Files haven't already been processed (check `processed_files` table)
4. Ingestion is in ACTIVE status
5. Schedule is triggering (check run history)

### Duplicate Processing

**Symptoms:** Same files being processed multiple times

**Check:**
1. Don't manually clear processed files during active runs
2. Check for multiple ingestions targeting same files
3. Verify processed file state is being recorded correctly

### Slow Backfill Performance

**Symptoms:** Historical data taking too long to process

**Solutions:**
1. Increase cluster size
2. Adjust batch size if configurable
3. Process in smaller chunks using multiple ingestions
4. Review file format efficiency (Parquet > CSV)

### Out of Memory During Backfill

**Symptoms:** Cluster runs out of memory processing large backfill

**Solutions:**
1. Reduce max files per run (if available)
2. Increase cluster memory
3. Process smaller date ranges sequentially
4. Review schema complexity and column count

---

## Related Documentation

- [Batch Processing Implementation Guide](../batch-processing/batch-processing-implementation-guide.md)
- [Refresh Operations API PRD](./refresh-operations-api-prd.md) (Planned feature)
- File Discovery Service: `app/services/file_discovery_service.py`
- Batch Orchestrator: `app/services/batch_orchestrator.py`
- Processed File Tracking: `app/repositories/processed_file_repository.py`

---

## Technical Notes for Developers

### Current File State Tracking

The `processed_files` table tracks:
```python
class ProcessedFile(Base):
    file_path: str              # Unique per ingestion
    file_size: int              # For change detection
    modified_at: datetime       # For change detection
    etag: Optional[str]         # For change detection
    state: FileState            # DISCOVERED/PROCESSING/SUCCESS/FAILED/SKIPPED
    processed_at: datetime      # When processing completed
```

### How to Implement Backfill

To add backfill functionality:

1. **In `batch_orchestrator.py` (line 171):**
   ```python
   # Current
   files = await file_discovery.list_files(bucket, prefix, pattern)

   # Needed
   since_date = ingestion.backfill_start_date if ingestion.backfill_enabled else None
   files = await file_discovery.list_files(bucket, prefix, pattern, since=since_date)
   ```

2. **Add backfill completion logic:**
   - Track when backfill is complete
   - Switch from backfill mode to incremental mode
   - Update `backfill_enabled = False` automatically

3. **Add backfill monitoring:**
   - Separate run type for backfill runs
   - Progress tracking (files processed / total estimated)
   - Cost tracking specific to backfill

---

**Document Version:** 1.0
**Last Updated:** 2025-11-05
**Status:** Current implementation review + planned features
