# File-Level Error Handling & UX Plan

Working notes on how we should surface, classify, and recover from per-file Spark errors (e.g., malformed records) before implementing any code changes.

## Goals
- Isolate bad files without blocking good ones.
- Make failures visible to users with concise, friendly messages and recovery actions.
- Keep operators/devs informed with deep logs, while showing end users short, actionable copy.
- Avoid “green” runs that actually contain failed files; distinguish SUCCESS vs PARTIAL vs FAILED.
- Provide targeted retries and optional quarantine for irreparable files.

## Current Behavior (baseline)
- `BatchFileProcessor.process_files` wraps `_process_single_file`; on exception it marks the file FAILED via `FileStateService.mark_file_failed`.
- `processed_files` table stores `status`, `error_message`, `error_type`, `retry_count`, `run_id` (sufficient to show file-level failures).
- `complete_run_record_task` sets the run to SUCCESS even when some files failed (so runs can appear green while containing failures).
- UI prototypes (`ui/detail.html`, `ui/runs.html`) show run/ingestion status but do not surface file-level failures or retry actions.

## Pain Points to Address
- Users can’t see which files failed or why; only logs expose that detail.
- Runs can be marked SUCCESS despite failed files (no PARTIAL state in UI/DB flow yet).
- Error messages are raw Spark traces; they need categorization and truncation for UI.
- No dedicated retry/quarantine UX for failed files.

## Backend Requirements (no code yet)
1) **Error classification & shaping**
   - Map exceptions to categories: `data_malformed`, `schema_mismatch`, `auth`, `connectivity`, `write_failure`, `unknown`.
   - Add a `retryable` flag and a short `user_message` (1–2 sentences) alongside the raw exception string.
   - Truncate stored `error_message` (e.g., 1–2 KB) and keep pointers to full logs/Spark job ID.

2) **Run status accuracy**
   - If any file fails, set run status to PARTIAL (or FAILED if 100% failed) instead of always SUCCESS.
   - Persist a run-level summary: counts by status (success/failed/skipped), top N errors with file paths, and whether failures are retryable.

3) **Retry & quarantine**
   - Reuse `processed_files.retry_count` to offer “retry failed files” (surgical retry).
   - Optional: quarantine irreparable files (e.g., malformed JSON in FAILFAST mode) by copying to a quarantine path and marking non-retryable.

4) **Visibility & telemetry**
   - Include `run_id`, `file_path`, `error_category`, `retryable` in structured logs.
   - Consider a lightweight API response for UI: paginated failed files with path, category, short message, last attempt time, retryable, and a link to logs.

5) **Config surface for malformed data**
   - Make the ingestion’s read mode explicit (e.g., JSON `mode=FAILFAST` vs `PERMISSIVE` with `columnNameOfCorruptRecord`).
   - When FAILFAST is set, show UI copy that the job stops on first malformed record; when PERMISSIVE, show that bad rows are redirected.

## UI Requirements (tie to existing prototypes)
- **Detail page (`ui/detail.html`)**
  - Banner when the latest run is PARTIAL/FAILED: show counts of failed files and the top error reason.
  - Card listing recent failed files (path, category, short message, last attempt) with actions: “Retry failed files”, “View all failed”, “Download failing file”.
- **Runs page (`ui/runs.html`)**
  - Show PARTIAL/FAILED status when any file failed; expose counts of failed files.
  - Right-side panel: “Failed files” list (top N) with retry badges and a link to the log viewer anchored to the Spark task/job.
- **Ingestion list (`ui/index.html`)**
  - Badge per ingestion like “3 failing files” with tooltip showing the top category and last failure time.
- **Modal/Drawer (new)**
  - On click of a failed file: show path, category, short message, retryable?, last attempt, actions (retry this file, download/quarantine).

## API/Contract Needs for the UI
- Endpoint: list failed files for an ingestion/run (path, category, short_message, retryable, last_attempt, run_id, log_url).
- Endpoint: aggregate per ingestion (failed_count, last_failed_at, top_category).
- Actions: retry failed files (all or selected paths); optional “ignore/quarantine” action.

## Open Questions to Decide Before Coding
- Do we want PERMISSIVE or FAILFAST as the default for JSON/CSV? If PERMISSIVE, where do corrupt rows land (column or dead-letter/quarantine path)?
- Naming for run status with partial failures: PARTIAL vs SUCCESS_WITH_ERRORS?
- Do we store a `user_message` column on `processed_files` or derive on the fly?
- Do we need a dedicated quarantine bucket/path or just mark and skip?
- How many failed files should the UI fetch per page (pagination defaults)?

## Implementation Sequence (proposed)
1) Decide on data-read mode defaults (FAILFAST vs PERMISSIVE) and the run status semantics.
2) Add error classification + shaping in `BatchFileProcessor` before calling `mark_file_failed`.
3) Update run completion logic to set PARTIAL/FAILED and store a summary.
4) Expose minimal APIs for failed-file listing and retry.
5) Wire UI components (banners, badges, failed-file panels) to those APIs.

These notes are to align on behavior and UX before making code or schema changes.
