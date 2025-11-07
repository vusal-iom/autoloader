"""
Main Prefect flow for ingestion execution.

This replaces BatchOrchestrator as the entry point for scheduled runs.
"""
from prefect import flow, get_run_logger

from app.prefect.tasks.discovery import discover_files_task
from app.prefect.tasks.state import check_file_state_task
from app.prefect.tasks.processing import process_files_task
from app.prefect.tasks.run_management import (
    create_run_record_task,
    complete_run_record_task,
    fail_run_record_task
)


@flow(
    name="run_ingestion",
    log_prints=True,
    retries=1,
    retry_delay_seconds=300,
)
def run_ingestion_flow(ingestion_id: str, trigger: str = "scheduled"):
    """
    Main Prefect flow for ingestion execution.

    This flow:
    1. Creates a Run record
    2. Discovers new files from cloud storage
    3. Checks which files are already processed
    4. Processes new files via Spark Connect
    5. Updates Run record with final status

    Args:
        ingestion_id: UUID of the ingestion to run (as string)
        trigger: Trigger type (scheduled, manual, retry)
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting ingestion flow for: {ingestion_id}")

    run_id = None
    try:
        # Step 1: Create Run record
        run_id = create_run_record_task(ingestion_id, trigger=trigger)
        logger.info(f"Created run: {run_id}")

        # Step 2: Discover files
        discovered_files = discover_files_task(ingestion_id)

        # Step 3: Check already-processed files
        processed_file_paths = check_file_state_task(ingestion_id)

        # Step 4: Compute new files
        new_files = [
            f for f in discovered_files
            if f['path'] not in processed_file_paths
        ]

        logger.info(
            f"Discovered: {len(discovered_files)}, "
            f"Already processed: {len(processed_file_paths)}, "
            f"New files: {len(new_files)}"
        )

        if len(new_files) == 0:
            logger.info("No new files to process")
            complete_run_record_task(run_id, {'success': 0, 'failed': 0, 'skipped': 0})
            return {
                "status": "NO_NEW_FILES",
                "run_id": run_id,
                "files_processed": 0
            }

        # Step 5: Process files
        metrics = process_files_task(ingestion_id, run_id, new_files)

        # Step 6: Complete run
        complete_run_record_task(run_id, metrics)

        logger.info(f"✅ Ingestion flow completed: {run_id}")

        return {
            "status": "SUCCESS",
            "run_id": run_id,
            "files_processed": metrics['success'] + metrics['failed'],
            "metrics": metrics
        }

    except Exception as e:
        logger.error(f"❌ Ingestion flow failed: {e}", exc_info=True)

        # Mark run as failed if we created one
        if run_id:
            fail_run_record_task(run_id, str(e))

        raise
