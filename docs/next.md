- Review storing files in the db. we may need to move it to iceberg table
- Performance implications of fetching all the files again and again. Instead moving processed files somewhere (an idea). Maybe force to notification system
- Notifications system (sqs, kafka, rabit, http handler) as a plugin architecture
- scaling the system
- Sending alert/email
- Monitoring and audit
- performance test (we can mock spark and minio ops here to simulate cases)


----
Critical Production Blockers (Must Fix)

  1. Authentication & Authorization System
    - Currently using hardcoded tenant/user IDs throughout the codebase
    - Need to implement proper FastAPI security dependencies
    - 8 TODO comments across multiple files
    - Files affected: app/api/v1/ingestions.py, app/config.py, app/services/ingestion_service.py
  2. Application Lifecycle Management
    - Missing scheduler initialization/shutdown
    - Missing Spark session pool lifecycle management
    - CORS configuration hardcoded for development
    - Missing health check implementations (database connectivity, scheduler status)
  3. Unit Test Coverage
    - Currently only E2E tests exist
    - No unit tests for services or repositories
    - Need tests for concurrency scenarios in file state tracking

  High Priority Features (80% Complete)

  4. Scheduler Integration
    - Database fields exist, but no APScheduler implementation
    - Would enable automated scheduled ingestion runs
    - ~4 hours estimated effort
  5. Date-Range Backfill Integration
    - Infrastructure 95% ready - just needs 1-line integration in batch_orchestrator.py:150-155
    - Database fields exist, file discovery supports it
    - ~2 hours estimated effort

  Medium Priority Enhancements

  6. Multi-Cloud Support
    - S3 fully implemented ✅
    - Azure Blob & GCS SDKs installed but no implementation code
    - Would require duplicating file discovery pattern for each cloud
  7. Schema Evolution Approval Workflow
    - Detection infrastructure exists
    - Need to implement approval/rejection workflow
    - User notification system
  8. Monitoring & Observability
    - Add Prometheus metrics (client already in requirements)
    - Distributed tracing
    - Performance dashboards

  Long-term / Phase 3 Features

  1. Performance Optimizations
    - Parallel file processing (currently sequential)
    - Advanced glob pattern support (currently simple suffix matching)
    - Per-run cost limits
  2.  Notification System
    - Email notifications for run completion/failures
    - Webhook support for integration with external systems

  Documentation & DevOps

  11. Production Deployment Guide
    - Need documentation for production setup
    - CORS configuration guide
    - Cluster credential management guide
  12. Performance Benchmarking
    - Load testing with 1000+ files
    - Multi-worker concurrency testing
    - Cost measurement validation

  ---
  Recommendation: I'd suggest prioritizing in this order:
  1. Authentication system (security critical)
  2. Unit tests (quality/confidence)
  3. Scheduler integration (core UX feature - currently requires manual triggering)
  4. Date-range backfill (trivial fix, high user value)
  5. Everything else based on customer needs

  Would you like me to start working on any of these items?
