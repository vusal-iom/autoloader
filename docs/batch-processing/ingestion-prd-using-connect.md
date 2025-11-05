# IOMETE Data Ingestion PRD - Addendum
# Spark Connect Architecture

**Version:** 1.1  
**Date:** November 2, 2025  
**Changes:** Updated architecture to use Spark Connect instead of job submission

---

## Overview of Change

**Original Approach:** Generate Spark jobs (JAR/Python scripts) and submit them to clusters via job scheduler.

**New Approach (Spark Connect):** Ingestion Service acts as a Spark Connect client, connecting to user's existing IOMETE compute clusters to execute ingestion logic remotely via gRPC protocol.

---

## Benefits of Spark Connect Approach

### 1. **Leverage Existing Infrastructure**
- Users already have compute clusters running in IOMETE
- No need to manage separate ingestion-specific clusters
- Better resource utilization (use existing cluster capacity)

### 2. **Simplified Architecture**
- No JAR/script generation and packaging
- No job submission orchestration
- Direct programmatic control via Spark Connect API
- Cleaner separation between control plane (Ingestion Service) and data plane (Spark clusters)

### 3. **Better Observability**
- Real-time visibility into Spark operations
- Easier to capture metrics during execution
- Better error handling and logging
- Can query Spark execution state directly

### 4. **Interactive Control**
- Can pause/resume operations programmatically
- Real-time progress updates
- Easier to implement preview/test mode (run small batches interactively)
- Better schema inference (immediate feedback)

### 5. **Multi-Tenancy**
- Each user connects to their own cluster
- Natural resource isolation
- Billing already tied to user's cluster usage

---

## Updated Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        IOMETE Platform                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────┐         ┌────────────────────┐              │
│  │  Ingestion API  │◄────────┤  UI (React/Next)   │              │
│  │  (REST/GraphQL) │         │                    │              │
│  └────────┬────────┘         └────────────────────┘              │
│           │                                                       │
│           ▼                                                       │
│  ┌────────────────────────────────────────────────┐             │
│  │   Ingestion Service (Spark Connect Client)    │             │
│  │                                                 │             │
│  │  - Configuration management                    │             │
│  │  - Spark Connect session management            │             │
│  │  - DataFrame operation builder                 │             │
│  │  - Schema inference via Spark Connect          │             │
│  │  - Cost estimation                             │             │
│  │  - Monitoring & metrics collection             │             │
│  │                                                 │             │
│  │  Dependencies:                                  │             │
│  │  - spark-connect-client library                │             │
│  │  - gRPC for communication                      │             │
│  └──────────┬──────────────────────────────────────┘             │
│             │                                                     │
│             ├──► Job Scheduler (trigger ingestion runs)          │
│             │                                                     │
│             └──► Metadata Store (PostgreSQL)                     │
│                  - Ingestion configs                             │
│                  - Spark Connect connection strings              │
│                  - Run history                                   │
│                  - Schema versions                               │
│                                                                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Spark Connect (gRPC)
                            │ Port: 15002
                            ▼
    ┌───────────────────────────────────────────────────┐
    │     User's IOMETE Compute Cluster                │
    │                                                   │
    │  ┌──────────────────────────────────────┐       │
    │  │   Spark Connect Server (built-in)    │       │
    │  │   - Listens on port 15002            │       │
    │  │   - Authenticates requests            │       │
    │  │   - Executes DataFrame operations     │       │
    │  └──────────────────────────────────────┘       │
    │                                                   │
    │  ┌──────────────────────────────────────┐       │
    │  │   Spark Executors                    │       │
    │  │   - Read from cloud storage          │       │
    │  │   - Process data                     │       │
    │  │   - Write to Apache Iceberg          │       │
    │  └──────────────────────────────────────┘       │
    │                                                   │
    └───────────────────────────────────────────────────┘
                            │
                            ▼
        ┌────────────────────────────────────┐
        │   Cloud Storage (S3/Azure/GCS)     │
        │   - Source files                    │
        │   - Checkpoints                    │
        │   - Apache Iceberg tables          │
        └────────────────────────────────────┘
```

---

## Implementation Details with Spark Connect

### TR-100: Spark Connect Client Setup

**Dependencies (Scala/Java):**
```scala
// build.sbt or pom.xml
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "3.5.0"
libraryDependencies += "io.grpc" % "grpc-netty" % "1.57.0"
```

**OR Python:**
```python
# requirements.txt
pyspark[connect]>=3.5.0
```

**Connection String Format:**
```
sc://[host]:[port]/;token=[auth_token];user_id=[user_id]
```

Example:
```
sc://cluster-abc123.iomete.com:15002/;token=eyJhbG...;user_id=user_xyz
```

### TR-101: Ingestion Service Implementation

**Configuration Model - Updated Fields:**
```json
{
  "id": "uuid",
  "tenant_id": "uuid",
  "cluster_id": "cluster-abc123",  // NEW: user's cluster ID
  "spark_connect_url": "sc://cluster-abc123.iomete.com:15002",  // NEW
  "spark_connect_token": "encrypted_token",  // NEW
  
  // ... rest of configuration (source, format, destination, schedule)
}
```

**Core Implementation (Scala Example):**

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

class IngestionExecutor(config: IngestionConfig) {
  
  // Create Spark Connect session
  private def createSession(): SparkSession = {
    SparkSession
      .builder()
      .remote(config.sparkConnectUrl)
      .appName(s"Ingestion-${config.id}")
      .config("spark.sql.streaming.checkpointLocation", config.checkpointLocation)
      .getOrCreate()
  }
  
  // Execute ingestion
  def execute(): IngestionResult = {
    val spark = createSession()
    
    try {
      // Build Auto Loader query
      val df = spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config.format.`type`)
        .option("cloudFiles.schemaLocation", s"${config.checkpointLocation}/schema")
        .option("cloudFiles.inferColumnTypes", config.format.schema.inferTypes)
        .option("cloudFiles.schemaEvolutionMode", 
                if (config.format.schema.evolutionEnabled) "addNewColumns" else "failOnNewColumns")
        // Add source-specific options
        .options(buildSourceOptions(config.source))
        // Add format-specific options
        .options(buildFormatOptions(config.format))
        .load(config.source.path)
      
      // Apply any transformations (future: light SQL transforms)
      val transformedDf = applyTransformations(df, config)
      
      // Write to target table
      val query = transformedDf.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", config.checkpointLocation)
        .option("mergeSchema", config.format.schema.evolutionEnabled)
        // Configure trigger based on schedule mode
        .trigger(getTrigger(config.schedule))
        // Apply partitioning if configured
        .partitionBy(config.destination.partitioning.columns: _*)
        .toTable(s"${config.destination.catalog}.${config.destination.database}.${config.destination.table}")
      
      // Monitor query execution
      monitorQuery(query, config.id)
      
      // Collect metrics
      val metrics = collectMetrics(query)
      
      IngestionResult(
        success = true,
        metrics = metrics
      )
      
    } catch {
      case e: Exception =>
        handleError(e, config)
        IngestionResult(
          success = false,
          error = Some(e.getMessage)
        )
    } finally {
      spark.stop()
    }
  }
  
  private def getTrigger(schedule: ScheduleConfig): Trigger = {
    schedule.mode match {
      case "scheduled" => 
        // Batch mode: process all available data then stop
        Trigger.AvailableNow()
      case "continuous" => 
        // Streaming mode: micro-batches every N seconds
        Trigger.ProcessingTime(schedule.processingInterval)
    }
  }
  
  private def buildSourceOptions(source: SourceConfig): Map[String, String] = {
    source.`type` match {
      case "s3" =>
        Map(
          "cloudFiles.region" -> source.credentials.region,
          "cloudFiles.useNotifications" -> determineNotificationMode(source).toString
        )
      case "azure_blob" =>
        Map(
          "cloudFiles.connectionString" -> source.credentials.connectionString
        )
      case "gcs" =>
        Map(
          "cloudFiles.serviceAccountKey" -> source.credentials.serviceAccountKey
        )
    }
  }
  
  private def buildFormatOptions(format: FormatConfig): Map[String, String] = {
    format.`type` match {
      case "json" =>
        Map(
          "multiline" -> format.options.multiline.toString,
          "compression" -> format.options.compression
        )
      case "csv" =>
        Map(
          "header" -> format.options.header.toString,
          "delimiter" -> format.options.delimiter,
          "quote" -> format.options.quote
        )
      case "parquet" | "avro" | "orc" =>
        Map(
          "compression" -> format.options.compression
        )
    }
  }
  
  private def monitorQuery(query: StreamingQuery, ingestionId: String): Unit = {
    // Launch monitoring thread
    new Thread(() => {
      while (query.isActive) {
        val progress = query.lastProgress
        if (progress != null) {
          // Extract metrics
          val metrics = QueryMetrics(
            numInputRows = progress.numInputRows,
            inputRowsPerSecond = progress.inputRowsPerSecond,
            processedRowsPerSecond = progress.processedRowsPerSecond,
            batchId = progress.batchId
          )
          
          // Store metrics
          metricsService.recordProgress(ingestionId, metrics)
          
          // Check for schema evolution
          if (progress.sources.exists(_.description.contains("schema evolution"))) {
            handleSchemaEvolution(ingestionId, progress)
          }
        }
        Thread.sleep(10000) // Check every 10 seconds
      }
    }).start()
  }
  
  private def collectMetrics(query: StreamingQuery): IngestionMetrics = {
    val recentProgress = query.recentProgress
    
    IngestionMetrics(
      filesProcessed = recentProgress.map(_.sources.head.numInputFiles).sum,
      recordsIngested = recentProgress.map(_.numInputRows).sum,
      bytesRead = recentProgress.map(_.sources.head.inputBytes).sum,
      bytesWritten = recentProgress.map(_.sink.numOutputRows * avgRowSize).sum,
      durationSeconds = recentProgress.map(_.batchDuration).sum / 1000,
      errorCount = 0  // Would track from _rescued_data if enabled
    )
  }
}
```

### TR-102: Session Management

**Session Lifecycle:**

```scala
class SparkConnectSessionManager {
  
  private val sessions = new ConcurrentHashMap[String, SparkSession]()
  private val lastActivity = new ConcurrentHashMap[String, Long]()
  
  def getOrCreateSession(config: IngestionConfig): SparkSession = {
    val key = s"${config.tenantId}-${config.clusterId}"
    
    sessions.computeIfAbsent(key, _ => {
      val session = SparkSession
        .builder()
        .remote(config.sparkConnectUrl)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
      
      lastActivity.put(key, System.currentTimeMillis())
      
      session
    })
  }
  
  def closeIdleSessions(idleTimeoutMs: Long = 30 * 60 * 1000): Unit = {
    val now = System.currentTimeMillis()
    
    sessions.forEach((key, session) => {
      val lastUsed = lastActivity.get(key)
      if (now - lastUsed > idleTimeoutMs) {
        session.stop()
        sessions.remove(key)
        lastActivity.remove(key)
      }
    })
  }
  
  // Run cleanup task periodically
  scheduleCleanup()
  
  private def scheduleCleanup(): Unit = {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    scheduler.scheduleAtFixedRate(
      () => closeIdleSessions(),
      5, 5, TimeUnit.MINUTES
    )
  }
}
```

### TR-103: Preview/Test Mode with Spark Connect

**Interactive Schema Inference:**

```scala
def previewIngestion(config: IngestionConfig): PreviewResult = {
  val spark = sessionManager.getOrCreateSession(config)
  
  // Sample first 10 files only
  val sampleDf = spark.read
    .format(config.format.`type`)
    .options(buildFormatOptions(config.format))
    .load(config.source.path)
    .limit(100)  // Only 100 rows for preview
  
  // Infer schema
  val schema = sampleDf.schema.fields.map { field =>
    ColumnSchema(
      name = field.name,
      dataType = field.dataType.simpleString,
      nullable = field.nullable
    )
  }
  
  // Get sample data
  val sampleData = sampleDf.take(5).map(_.toString)
  
  // Count files
  val fileCount = spark.read
    .format("binaryFile")
    .load(config.source.path)
    .count()
  
  PreviewResult(
    schema = schema,
    sampleData = sampleData,
    fileCount = fileCount,
    estimatedRecords = fileCount * avgRecordsPerFile
  )
}
```

### TR-104: Schema Evolution Detection

**Detect Changes via Spark Connect:**

```scala
def detectSchemaChanges(config: IngestionConfig): Option[SchemaChange] = {
  val spark = sessionManager.getOrCreateSession(config)
  
  // Get current schema from checkpoint
  val currentSchema = getCurrentSchema(config.checkpointLocation)
  
  // Infer schema from recent files (last 24 hours)
  val recentFiles = getRecentFiles(config.source.path, hours = 24)
  
  if (recentFiles.nonEmpty) {
    val newSchema = spark.read
      .format(config.format.`type`)
      .load(recentFiles: _*)
      .schema
    
    val changes = compareSchemas(currentSchema, newSchema)
    
    if (changes.nonEmpty) {
      // Pause streaming query
      pauseIngestion(config.id)
      
      // Create schema version record
      Some(SchemaChange(
        ingestionId = config.id,
        oldSchema = currentSchema,
        newSchema = newSchema,
        changes = changes,
        affectedFiles = recentFiles
      ))
    } else {
      None
    }
  } else {
    None
  }
}
```

### TR-105: Cost Estimation with Cluster Info

**Enhanced Cost Calculation:**

```scala
def estimateCost(config: IngestionConfig): CostEstimate = {
  val spark = sessionManager.getOrCreateSession(config)
  
  // Get actual cluster info
  val clusterInfo = getClusterInfo(config.clusterId)
  val costPerDbUHour = clusterInfo.pricing.dbURate
  val clusterDbUs = clusterInfo.workers * clusterInfo.dbUPerWorker
  
  // Sample files to estimate processing time
  val sampleFiles = listFiles(config.source.path).take(10)
  val sampleSize = sampleFiles.map(_.size).sum
  val totalSize = estimateTotalSize(config.source.path)
  
  // Benchmark processing time on sample
  val startTime = System.currentTimeMillis()
  val sampleDf = spark.read
    .format(config.format.`type`)
    .load(sampleFiles.map(_.path): _*)
  sampleDf.count()  // Force execution
  val sampleDuration = (System.currentTimeMillis() - startTime) / 1000.0 / 60.0  // minutes
  
  // Extrapolate to full dataset
  val estimatedDuration = (totalSize.toDouble / sampleSize) * sampleDuration
  
  // Calculate costs
  val runsPerMonth = getRunsPerMonth(config.schedule)
  val computeCostPerRun = (estimatedDuration / 60.0) * clusterDbUs * costPerDbUHour
  val computeCostMonthly = computeCostPerRun * runsPerMonth
  
  val storageCostMonthly = (totalSize / 1e9) * runsPerMonth * 0.023  // $0.023/GB
  val discoveryCostMonthly = (sampleFiles.size * runsPerMonth / 1000.0) * 0.005  // $0.005/1k operations
  
  CostEstimate(
    computePerRun = computeCostPerRun,
    computeMonthly = computeCostMonthly,
    storageMonthly = storageCostMonthly,
    discoveryMonthly = discoveryCostMonthly,
    totalMonthly = computeCostMonthly + storageCostMonthly + discoveryCostMonthly,
    breakdown = Map(
      "estimatedDuration" -> estimatedDuration,
      "clusterDbUs" -> clusterDbUs,
      "runsPerMonth" -> runsPerMonth
    )
  )
}
```

---

## API Changes

### New Endpoints for Cluster Management

**GET /api/v1/clusters**
- List user's available compute clusters
- Return cluster ID, name, status, Spark Connect URL
- Used in wizard to select target cluster

**GET /api/v1/clusters/{id}/info**
- Get detailed cluster info (workers, DBUs, pricing)
- Used for cost estimation

**POST /api/v1/clusters/{id}/test-connection**
- Test Spark Connect connectivity
- Verify authentication token works
- Return cluster capabilities (Spark version, available memory)

### Updated Ingestion Endpoints

**POST /api/v1/ingestions (Updated)**
```json
{
  "cluster_id": "cluster-abc123",  // NEW: Required
  "source": { ... },
  "format": { ... },
  "destination": { ... },
  "schedule": { ... }
}
```

Response includes:
```json
{
  "id": "ingestion-xyz",
  "spark_connect_url": "sc://cluster-abc123.iomete.com:15002",
  "status": "active",
  ...
}
```

---

## Configuration Changes

### Ingestion Wizard - Step 0.5: Select Cluster

**NEW STEP before Source configuration:**

```
┌─────────────────────────────────────────┐
│  Step 0: Select Compute Cluster        │
├─────────────────────────────────────────┤
│                                         │
│  Choose the cluster to run ingestion:  │
│                                         │
│  ○ cluster-prod-01                     │
│    Status: Running                      │
│    8 workers • 64 DBU                   │
│    Cost: $0.40/DBU-hour                │
│                                         │
│  ● cluster-analytics-02                │
│    Status: Running                      │
│    4 workers • 32 DBU                   │
│    Cost: $0.40/DBU-hour                │
│    ✓ Selected                          │
│                                         │
│  ○ cluster-dev-03                      │
│    Status: Stopped                      │
│    [Start Cluster]                     │
│                                         │
│  [+ Create New Cluster]                │
│                                         │
└─────────────────────────────────────────┘
```

### Security: Authentication Token Management

**Token Storage:**
```json
{
  "cluster_id": "cluster-abc123",
  "spark_connect_token": {
    "encrypted_value": "AES256_ENCRYPTED_STRING",
    "created_at": "2025-11-02T10:00:00Z",
    "expires_at": "2025-11-09T10:00:00Z",  // 7-day expiration
    "auto_refresh": true
  }
}
```

**Token Refresh Logic:**
```scala
def refreshTokenIfNeeded(config: IngestionConfig): String = {
  val token = config.sparkConnectToken
  
  if (token.expiresAt.isBefore(Instant.now().plusHours(24))) {
    // Refresh token using IOMETE auth service
    val newToken = authService.generateSparkConnectToken(
      clusterId = config.clusterId,
      userId = config.createdBy,
      validityDays = 7
    )
    
    // Update stored token
    updateIngestionToken(config.id, newToken)
    
    newToken.value
  } else {
    token.decryptedValue
  }
}
```

---

## Error Handling Specific to Spark Connect

### Connection Errors

```scala
def executeWithRetry(config: IngestionConfig, maxRetries: Int = 3): IngestionResult = {
  var attempt = 0
  var lastError: Option[Throwable] = None
  
  while (attempt < maxRetries) {
    try {
      return execute(config)
    } catch {
      case e: io.grpc.StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE =>
        // Cluster unreachable
        lastError = Some(e)
        attempt += 1
        if (attempt < maxRetries) {
          Thread.sleep(Math.pow(2, attempt).toLong * 1000)  // Exponential backoff
        }
      
      case e: io.grpc.StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAUTHENTICATED =>
        // Token expired or invalid
        val newToken = refreshTokenIfNeeded(config)
        // Retry with new token
        attempt += 1
      
      case e: Exception =>
        // Other errors - don't retry
        return IngestionResult(success = false, error = Some(e.getMessage))
    }
  }
  
  // All retries failed
  IngestionResult(
    success = false,
    error = Some(s"Failed after $maxRetries retries: ${lastError.map(_.getMessage)}")
  )
}
```

### User-Friendly Error Messages

```scala
def translateError(error: Throwable): UserFriendlyError = {
  error match {
    case e: io.grpc.StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE =>
      UserFriendlyError(
        title = "Cluster Unreachable",
        message = "Cannot connect to your compute cluster. Please ensure the cluster is running.",
        action = "Start the cluster or select a different cluster.",
        technicalDetails = e.getMessage
      )
    
    case e: io.grpc.StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAUTHENTICATED =>
      UserFriendlyError(
        title = "Authentication Failed",
        message = "Your session has expired or the cluster credentials are invalid.",
        action = "Please refresh the page and try again.",
        technicalDetails = e.getMessage
      )
    
    case e: org.apache.spark.sql.streaming.StreamingQueryException if e.getMessage.contains("schema") =>
      UserFriendlyError(
        title = "Schema Change Detected",
        message = "The structure of your source files has changed.",
        action = "Review the schema changes and choose how to proceed.",
        technicalDetails = e.getMessage
      )
    
    case e =>
      UserFriendlyError(
        title = "Ingestion Failed",
        message = "An unexpected error occurred during data ingestion.",
        action = "Check the logs for more details or contact support.",
        technicalDetails = e.getMessage
      )
  }
}
```

---

## Performance Considerations

### Session Pooling

**Problem:** Creating new Spark Connect session for each ingestion is slow (2-5 seconds).

**Solution:** Pool sessions by cluster and reuse them.

```scala
class SparkConnectSessionPool {
  private val pool = new ConcurrentHashMap[String, ArrayBlockingQueue[SparkSession]]()
  
  def getSession(clusterId: String, connectUrl: String): SparkSession = {
    val queue = pool.computeIfAbsent(clusterId, _ => 
      new ArrayBlockingQueue[SparkSession](10)  // Max 10 sessions per cluster
    )
    
    Option(queue.poll()) match {
      case Some(session) if session.sparkContext.isStopped == false =>
        session
      case _ =>
        createNewSession(connectUrl)
    }
  }
  
  def returnSession(clusterId: String, session: SparkSession): Unit = {
    val queue = pool.get(clusterId)
    if (queue != null && !session.sparkContext.isStopped) {
      queue.offer(session)  // Return to pool
    }
  }
}
```

### Async Query Submission

**Problem:** Waiting for Spark query to complete blocks the API thread.

**Solution:** Submit queries asynchronously and poll for status.

```scala
def executeAsync(config: IngestionConfig): Future[IngestionResult] = {
  Future {
    val session = sessionPool.getSession(config.clusterId, config.sparkConnectUrl)
    try {
      execute(session, config)
    } finally {
      sessionPool.returnSession(config.clusterId, session)
    }
  }(executionContext)
}
```

---

## Testing with Spark Connect

### Integration Tests

```scala
class IngestionServiceIntegrationTest extends FunSuite with BeforeAndAfterAll {
  
  var testCluster: SparkSession = _
  var testConfig: IngestionConfig = _
  
  override def beforeAll(): Unit = {
    // Start local Spark Connect server for testing
    testCluster = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.connect.enabled", "true")
      .config("spark.connect.port", "15002")
      .getOrCreate()
    
    testConfig = IngestionConfig(
      id = "test-ingestion",
      clusterId = "test-cluster",
      sparkConnectUrl = "sc://localhost:15002",
      source = SourceConfig(
        `type` = "s3",
        path = "s3a://test-bucket/data/",
        credentials = ...
      ),
      // ... rest of config
    )
  }
  
  test("execute ingestion successfully") {
    val service = new IngestionExecutor(testConfig)
    val result = service.execute()
    
    assert(result.success)
    assert(result.metrics.recordsIngested > 0)
  }
  
  test("detect schema changes") {
    // Add files with new schema
    // ... 
    
    val changes = service.detectSchemaChanges(testConfig)
    assert(changes.isDefined)
    assert(changes.get.changes.exists(_.changeType == "NEW_COLUMN"))
  }
  
  override def afterAll(): Unit = {
    testCluster.stop()
  }
}
```

---

## Migration Plan (for existing IOMETE deployments)

### Phase 1: Deploy with Spark Connect Support
1. Add Spark Connect client library to Ingestion Service
2. Update cluster creation to enable Spark Connect by default
3. Implement session management
4. Deploy behind feature flag

### Phase 2: UI Updates
1. Add cluster selection step to wizard
2. Update cost estimation to use actual cluster pricing
3. Add cluster status indicators

### Phase 3: Rollout
1. Enable for new ingestions (green field)
2. Migrate existing ingestions one-by-one (optional)
3. Provide migration tool for bulk migration

---

## Advantages Summary: Spark Connect vs. Job Submission

| Aspect | Job Submission | Spark Connect |
|--------|---------------|---------------|
| **Setup Complexity** | High (generate JAR, submit) | Low (direct API calls) |
| **Latency** | 10-30 seconds (job startup) | 1-3 seconds (session reuse) |
| **Resource Usage** | Dedicated cluster per job | Shared user cluster |
| **Observability** | Logs only | Real-time progress |
| **Error Handling** | Parse logs | Structured exceptions |
| **Interactive Features** | Difficult | Easy (preview, test) |
| **Cost Model** | Complex (separate clusters) | Simple (user's cluster) |
| **Multi-tenancy** | Separate clusters per tenant | Natural via separate clusters |

---

## Open Questions with Spark Connect

1. **Q:** What if user's cluster is stopped?
    - **A:** Show warning in wizard. Option to start cluster or select different one.

2. **Q:** Session timeout handling?
    - **A:** Implement keep-alive ping every 60 seconds. Reconnect on timeout.

3. **Q:** Concurrent ingestions on same cluster?
    - **A:** Spark handles this. Configure max concurrent sessions (e.g., 10 per cluster).

4. **Q:** Network latency to cluster?
    - **A:** Minimal impact. Only control messages via gRPC. Data stays in cloud.

5. **Q:** Spark Connect version compatibility?
    - **A:** Require Spark 3.5+ (when Spark Connect became stable). Check version on connection.

---

## Updated Implementation Timeline

**Month 1:**
- Week 1-2: Add Spark Connect client integration
- Week 3: Implement session management and pooling
- Week 4: Build DataFrame operation builder (replaces code generation)

**Month 2:**
- Week 1: Implement preview/test mode with Spark Connect
- Week 2: Cluster selection UI
- Week 3-4: Integration testing and bug fixes

**Month 3:**
- Week 1-2: Beta testing with customers
- Week 3-4: Production deployment

**Benefits of Spark Connect approach:**
- ✅ Faster development (no JAR building/packaging)
- ✅ Easier testing (no cluster coordination)
- ✅ Better UX (faster preview/test mode)
- ✅ Simpler architecture (fewer moving parts)

---

**END OF ADDENDUM**