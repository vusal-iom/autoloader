# Directory Listing Optimizations for IOMETE Autoloader

## Problem Statement

As directories accumulate files over time (hundreds of thousands to millions of files), directory listing becomes one of the most expensive operations in the ingestion pipeline:

1. **S3 API Costs**: Each `list_objects_v2` call returns max 1000 objects, requiring hundreds/thousands of API calls
2. **Database Lookups**: Each discovered file requires checking against `processed_files` table
3. **Memory Usage**: Loading large file lists into memory
4. **Time Overhead**: Listing can take minutes for large directories, blocking ingestion start

**Current Implementation** (app/services/file_discovery_service.py):
- Uses boto3 `list_objects_v2` with continuation tokens
- Fetches 1000 objects per API call
- No caching, no incremental optimization
- Relies on database to filter already-processed files

---

## Optimization Categories

### 1. S3 Listing API Optimizations

#### 1.1 Use StartAfter / Marker-Based Pagination
**Concept**: Remember the last file processed and resume from there

```python
# Store last processed file key in ingestion metadata
last_processed_key = ingestion.metadata.get('last_processed_s3_key')

kwargs = {
    'Bucket': bucket,
    'Prefix': prefix,
    'StartAfter': last_processed_key  # Resume from here
}
```

**Pros**:
- Skip already-processed files at S3 API level
- Dramatically reduces API calls for incremental loads
- Works naturally with append-only data patterns

**Cons**:
- Only works if files arrive in lexicographic order
- Doesn't handle late-arriving files (out-of-order writes)
- Requires storing cursor state per ingestion

**Use Cases**:
- Time-partitioned data (e.g., `/year=2024/month=01/day=15/`)
- Sequential log files with timestamps in names
- Data lakes with append-only patterns

---

#### 1.2 Prefix Partitioning Strategy
**Concept**: Use date/time prefixes to limit listing scope

```python
# Only list recent partitions
if ingestion.incremental_mode:
    # Last 7 days of partitions
    prefixes = generate_date_prefixes(
        base=source_path,
        days=7,
        pattern="year={year}/month={month}/day={day}/"
    )

    for prefix in prefixes:
        files = list_files(bucket, prefix)
```

**Pros**:
- Massively reduces listing scope (7 days vs 3 years)
- Natural fit for time-series data
- Enables parallel listing across partitions

**Cons**:
- Requires predictable partitioning scheme
- User must configure partition pattern
- Misses files in unexpected locations

**Implementation**:
- Add `partition_pattern` to ingestion config
- Support Hive-style partitioning detection
- Allow user to specify "lookback window" (e.g., last 7 days)

---

#### 1.3 S3 Inventory for Large Buckets
**Concept**: Use S3 Inventory instead of LIST API for buckets with millions of files

**How S3 Inventory Works**:
- S3 generates daily/weekly manifest of all objects
- Delivered as Parquet/CSV files to destination bucket
- Contains: key, size, last_modified, etag, storage_class

```python
def list_from_inventory(bucket: str, inventory_bucket: str):
    """
    Read S3 Inventory manifest instead of calling LIST API
    """
    # Read latest inventory manifest
    manifest = read_inventory_manifest(inventory_bucket)

    # Use Spark to filter inventory (already Parquet!)
    df = spark.read.parquet(manifest.data_files)

    # Filter to our prefix
    filtered = df.filter(col("key").startswith(prefix))

    return filtered.collect()
```

**Pros**:
- **No LIST API costs** - inventory is free (except storage)
- Extremely fast for millions of files (read Parquet with Spark)
- Includes size, modified_at, etag - no need for HEAD requests
- Can be processed in parallel with Spark

**Cons**:
- Inventory updates are delayed (daily/weekly)
- Requires S3 Inventory configuration by user
- Not suitable for real-time ingestion
- Additional setup complexity

**Use Cases**:
- Data lakes with millions of files
- Batch ingestion (hourly/daily schedules)
- Historical backfills

---

#### 1.4 Parallel Listing with Prefix Sharding
**Concept**: Split listing across multiple prefixes in parallel

```python
from concurrent.futures import ThreadPoolExecutor

def list_files_parallel(bucket: str, prefix: str, workers: int = 10):
    """
    List files in parallel using common prefix sharding
    """
    # Get common prefixes (directories)
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter='/'
    )

    prefixes = [p['Prefix'] for p in response.get('CommonPrefixes', [])]

    # List each prefix in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(list_single_prefix, bucket, p)
            for p in prefixes
        ]
        results = [f.result() for f in futures]

    return flatten(results)
```

**Pros**:
- 10x+ speedup for directories with many subdirectories
- Better resource utilization
- Works with existing code

**Cons**:
- More complex code
- Need to tune worker count
- May hit S3 rate limits if too aggressive

---

### 2. Database Query Optimizations

#### 2.1 Batch Existence Checks
**Current Problem**: Checking file-by-file if processed

```python
# SLOW: N database queries
for file in discovered_files:
    if is_processed(file.path):  # SELECT query per file
        continue
```

**Optimized Approach**: Batch query with `IN` clause

```python
def get_processed_files_batch(
    ingestion_id: str,
    file_paths: List[str]
) -> Set[str]:
    """
    Fetch all processed files in one query
    """
    # Single query with IN clause
    processed = db.query(ProcessedFile.file_path).filter(
        ProcessedFile.ingestion_id == ingestion_id,
        ProcessedFile.file_path.in_(file_paths),
        ProcessedFile.status.in_(['SUCCESS', 'SKIPPED'])
    ).all()

    return {f.file_path for f in processed}

# Usage
discovered_paths = [f.path for f in files]
processed_paths = get_processed_files_batch(ingestion_id, discovered_paths)

new_files = [f for f in files if f.path not in processed_paths]
```

**Pros**:
- Reduces N queries to 1 query
- Massive speedup (100x+)
- Simple to implement

**Cons**:
- IN clause size limits (tens of thousands)
- May need to batch the batches for very large lists

**Note**: This is already partially implemented in `processed_file_repository.py:get_processed_file_paths()`, but could be optimized further.

---

#### 2.2 Indexed Prefix Queries
**Concept**: Use database indexes optimized for prefix searches

```sql
-- Create GIN index for prefix matching (PostgreSQL)
CREATE INDEX idx_processed_files_path_prefix
ON processed_files
USING GIN (file_path gin_trgm_ops);

-- Or use prefix matching with B-tree
CREATE INDEX idx_processed_files_path_btree
ON processed_files (ingestion_id, file_path varchar_pattern_ops);
```

**Query Optimization**:
```python
# Instead of loading ALL processed files
# Load only files matching prefix pattern
processed = db.query(ProcessedFile.file_path).filter(
    ProcessedFile.ingestion_id == ingestion_id,
    ProcessedFile.file_path.like(f's3://bucket/{prefix}%'),
    ProcessedFile.status.in_(['SUCCESS', 'SKIPPED'])
).all()
```

**Pros**:
- Only fetch relevant subset
- Index scan instead of full table scan
- Scalable to millions of files

**Cons**:
- PostgreSQL-specific features
- Index storage overhead

---

#### 2.3 Bloom Filters for Fast Negative Lookups
**Concept**: Use in-memory Bloom filter for "probably not processed" checks

```python
class ProcessedFileBloomFilter:
    """
    Memory-efficient probabilistic data structure
    """
    def __init__(self, ingestion_id: str, db: Session):
        self.bloom = BloomFilter(capacity=1_000_000, error_rate=0.001)

        # Load all processed file paths into bloom filter
        processed = db.query(ProcessedFile.file_path).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status.in_(['SUCCESS', 'SKIPPED'])
        ).all()

        for f in processed:
            self.bloom.add(f.file_path)

    def might_be_processed(self, file_path: str) -> bool:
        """
        Fast O(1) check. False positives possible, no false negatives.
        """
        return file_path in self.bloom

# Usage
bloom = ProcessedFileBloomFilter(ingestion_id, db)

new_files = []
maybe_processed = []

for file in discovered_files:
    if not bloom.might_be_processed(file.path):
        # Definitely new - no database check needed
        new_files.append(file)
    else:
        # Maybe processed - verify with database
        maybe_processed.append(file)

# Only query database for uncertain files
verified_processed = get_processed_files_batch(
    ingestion_id,
    [f.path for f in maybe_processed]
)
```

**Pros**:
- Extremely fast O(1) lookups
- Low memory usage (~1.2MB per million files)
- Eliminates most database queries

**Cons**:
- False positive rate (0.1% default)
- Requires loading bloom filter on startup
- Additional dependency (pybloom-live)

---

### 3. Temporal Filtering Optimizations

#### 3.1 Time-Based Incremental Listing
**Concept**: Only list files modified after last run

```python
def list_files_incremental(
    bucket: str,
    prefix: str,
    since: datetime
) -> List[FileMetadata]:
    """
    Only return files modified after 'since' timestamp
    """
    files = []

    for obj in paginate_s3_objects(bucket, prefix):
        # Filter at S3 level
        if obj['LastModified'] < since:
            continue  # Skip old files

        files.append(FileMetadata(
            path=f"s3a://{bucket}/{obj['Key']}",
            modified_at=obj['LastModified'],
            size=obj['Size'],
            etag=obj['ETag']
        ))

    return files

# Usage
last_run_time = ingestion.last_run_time or datetime.min
new_files = list_files_incremental(bucket, prefix, since=last_run_time)
```

**Current Status**: Already implemented in `file_discovery_service.py:list_files(since=...)` ✅

**Enhancement**: Make this the default behavior
- Auto-populate `since` from `ingestion.last_run_time`
- Add `full_refresh_mode` flag to override

---

#### 3.2 ETag-Based Change Detection
**Concept**: Use S3 ETag to detect file modifications without re-processing

**Current Implementation**: Already tracks ETags in `ProcessedFile.file_etag` ✅

**Optimization**: Cache ETags in memory
```python
class ETagCache:
    """
    In-memory cache of file_path -> etag mappings
    """
    def __init__(self, ingestion_id: str, db: Session):
        # Load on startup
        self.etags = {
            f.file_path: f.file_etag
            for f in db.query(
                ProcessedFile.file_path,
                ProcessedFile.file_etag
            ).filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.status == 'SUCCESS'
            ).all()
        }

    def has_changed(self, file_path: str, new_etag: str) -> bool:
        """Fast in-memory check if file changed"""
        old_etag = self.etags.get(file_path)
        return old_etag is None or old_etag != new_etag
```

---

### 4. Caching Strategies

#### 4.1 Redis Cache for Processed Files
**Concept**: Cache processed file paths in Redis for ultra-fast lookups

```python
class RedisFileStateCache:
    """
    Redis-backed cache for processed files
    """
    def __init__(self, redis_client, ingestion_id: str):
        self.redis = redis_client
        self.key = f"autoloader:processed:{ingestion_id}"

    def is_processed(self, file_path: str) -> bool:
        """O(1) Redis SET membership check"""
        return self.redis.sismember(self.key, file_path)

    def mark_processed(self, file_path: str):
        """Add to Redis SET"""
        self.redis.sadd(self.key, file_path)

    def sync_from_db(self, db: Session, ingestion_id: str):
        """Periodic sync from PostgreSQL"""
        processed = db.query(ProcessedFile.file_path).filter(
            ProcessedFile.ingestion_id == ingestion_id,
            ProcessedFile.status.in_(['SUCCESS', 'SKIPPED'])
        ).all()

        pipeline = self.redis.pipeline()
        for f in processed:
            pipeline.sadd(self.key, f.file_path)
        pipeline.execute()
```

**Pros**:
- Sub-millisecond lookups
- Shared across multiple workers
- Persistence across restarts
- Simple SET operations

**Cons**:
- Additional infrastructure (Redis)
- Cache invalidation complexity
- Memory usage in Redis

**When to Use**:
- High-frequency ingestion (multiple runs per hour)
- Multiple concurrent ingestion workers
- Large processed file counts (>100k files)

---

#### 4.2 Local File Cache
**Concept**: Cache listing results to local disk with TTL

```python
import pickle
from pathlib import Path
from datetime import datetime, timedelta

class LocalListingCache:
    """
    Cache S3 listing results to local filesystem
    """
    def __init__(self, cache_dir: str = "/tmp/autoloader-cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

    def get_cached_listing(
        self,
        ingestion_id: str,
        ttl_minutes: int = 60
    ) -> Optional[List[FileMetadata]]:
        """
        Retrieve cached listing if fresh
        """
        cache_file = self.cache_dir / f"{ingestion_id}.pkl"

        if not cache_file.exists():
            return None

        # Check age
        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if datetime.now() - mtime > timedelta(minutes=ttl_minutes):
            cache_file.unlink()  # Expired
            return None

        with open(cache_file, 'rb') as f:
            return pickle.load(f)

    def cache_listing(
        self,
        ingestion_id: str,
        files: List[FileMetadata]
    ):
        """Store listing to cache"""
        cache_file = self.cache_dir / f"{ingestion_id}.pkl"
        with open(cache_file, 'wb') as f:
            pickle.dump(files, f)
```

**Pros**:
- Zero infrastructure
- Simple implementation
- Useful for debugging/testing

**Cons**:
- Not shared across workers
- Disk I/O overhead
- Stale data risk

---

### 5. Architectural Optimizations

#### 5.1 Delta Lake/Iceberg Manifest-Based Discovery
**Concept**: Use Iceberg's manifest files to track which source files have been ingested

**How Iceberg Works**:
- Iceberg tables maintain manifest files with metadata
- Manifests track source file paths used in each write
- Can query manifests to see what's been ingested

```python
def get_processed_files_from_iceberg(
    spark,
    table_identifier: str
) -> Set[str]:
    """
    Query Iceberg metadata to find source files
    """
    # Read Iceberg metadata
    manifests = spark.sql(f"""
        SELECT input_file_name
        FROM {table_identifier}.files
    """).collect()

    return {row.input_file_name for row in manifests}
```

**Pros**:
- Single source of truth (Iceberg table)
- No separate `processed_files` table needed
- Automatic deduplication via Iceberg

**Cons**:
- Requires Iceberg metadata queries
- Tight coupling to Iceberg
- Harder to track failures/retries

---

#### 5.2 Event-Driven Architecture (S3 Event Notifications)
**Concept**: Use S3 Event Notifications instead of periodic listing

**How It Works**:
```
S3 Bucket → SNS Topic → SQS Queue → Autoloader Worker
```

**Flow**:
1. Configure S3 bucket to send notifications on `s3:ObjectCreated:*`
2. Notifications go to SQS queue
3. Autoloader polls SQS for new files
4. Process files immediately (no listing needed!)

```python
def poll_s3_events(queue_url: str) -> List[str]:
    """
    Poll SQS for S3 event notifications
    """
    sqs = boto3.client('sqs')

    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20
    )

    file_paths = []
    for message in response.get('Messages', []):
        event = json.loads(message['Body'])
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            file_paths.append(f"s3a://{bucket}/{key}")

    return file_paths
```

**Pros**:
- **Zero listing costs**
- Real-time ingestion (sub-minute latency)
- Scales to billions of files
- Only process new files

**Cons**:
- Requires AWS infrastructure setup
- Doesn't work for backfills
- Complex operational model
- SQS costs

**Use Cases**:
- Real-time/near-real-time ingestion
- High-frequency file arrivals
- Production data lakes

---

#### 5.3 Hybrid: Checkpoint + Incremental Listing
**Concept**: Combine checkpointing with smart incremental listing

```python
class CheckpointedListing:
    """
    Resume listing from last checkpoint
    """
    def __init__(self, ingestion: Ingestion, db: Session):
        self.ingestion = ingestion
        self.db = db
        self.checkpoint = self._load_checkpoint()

    def list_new_files(self) -> List[FileMetadata]:
        """
        List only files after checkpoint
        """
        since_time = self.checkpoint.get('last_modified_time')
        start_after = self.checkpoint.get('last_file_key')

        # Combine temporal + lexicographic filtering
        files = list_files(
            bucket=self.bucket,
            prefix=self.prefix,
            since=since_time,
            start_after=start_after
        )

        # Update checkpoint
        if files:
            self._save_checkpoint(
                last_file_key=files[-1].path,
                last_modified_time=max(f.modified_at for f in files)
            )

        return files

    def _load_checkpoint(self) -> dict:
        """Load checkpoint from ingestion metadata"""
        return self.ingestion.checkpoint_metadata or {}

    def _save_checkpoint(self, **kwargs):
        """Save checkpoint to database"""
        self.ingestion.checkpoint_metadata = kwargs
        self.db.commit()
```

---

### 6. Configuration-Based Optimizations

#### 6.1 User-Configurable Listing Strategy
**Concept**: Let users choose listing strategy based on their data pattern

```python
class ListingStrategy(str, Enum):
    FULL = "full"                    # List all files every time
    INCREMENTAL_TIME = "incremental_time"  # Only files modified since last run
    INCREMENTAL_LEXICAL = "incremental_lexical"  # StartAfter last processed key
    PARTITIONED = "partitioned"      # List only recent partitions
    INVENTORY = "inventory"          # Use S3 Inventory
    EVENT_DRIVEN = "event_driven"    # S3 Event Notifications

# Add to Ingestion model
class Ingestion(Base):
    listing_strategy = Column(SQLEnum(ListingStrategy), default=ListingStrategy.INCREMENTAL_TIME)
    listing_lookback_days = Column(Integer, default=7)  # For partitioned strategy
    inventory_bucket = Column(String, nullable=True)    # For inventory strategy
```

**UI/API**:
- Wizard: "How are your files organized?"
  - Append-only, time-partitioned → PARTITIONED
  - Random writes, millions of files → INVENTORY
  - Real-time requirements → EVENT_DRIVEN
  - Default → INCREMENTAL_TIME

---

### 7. Performance Metrics & Monitoring

#### 7.1 Listing Performance Tracking
**Concept**: Measure and expose listing performance metrics

```python
@dataclass
class ListingMetrics:
    """Track listing performance"""
    total_files_discovered: int
    total_api_calls: int
    listing_duration_seconds: float
    files_per_second: float
    new_files_count: int
    already_processed_count: int
    database_query_duration_seconds: float
    s3_api_duration_seconds: float

# Store in Run model
class Run(Base):
    listing_metrics = Column(JSON, nullable=True)
```

**Monitoring**:
- Alert if listing takes >5 minutes
- Track API call count trends
- Identify slow database queries

---

## Implementation Priority

### Phase 1: Quick Wins (1-2 weeks)
1. **Batch Database Queries** - Already partially done, optimize further
2. **Incremental Time-Based Listing** - Make `since` parameter default
3. **Parallel Listing** - Add ThreadPoolExecutor for prefix sharding
4. **Listing Metrics** - Add performance tracking

**Expected Impact**: 10-50x speedup for typical workloads

---

### Phase 2: Medium Effort (1 month)
1. **Prefix Partitioning Strategy** - Add partition pattern support
2. **Bloom Filters** - Add probabilistic filtering
3. **ETag Caching** - In-memory cache for change detection
4. **Checkpoint-Based Listing** - Store last processed key

**Expected Impact**: 100x+ speedup for partitioned data

---

### Phase 3: Advanced (2-3 months)
1. **S3 Inventory Support** - For million+ file buckets
2. **Redis Caching** - Distributed cache layer
3. **Event-Driven Architecture** - S3 notifications
4. **Configurable Strategies** - User-selectable listing modes

**Expected Impact**: Handle billions of files, real-time ingestion

---

## Benchmark Scenarios

### Scenario 1: Small Dataset (10K files)
- **Current**: ~30 seconds
- **Phase 1**: ~3 seconds (10x)
- **Phase 2**: ~1 second (30x)

### Scenario 2: Medium Dataset (100K files)
- **Current**: ~5 minutes
- **Phase 1**: ~30 seconds (10x)
- **Phase 2**: ~5 seconds (60x)

### Scenario 3: Large Dataset (1M files)
- **Current**: ~50 minutes (600+ API calls)
- **Phase 1**: ~5 minutes (10x)
- **Phase 2**: ~30 seconds (100x with partitioning)
- **Phase 3**: ~10 seconds (300x with inventory)

### Scenario 4: Massive Dataset (10M+ files)
- **Current**: Hours, potentially timeout
- **Phase 3**: <1 minute (with S3 Inventory + Spark processing)

---

## Recommendations

### For Most Users (General Purpose):
1. Enable **Incremental Time-Based Listing** by default
2. Implement **Batch Database Queries**
3. Add **Parallel Listing** for subdirectories
4. Track **Listing Metrics** for visibility

### For Time-Partitioned Data Lakes:
1. Use **Partitioned Listing Strategy**
2. Configure lookback window (7-30 days)
3. Enable **Checkpoint-Based Resumption**

### For Million+ File Buckets:
1. Use **S3 Inventory**
2. Process inventory with Spark (already available!)
3. Consider **Redis Cache** for multi-worker setups

### For Real-Time Use Cases:
1. Implement **Event-Driven Architecture** (S3 → SNS → SQS)
2. Complement with periodic full scans (daily) for reliability

---

## Code Changes Required

### 1. Enhanced FileDiscoveryService

```python
class FileDiscoveryService:
    def list_files_smart(
        self,
        bucket: str,
        prefix: str,
        strategy: ListingStrategy,
        checkpoint: Optional[dict] = None,
        **options
    ) -> Tuple[List[FileMetadata], dict]:
        """
        Smart listing with strategy selection

        Returns:
            (files, new_checkpoint)
        """
        if strategy == ListingStrategy.INCREMENTAL_TIME:
            return self._list_incremental_time(bucket, prefix, checkpoint)
        elif strategy == ListingStrategy.PARTITIONED:
            return self._list_partitioned(bucket, prefix, options)
        elif strategy == ListingStrategy.INVENTORY:
            return self._list_from_inventory(bucket, options)
        else:
            return self._list_full(bucket, prefix)
```

### 2. Enhanced ProcessedFileRepository

```python
class ProcessedFileRepository:
    def get_processed_paths_batch(
        self,
        ingestion_id: str,
        file_paths: List[str],
        batch_size: int = 10000
    ) -> Set[str]:
        """
        Batch query with chunking for large lists
        """
        processed = set()

        for i in range(0, len(file_paths), batch_size):
            chunk = file_paths[i:i+batch_size]
            results = self.db.query(ProcessedFile.file_path).filter(
                ProcessedFile.ingestion_id == ingestion_id,
                ProcessedFile.file_path.in_(chunk),
                ProcessedFile.status.in_(['SUCCESS', 'SKIPPED'])
            ).all()
            processed.update(r.file_path for r in results)

        return processed
```

---

## Testing Strategy

1. **Benchmark Tests**: Create test buckets with 10K, 100K, 1M files
2. **Comparison Tests**: Measure before/after for each optimization
3. **Load Tests**: Ensure optimizations scale under concurrent load
4. **Correctness Tests**: Verify no files are missed or double-processed

---

## Conclusion

Directory listing optimization is **critical for production scalability**. The current implementation works for small datasets (<10K files) but becomes a bottleneck as data grows.

**Recommended Action Plan**:
1. Implement Phase 1 optimizations immediately (quick wins)
2. Add configurable strategies for Phase 2
3. Evaluate Phase 3 based on user feedback and scale requirements

**Key Insight**: Different data patterns require different strategies. Providing configurable options gives users the flexibility to optimize for their specific use case.
