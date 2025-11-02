# IOMETE Autoloader - Backend

A zero-code, UI-driven data ingestion system for IOMETE that enables automatic file loading from cloud storage into IOMETE tables using Spark Connect.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Backend                      │
│                                                         │
│  API Layer → Service Layer → Spark Connect → Cluster  │
│      ↓                                                  │
│  Repository Layer → Database (PostgreSQL/SQLite)       │
└─────────────────────────────────────────────────────────┘
```

## Key Features

- **Spark Connect Integration**: Direct connection to user's IOMETE clusters via Spark Connect protocol
- **Session Pooling**: Efficient reuse of Spark sessions across ingestions
- **Auto Loader**: Leverages Spark's cloudFiles for incremental file ingestion
- **Schema Evolution**: Automatic detection and handling of schema changes
- **Cost Estimation**: Upfront cost calculation before creating ingestions
- **Preview Mode**: Test configurations without committing
- **Multi-Cloud Support**: AWS S3, Azure Blob Storage, Google Cloud Storage

## Project Structure

```
app/
├── main.py                 # FastAPI application entry point
├── config.py              # Configuration management
├── database.py            # Database setup and session management
│
├── api/v1/                # API endpoints
│   ├── ingestions.py      # Ingestion CRUD operations
│   ├── runs.py            # Run history
│   └── clusters.py        # Cluster management
│
├── models/                # Data models
│   ├── domain.py          # SQLAlchemy models (database)
│   └── schemas.py         # Pydantic models (API)
│
├── services/              # Business logic
│   ├── ingestion_service.py   # Ingestion operations
│   ├── spark_service.py       # Spark operations wrapper
│   └── cost_estimator.py      # Cost calculation
│
├── repositories/          # Database operations
│   ├── ingestion_repository.py
│   └── run_repository.py
│
└── spark/                 # Spark Connect integration
    ├── connect_client.py      # Spark Connect client
    ├── session_manager.py     # Session pooling
    └── executor.py            # Ingestion executor
```

## Tech Stack

- **Framework**: FastAPI
- **ORM**: SQLAlchemy
- **Validation**: Pydantic
- **Processing**: Apache Spark (via Spark Connect)
- **Database**: PostgreSQL (SQLite for development)
- **Cloud SDKs**: boto3 (S3), azure-storage-blob, google-cloud-storage

## Getting Started

### Prerequisites

- Python 3.9+
- IOMETE cluster with Spark Connect enabled (Spark 3.5+)
- PostgreSQL database (or use SQLite for development)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd autoloader
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize database:
```bash
# Database will be auto-initialized on first run
# Or manually run:
python -c "from app.database import init_db; init_db()"
```

### Running the Application

Development mode:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Production mode:
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### API Documentation

Once running, access:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API Endpoints

### Ingestions

- `POST /api/v1/ingestions` - Create new ingestion
- `GET /api/v1/ingestions` - List all ingestions
- `GET /api/v1/ingestions/{id}` - Get ingestion details
- `PUT /api/v1/ingestions/{id}` - Update ingestion
- `DELETE /api/v1/ingestions/{id}` - Delete ingestion
- `POST /api/v1/ingestions/{id}/run` - Trigger manual run
- `POST /api/v1/ingestions/{id}/pause` - Pause ingestion
- `POST /api/v1/ingestions/{id}/resume` - Resume ingestion
- `POST /api/v1/ingestions/test` - Preview/test configuration
- `POST /api/v1/ingestions/estimate-cost` - Estimate costs

### Runs

- `GET /api/v1/ingestions/{id}/runs` - Get run history
- `GET /api/v1/ingestions/{id}/runs/{run_id}` - Get run details
- `POST /api/v1/ingestions/{id}/runs/{run_id}/retry` - Retry failed run

### Clusters

- `GET /api/v1/clusters` - List available clusters
- `GET /api/v1/clusters/{id}` - Get cluster details
- `POST /api/v1/clusters/{id}/test-connection` - Test connectivity

## Configuration

Key configuration options in `.env`:

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost/autoloader

# Spark Connect
SPARK_CONNECT_DEFAULT_PORT=15002
SPARK_SESSION_POOL_SIZE=10

# Checkpoints
CHECKPOINT_BASE_PATH=s3://iomete-checkpoints/

# Cost Rates
COST_PER_DBU_HOUR=0.40
STORAGE_COST_PER_GB=0.023
```

## Core Concepts

### Ingestion Workflow

1. **Create Configuration**: Define source, format, destination, and schedule
2. **Preview**: Test with sample data before activating
3. **Activate**: Start scheduled or continuous ingestion
4. **Monitor**: Track runs, metrics, and health
5. **Manage**: Pause, resume, edit, or delete

### Spark Connect Integration

The backend uses Spark Connect to:
- Connect directly to user's IOMETE clusters
- Execute ingestion operations remotely
- Leverage existing cluster resources
- Provide real-time progress updates

### Session Pooling

Sessions are pooled for efficiency:
- Reuse connections across ingestions
- Automatic cleanup of idle sessions
- Configurable pool size and timeout

### Cost Estimation

Estimates based on:
- File count and size analysis
- Processing frequency
- Cluster size and pricing
- Storage and API operation costs

## Development

### Adding a New Feature

1. Define models in `models/schemas.py` (API) and `models/domain.py` (database)
2. Create repository methods in `repositories/`
3. Implement business logic in `services/`
4. Add API endpoints in `api/v1/`
5. Update tests

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
# Format code
black app/

# Lint
flake8 app/

# Type checking
mypy app/
```

## Deployment

### Docker

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Environment Variables

Production deployment requires:
- `DATABASE_URL`: PostgreSQL connection string
- `SECRET_KEY`: Strong secret for JWT tokens
- `CHECKPOINT_BASE_PATH`: S3/Azure/GCS path for checkpoints
- Cloud credentials (AWS_ACCESS_KEY_ID, etc.)

## Monitoring

- Health check: `GET /health`
- Metrics: Prometheus metrics available at `/metrics` (TODO)
- Logs: Structured logging to stdout

## Next Steps (TODO)

- [ ] Add authentication/authorization
- [ ] Implement scheduler for automated runs
- [ ] Add schema evolution handling UI
- [ ] Implement alert/notification system
- [ ] Add data quality checks
- [ ] Support for merge/upsert mode
- [ ] Add transformation support (light SQL)
- [ ] Metrics and monitoring dashboard
- [ ] Integration tests with real Spark cluster

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Submit pull request

## License

[Add your license here]

## Support

For issues and questions:
- GitHub Issues: [Add URL]
- Documentation: [Add URL]
- Slack: [Add channel]
