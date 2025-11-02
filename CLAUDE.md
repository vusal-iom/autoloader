# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the IOMETE Autoloader project - a zero-code, UI-driven data ingestion system. Currently in the planning phase with comprehensive documentation and UI mockups ready for implementation.

## Key Documentation

- **PRD**: `docs/ingestion-prd-v1.md` - Complete product requirements (1355+ lines)
- **Architecture**: `docs/ingestion-prd-using-connect.md` - Spark Connect architecture design
- **UI/UX Specs**: `docs/autoloaderv1.md` - Visual design specifications
- **Data Models**: `docs/code-examples/version1.py` - Python API models using Pydantic/SQLAlchemy
- **UI Mockups**: `docs/mocks/` - Interactive HTML prototypes

## Architecture Overview

### Core Components
1. **Ingestion Service**: Core business logic (Python/FastAPI)
2. **Spark Connect**: Remote job execution without JAR deployment
3. **Database**: PostgreSQL for metadata storage
4. **Storage**: Multi-cloud support (S3, Azure Blob, GCS)

### Technology Stack
- **Backend**: Python, FastAPI, SQLAlchemy, Pydantic
- **Processing**: Apache Spark with Delta Lake
- **Frontend**: React (planned)
- **Database**: PostgreSQL

### Key Patterns
- Repository pattern for data access
- Strategy pattern for cloud storage providers
- Builder pattern for Spark job generation
- Event-driven schema evolution detection

## Development Commands

**Note**: This project is in planning phase. No build/test commands are configured yet.

When implementation begins, expected commands will be:
- Python virtual environment setup
- FastAPI development server
- Spark Connect integration tests
- Database migrations

## API Design

The planned API structure (see `docs/code-examples/version1.py`):
- RESTful endpoints for ingestion management
- Comprehensive data models for all entities
- Support for S3, Azure Blob, and GCS configurations
- Schema evolution tracking

## Important Considerations

1. **Multi-tenancy**: Each lakehouse gets isolated ingestion jobs
2. **Cost Estimation**: Built-in cost prediction before job execution
3. **Schema Evolution**: Automatic detection and user approval workflow
4. **Performance**: Optimized for datasets 100GB-10TB daily
5. **Security**: Service account authentication, no credentials storage

## Current Status

- Phase 0: Planning (Complete)
- Phase 1: MVP Implementation (Ready to start)
- Phase 2: Production Features (Months 4-6)
- Phase 3: Advanced Features (Months 7-12)