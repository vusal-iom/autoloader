# IOMETE Autoloader - Project Documentation

This directory contains detailed technical documentation about the IOMETE Autoloader project architecture, patterns, and development practices.

## Documentation Index

### Getting Started

- **[Project Structure](./project-structure.md)** - ⭐ **Start here!** Complete guide to project organization
  - Directory layout and architecture
  - Layered architecture explanation
  - Where to find things
  - How components interact
  - Naming conventions
  - Development workflow

### Development Guides

- **[Alembic Guide](./alembic-guide.md)** - Database migrations with Alembic
- **[Running Integration Tests in PyCharm](./running-integration-tests-in-pycharm.md)** - Setting up and running tests in PyCharm

### Architecture & Patterns

- **[Pydantic Usage](./pydantic-usage.md)** - How Pydantic is used for data validation and API schemas
  - API request/response models
  - Validation patterns
  - Nested schemas
  - FastAPI integration
  - Best practices

- **[Configuration Management](./configuration-management.md)** - Application configuration with Pydantic Settings
  - Settings architecture
  - Environment variables
  - Configuration sources and precedence
  - Environment-specific configuration
  - Security best practices

## Quick Links

### Related Documentation

- [Project README](../../README.md) - Getting started and overview
- [Product Requirements](../ingestion-prd-v1.md) - Comprehensive PRD
- [Spark Connect Architecture](../ingestion-prd-using-connect.md) - Spark Connect integration
- [Batch Processing](../batch-processing/) - Batch processing implementation guides

### Configuration Files

- `.env.example` - Environment configuration template (copy to `.env`)
- `app/config.py` - Settings class implementation
- `app/models/schemas.py` - Pydantic API schemas
- `app/models/domain.py` - SQLAlchemy database models

## Documentation Coverage

### Covered Topics

✅ **Project Structure** - Complete guide to how the codebase is organized
✅ **Pydantic Usage** - Complete guide to Pydantic patterns in the project
✅ **Configuration Management** - Environment variables, settings, and configuration sources
✅ **Database Migrations** - Alembic setup and usage
✅ **Testing** - PyCharm integration test setup

### Future Documentation

The following topics would benefit from dedicated documentation:

🔜 **SQLAlchemy Patterns** - ORM usage, relationships, and query patterns (partially covered in project structure)
🔜 **Repository Pattern** - Data access layer implementation (partially covered in project structure)
🔜 **Service Layer** - Business logic organization (partially covered in project structure)
🔜 **FastAPI Patterns** - API structure, dependency injection, error handling (partially covered in project structure)
🔜 **Spark Connect Integration** - Connection management, session pooling
🔜 **Testing Strategy** - Unit tests, integration tests, fixtures
🔜 **Deployment Guide** - Production deployment and configuration

## Contributing to Documentation

When adding new documentation:

1. **Follow the existing structure** - Use similar formatting and sections
2. **Include code examples** - Demonstrate concepts with real project code
3. **Cross-reference** - Link to related documentation
4. **Update this README** - Add new documents to the index
5. **Keep it current** - Update docs when code changes

### Documentation Standards

- Use Markdown for all documentation
- Include table of contents for long documents
- Provide code examples from the actual codebase
- Explain the "why" not just the "how"
- Include troubleshooting sections where applicable
- Reference specific file paths and line numbers when helpful

## Questions or Suggestions?

If you find gaps in the documentation or have suggestions for improvements, please:
- Open an issue describing what you'd like to see documented
- Contribute documentation via pull request
- Ask questions in team discussions

## Document Metadata

Last updated: 2024-11-05
