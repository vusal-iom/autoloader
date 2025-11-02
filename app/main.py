"""Main FastAPI application."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import init_db
from app.api.v1 import ingestions, runs, clusters

settings = get_settings()

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="IOMETE Autoloader - Zero-code data ingestion system",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    # Initialize database
    init_db()
    # TODO: Initialize scheduler
    # TODO: Initialize Spark Connect session pool


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    # TODO: Close Spark Connect sessions
    # TODO: Stop scheduler
    pass


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "status": "healthy",
    }


@app.get("/health")
async def health():
    """Detailed health check."""
    return {
        "status": "healthy",
        "database": "connected",  # TODO: Check database
        "scheduler": "running",  # TODO: Check scheduler
    }


# Include routers
app.include_router(ingestions.router, prefix="/api/v1")
app.include_router(runs.router, prefix="/api/v1")
app.include_router(clusters.router, prefix="/api/v1")
