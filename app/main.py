"""Main FastAPI application."""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import init_db
from app.api.v1 import ingestions, runs, refresh

logger = logging.getLogger(__name__)
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
    logger.info("🚀 Starting IOMETE Autoloader")

    # Initialize database
    init_db()
    logger.info("✅ Database initialized")

    # Initialize Prefect service
    try:
        from app.services.prefect_service import get_prefect_service
        prefect = await get_prefect_service()
        if prefect.initialized:
            logger.info("✅ Prefect service initialized and connected")
        else:
            logger.warning("⚠️  Prefect service initialized but not connected (will retry)")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Prefect service: {e}")
        logger.warning("Prefect features will be unavailable until connection is restored")

    # TODO: Initialize Spark Connect session pool


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("🛑 Shutting down IOMETE Autoloader")
    # TODO: Close Spark Connect sessions
    # TODO: Cleanup Prefect connections
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
    # Check Prefect connectivity
    prefect_status = "unknown"
    try:
        from app.services.prefect_service import get_prefect_service_sync
        prefect = get_prefect_service_sync()
        prefect_status = "connected" if prefect.initialized else "disconnected"
    except Exception:
        prefect_status = "error"

    return {
        "status": "healthy",
        "database": "connected",  # TODO: Check database
        "prefect": prefect_status,
    }


# Include routers
app.include_router(ingestions.router, prefix="/api/v1")
app.include_router(runs.router, prefix="/api/v1")
app.include_router(refresh.router)
