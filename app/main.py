from pathlib import Path
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from app.api.v1.routes.attractions import router as attractions_router
from app.api.v1.routes.pipeline import router as pipeline_router
from app.api.v1.routes.frontend import router as frontend_router
from app.api.v1.routes.health import router as health_router
from app.api.v1.routes.admin import router as admin_router
from app.api.v1.routes.sitemap import router as sitemap_router
from app.api.pipeline_tracking_routes import router as tracking_router
from app.core.database_init import initialize_database

logger = logging.getLogger(__name__)

# Setup Jinja2 templates
templates = Jinja2Templates(directory="app/templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    logger.info("Starting up application...")
    
    # Initialize database
    try:
        initialize_database()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        # Continue anyway - app can still run without DB
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")


def create_app() -> FastAPI:
    """Create FastAPI application and include routers."""
    app = FastAPI(
        title="Storyboard Backend",
        version="0.1.0",
        lifespan=lifespan
    )
    
    # Add CORS middleware to allow frontend connections
    # Allow common development ports and local network access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:3001",
            "http://127.0.0.1:3001",
            "http://localhost:3002",
            "http://127.0.0.1:3002",
            "http://10.80.121.45:3000",
            "http://10.80.121.45:3001",
            "http://10.80.121.45:3002",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.include_router(attractions_router, prefix="/api/v1")
    app.include_router(pipeline_router, prefix="/api/v1")
    app.include_router(frontend_router, prefix="/api/v1")
    app.include_router(health_router, prefix="/api/v1")
    app.include_router(admin_router, prefix="/api/v1")
    app.include_router(sitemap_router, prefix="/api/v1")
    app.include_router(tracking_router)  # No prefix - routes already include /api/pipeline
    return app


app = create_app()


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/admin/attractions", response_class=HTMLResponse)
async def admin_attractions_page(request: Request):
    """Admin page for viewing attraction data.

    Args:
        request: FastAPI request object

    Returns:
        HTML template response
    """
    return templates.TemplateResponse(
        "admin_attractions.html",
        {"request": request}
    )


