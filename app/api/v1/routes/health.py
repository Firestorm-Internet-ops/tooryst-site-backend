"""Health check endpoints."""
from fastapi import APIRouter, Response, status
from typing import Dict, Any

from app.services.health_service import health_service, HealthStatus

router = APIRouter()


@router.get("/health", tags=["health"])
async def simple_health_check() -> Dict[str, str]:
    """
    Simple health check for load balancer - no dependency checks.
    
    Returns HTTP 200 OK if the application is running.
    This endpoint is lightweight and fast for load balancer health checks.
    """
    return {"status": "ok"}


@router.get("/health/detailed", tags=["health"])
async def detailed_health_check(response: Response) -> Dict[str, Any]:
    """
    Detailed health check with all dependency checks.
    
    Returns HTTP 200 if all components are healthy.
    Returns HTTP 503 if any component is unhealthy.
    
    Response includes:
    - Overall system status
    - Individual component statuses (database, Redis, Celery)
    - Last pipeline run information
    """
    health_data = health_service.get_overall_health()
    
    # Set HTTP status code based on health
    if health_data["status"] == HealthStatus.UNHEALTHY:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        response.status_code = status.HTTP_200_OK
    
    return health_data
