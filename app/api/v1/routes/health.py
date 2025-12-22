"""Health check endpoint."""
from fastapi import APIRouter, Response, status
from typing import Dict, Any

from app.services.health_service import health_service, HealthStatus

router = APIRouter()


@router.get("/health", tags=["health"])
async def health_check(response: Response) -> Dict[str, Any]:
    """
    Check system health status.
    
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
