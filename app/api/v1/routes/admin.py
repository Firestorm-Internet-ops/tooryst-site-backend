"""Admin endpoints for viewing attraction data."""
import math
from typing import Optional, Dict, Any, List
from datetime import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import or_, func, select, exists
from sqlalchemy.orm import Session

from app.api.dependencies import verify_admin_key
from app.api.v1.schemas.admin_schemas import (
    AttractionListResponseSchema,
    AttractionListItemSchema,
    AttractionCompleteDataSchema,
    HasDataSchema,
    CompletenessSchema,
    PaginationSchema
)
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models


router = APIRouter(prefix="/admin", tags=["admin"])


def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def model_to_dict(obj: Any) -> Optional[Dict[str, Any]]:
    """Convert SQLAlchemy model to dict.

    Args:
        obj: SQLAlchemy model instance

    Returns:
        Dict representation of model or None if obj is None
    """
    if obj is None:
        return None

    result = {}
    for column in obj.__table__.columns:
        value = getattr(obj, column.name)

        # Handle different data types
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, bytes):
            value = value.decode('utf-8')

        result[column.name] = value

    return result


def models_to_list(objs: List[Any]) -> Optional[List[Dict[str, Any]]]:
    """Convert list of SQLAlchemy models to list of dicts.

    Args:
        objs: List of SQLAlchemy model instances

    Returns:
        List of dict representations or None if empty
    """
    if not objs:
        return None

    return [model_to_dict(obj) for obj in objs]


def calculate_completeness(has_data: HasDataSchema) -> CompletenessSchema:
    """Calculate data completeness score for an attraction.

    Args:
        has_data: Data counts and flags for the attraction

    Returns:
        Completeness information with score, percentage, and status
    """
    # Data types that contribute to completeness score
    data_types = [
        has_data.hero_images > 0,
        has_data.reviews > 0,
        has_data.best_time > 0,
        has_data.weather > 0,
        has_data.tips > 0,
        has_data.social_videos > 0,
        has_data.nearby > 0,
        has_data.audiences > 0,
        has_data.has_map,
        has_data.has_metadata
    ]

    complete_sections = sum(data_types)
    total_sections = len(data_types)
    completeness_score = complete_sections
    completeness_percentage = (complete_sections / total_sections) * 100

    # Determine status
    if completeness_score == 10:
        status = "complete"
    elif completeness_score >= 7:
        status = "partial"
    else:
        status = "incomplete"

    return CompletenessSchema(
        score=completeness_score,
        percentage=round(completeness_percentage, 1),
        status=status
    )


@router.get(
    "/attractions",
    response_model=AttractionListResponseSchema
)
async def list_attractions(
    city: Optional[str] = Query(None, description="Filter by city slug"),
    search: Optional[str] = Query(None, description="Search attraction name/slug"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db)
):
    """List all attractions with filters and pagination.

    Args:
        city: Filter by city slug (optional)
        search: Search by attraction name or slug (optional)
        page: Page number (default 1)
        per_page: Items per page (default 20, max 100)
        db: Database session

    Returns:
        List of attractions with pagination metadata
    """
    # Create scalar subqueries for all counts (avoids N+1 query problem)
    hero_count_sq = (
        select(func.count(models.HeroImage.id))
        .where(models.HeroImage.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    review_count_sq = (
        select(func.count(models.Review.id))
        .where(models.Review.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    best_time_count_sq = (
        select(func.count(models.BestTimeData.id))
        .where(models.BestTimeData.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    weather_count_sq = (
        select(func.count(models.WeatherForecast.id))
        .where(models.WeatherForecast.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    tips_count_sq = (
        select(func.count(models.Tip.id))
        .where(models.Tip.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    videos_count_sq = (
        select(func.count(models.SocialVideo.id))
        .where(models.SocialVideo.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    nearby_count_sq = (
        select(func.count(models.NearbyAttraction.id))
        .where(models.NearbyAttraction.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    audiences_count_sq = (
        select(func.count(models.AudienceProfile.id))
        .where(models.AudienceProfile.attraction_id == models.Attraction.id)
        .scalar_subquery()
    )

    has_map_sq = (
        select(1)
        .where(models.MapSnapshot.attraction_id == models.Attraction.id)
        .exists()
    )

    has_metadata_sq = (
        select(1)
        .where(models.AttractionMetadata.attraction_id == models.Attraction.id)
        .exists()
    )

    has_widget_sq = (
        select(1)
        .where(models.WidgetConfig.attraction_id == models.Attraction.id)
        .exists()
    )

    # Build base query with all counts in single query
    query = db.query(
        models.Attraction.id,
        models.Attraction.slug,
        models.Attraction.name,
        models.City.name.label('city'),
        models.City.country,
        hero_count_sq.label('hero_count'),
        review_count_sq.label('review_count'),
        best_time_count_sq.label('best_time_count'),
        weather_count_sq.label('weather_count'),
        tips_count_sq.label('tips_count'),
        videos_count_sq.label('videos_count'),
        nearby_count_sq.label('nearby_count'),
        audiences_count_sq.label('audiences_count'),
        has_map_sq.label('has_map'),
        has_metadata_sq.label('has_metadata'),
        has_widget_sq.label('has_widget')
    ).join(models.City, models.Attraction.city_id == models.City.id)

    # Apply filters
    if city:
        query = query.filter(models.City.slug == city)

    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                models.Attraction.name.ilike(search_term),
                models.Attraction.slug.ilike(search_term)
            )
        )

    # Get total count
    total = query.count()

    # Calculate pagination
    pages = math.ceil(total / per_page) if total > 0 else 1
    skip = (page - 1) * per_page

    # Get page of results (single query with all counts)
    results = query.order_by(models.Attraction.name).offset(skip).limit(per_page).all()

    # Build response directly from query results (no additional queries needed)
    attraction_list = []
    for result in results:
        has_data = HasDataSchema(
            hero_images=result.hero_count or 0,
            reviews=result.review_count or 0,
            best_time=result.best_time_count or 0,
            weather=result.weather_count or 0,
            tips=result.tips_count or 0,
            social_videos=result.videos_count or 0,
            nearby=result.nearby_count or 0,
            audiences=result.audiences_count or 0,
            has_map=bool(result.has_map),
            has_metadata=bool(result.has_metadata),
            has_widget=bool(result.has_widget)
        )

        completeness = calculate_completeness(has_data)

        attraction_list.append(
            AttractionListItemSchema(
                id=result.id,
                slug=result.slug,
                name=result.name,
                city=result.city,
                country=result.country,
                has_data=has_data,
                completeness=completeness
            )
        )

    return AttractionListResponseSchema(
        attractions=attraction_list,
        pagination=PaginationSchema(
            page=page,
            per_page=per_page,
            total=total,
            pages=pages
        )
    )


@router.get(
    "/attractions/{attraction_id}/data",
    response_model=AttractionCompleteDataSchema
)
async def get_attraction_data(
    attraction_id: int,
    db: Session = Depends(get_db)
):
    """Get complete attraction data grouped by table.

    Args:
        attraction_id: ID of the attraction
        db: Database session

    Returns:
        Complete attraction data from all related tables

    Raises:
        HTTPException: 404 if attraction not found
    """
    # Get main attraction
    attraction = db.query(models.Attraction).filter(
        models.Attraction.id == attraction_id
    ).first()

    if not attraction:
        raise HTTPException(status_code=404, detail="Attraction not found")

    # Get city
    city = db.query(models.City).filter(
        models.City.id == attraction.city_id
    ).first()

    # Get all related data
    hero_images = db.query(models.HeroImage).filter(
        models.HeroImage.attraction_id == attraction_id
    ).order_by(models.HeroImage.position).all()

    best_time = db.query(models.BestTimeData).filter(
        models.BestTimeData.attraction_id == attraction_id
    ).order_by(models.BestTimeData.date_local).all()

    weather = db.query(models.WeatherForecast).filter(
        models.WeatherForecast.attraction_id == attraction_id
    ).order_by(models.WeatherForecast.date_local).all()

    reviews = db.query(models.Review).filter(
        models.Review.attraction_id == attraction_id
    ).all()

    tips = db.query(models.Tip).filter(
        models.Tip.attraction_id == attraction_id
    ).order_by(models.Tip.position).all()

    map_data = db.query(models.MapSnapshot).filter(
        models.MapSnapshot.attraction_id == attraction_id
    ).first()

    metadata = db.query(models.AttractionMetadata).filter(
        models.AttractionMetadata.attraction_id == attraction_id
    ).first()

    videos = db.query(models.SocialVideo).filter(
        models.SocialVideo.attraction_id == attraction_id
    ).order_by(models.SocialVideo.position).all()

    nearby = db.query(models.NearbyAttraction).filter(
        models.NearbyAttraction.attraction_id == attraction_id
    ).all()

    audiences = db.query(models.AudienceProfile).filter(
        models.AudienceProfile.attraction_id == attraction_id
    ).all()

    widget = db.query(models.WidgetConfig).filter(
        models.WidgetConfig.attraction_id == attraction_id
    ).first()

    # Build response grouped by table
    return AttractionCompleteDataSchema(
        attractions=model_to_dict(attraction),
        cities=model_to_dict(city),
        hero_images=models_to_list(hero_images),
        best_time_data=models_to_list(best_time),
        weather_forecast=models_to_list(weather),
        reviews=models_to_list(reviews),
        tips=models_to_list(tips),
        map_snapshot=model_to_dict(map_data),
        attraction_metadata=model_to_dict(metadata),
        social_videos=models_to_list(videos),
        nearby_attractions=models_to_list(nearby),
        audience_profiles=models_to_list(audiences),
        widget_config=model_to_dict(widget)
    )



# ============================================================================
# NEARBY ATTRACTIONS MANAGEMENT ENDPOINTS
# ============================================================================

@router.post("/nearby-attractions/refresh/{attraction_id}")
async def refresh_nearby_attractions_for_attraction(
    attraction_id: int,
    _: bool = Depends(verify_admin_key),
    db: Session = Depends(get_db)
):
    """Manually trigger nearby attractions refresh for a specific attraction.
    
    This endpoint queues a Celery task to fetch and update nearby attractions
    for the specified attraction.
    
    Args:
        attraction_id: ID of the attraction to refresh nearby attractions for
        
    Returns:
        Task ID and status
    """
    from app.tasks.nearby_attractions_tasks import update_nearby_attractions_for_attraction
    
    # Verify attraction exists
    attraction = db.query(models.Attraction).filter(
        models.Attraction.id == attraction_id
    ).first()
    
    if not attraction:
        raise HTTPException(status_code=404, detail=f"Attraction {attraction_id} not found")
    
    # Queue the task
    task = update_nearby_attractions_for_attraction.delay(attraction_id)
    
    return {
        "status": "queued",
        "task_id": task.id,
        "attraction_id": attraction_id,
        "attraction_name": attraction.name,
        "message": f"Nearby attractions refresh queued for {attraction.name}"
    }


@router.post("/nearby-attractions/refresh-city/{city_id}")
async def refresh_nearby_attractions_for_city(
    city_id: int,
    _: bool = Depends(verify_admin_key),
    db: Session = Depends(get_db)
):
    """Manually trigger nearby attractions refresh for all attractions in a city.
    
    This endpoint queues Celery tasks to fetch and update nearby attractions
    for all attractions in the specified city.
    
    Args:
        city_id: ID of the city
        
    Returns:
        Task IDs and status
    """
    from app.tasks.nearby_attractions_tasks import update_nearby_attractions_for_city
    
    # Verify city exists
    city = db.query(models.City).filter(
        models.City.id == city_id
    ).first()
    
    if not city:
        raise HTTPException(status_code=404, detail=f"City {city_id} not found")
    
    # Queue the task
    task = update_nearby_attractions_for_city.delay(city_id)
    
    return {
        "status": "queued",
        "task_id": task.id,
        "city_id": city_id,
        "city_name": city.name,
        "message": f"Nearby attractions refresh queued for all attractions in {city.name}"
    }


@router.post("/nearby-attractions/refresh-all")
async def refresh_all_nearby_attractions(
    _: bool = Depends(verify_admin_key)
):
    """Manually trigger periodic nearby attractions refresh for all attractions.
    
    This endpoint queues a Celery task to find and refresh nearby attractions
    for all attractions that need it (missing data, stale data, or below threshold).
    
    Returns:
        Task ID and status
    """
    from app.tasks.nearby_attractions_tasks import refresh_all_nearby_attractions
    
    # Queue the task
    task = refresh_all_nearby_attractions.delay()
    
    return {
        "status": "queued",
        "task_id": task.id,
        "message": "Periodic nearby attractions refresh queued for all attractions needing update"
    }


@router.post("/nearby-attractions/backfill")
async def backfill_nearby_attractions(
    batch_size: int = Query(10, ge=1, le=100),
    _: bool = Depends(verify_admin_key)
):
    """Manually trigger backfill of nearby attractions for attractions without them.
    
    This endpoint queues a Celery task to backfill nearby attractions data
    for attractions that don't have any nearby attractions yet.
    
    Args:
        batch_size: Number of attractions to process in this batch (1-100, default 10)
        
    Returns:
        Task ID and status
    """
    from app.tasks.nearby_attractions_tasks import backfill_nearby_attractions
    
    # Queue the task
    task = backfill_nearby_attractions.delay(batch_size=batch_size)
    
    return {
        "status": "queued",
        "task_id": task.id,
        "batch_size": batch_size,
        "message": f"Backfill of nearby attractions queued (batch_size={batch_size})"
    }


@router.get("/nearby-attractions/status/{attraction_id}")
async def get_nearby_attractions_status(
    attraction_id: int,
    _: bool = Depends(verify_admin_key),
    db: Session = Depends(get_db)
):
    """Get nearby attractions status for an attraction.
    
    Returns count and details of nearby attractions for the specified attraction.
    
    Args:
        attraction_id: ID of the attraction
        
    Returns:
        Nearby attractions count and details
    """
    # Verify attraction exists
    attraction = db.query(models.Attraction).filter(
        models.Attraction.id == attraction_id
    ).first()
    
    if not attraction:
        raise HTTPException(status_code=404, detail=f"Attraction {attraction_id} not found")
    
    # Get nearby attractions
    nearby = db.query(models.NearbyAttraction).filter(
        models.NearbyAttraction.attraction_id == attraction_id
    ).all()
    
    # Count by type
    db_attractions = sum(1 for n in nearby if n.nearby_attraction_id is not None)
    google_attractions = sum(1 for n in nearby if n.nearby_attraction_id is None)
    
    return {
        "attraction_id": attraction_id,
        "attraction_name": attraction.name,
        "total_nearby": len(nearby),
        "from_database": db_attractions,
        "from_google_places": google_attractions,
        "nearby_attractions": [
            {
                "id": n.id,
                "name": n.name,
                "distance_km": float(n.distance_km) if n.distance_km else None,
                "rating": float(n.rating) if n.rating else None,
                "review_count": n.review_count,
                "source": "database" if n.nearby_attraction_id else "google_places",
                "has_image": n.image_url is not None,
                "has_link": n.link is not None
            }
            for n in nearby
        ]
    }
