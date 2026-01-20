"""Frontend API routes for homepage, cities, and attractions."""
import logging
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Depends, status
from sqlalchemy.orm import Session
from app.infrastructure.persistence.db import SessionLocal, get_db
from pydantic import BaseModel
from sqlalchemy import func, desc, or_, case

from app.config import settings
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models

logger = logging.getLogger(__name__)

router = APIRouter(tags=["frontend"])


# Response Models
class AttractionSummary(BaseModel):
    """Summary of an attraction for lists."""
    id: int
    slug: str
    name: str
    city: str
    city_slug: str
    country: str
    hero_image: Optional[str] = None
    average_rating: Optional[float] = None
    total_reviews: Optional[int] = None
    summary_text: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class CityWithAttractions(BaseModel):
    """City with its attractions."""
    id: int
    name: str
    slug: str
    country: str
    attraction_count: int
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    attractions: List[AttractionSummary]


class CitySummary(BaseModel):
    """Summary of a city for lists."""
    id: int
    name: str
    slug: str
    country: str
    attraction_count: int
    lat: Optional[float] = None
    lng: Optional[float] = None


class HomepageResponse(BaseModel):
    """Homepage data."""
    featured_attractions: List[AttractionSummary]
    popular_cities: List[CitySummary]
    total_attractions: int
    total_cities: int


# Endpoints

@router.get("/homepage", response_model=HomepageResponse)
async def get_homepage():
    """Get homepage data with featured attractions and popular cities."""
    session = SessionLocal()
    try:
        import traceback
        # Get total counts
        total_attractions = session.query(func.count(models.Attraction.id)).scalar()
        total_cities = session.query(func.count(models.City.id)).scalar()
        
        # Get featured attractions
        featured = (
            session.query(
                models.Attraction,
                models.City,
                case(
                    (models.HeroImage.gcs_url_card.isnot(None), models.HeroImage.gcs_url_card),
                    else_=models.HeroImage.url
                ).label("hero_image")
            )
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) &
                (models.HeroImage.position == 1)
            )
            .order_by(
                desc(models.HeroImage.last_refreshed_at.isnot(None)),
                desc(models.HeroImage.last_refreshed_at),
                models.Attraction.name
            )
            .limit(settings.FEATURED_ATTRACTIONS_LIMIT)
            .all()
        )

        featured_attractions = []
        for attr, city, hero_img in featured:
            featured_attractions.append(AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city.name,
                city_slug=city.slug,
                country=city.country,
                hero_image=hero_img,
                average_rating=float(attr.rating) if attr.rating else None,
                total_reviews=attr.review_count if attr.review_count else 0,
                summary_text=attr.summary_gemini,
                latitude=float(attr.latitude) if attr.latitude else 0.0,
                longitude=float(attr.longitude) if attr.longitude else 0.0
            ))
        
        # Get popular cities (cities with most attractions)
        popular = (
            session.query(
                models.City,
                func.count(models.Attraction.id).label('count')
            )
            .join(models.Attraction, models.City.id == models.Attraction.city_id)
            .group_by(models.City.id)
            .order_by(desc('count'))
            .limit(10)
            .all()
        )
        
        popular_cities = [
            CitySummary(
                id=city.id,
                name=city.name,
                slug=city.slug,
                country=city.country,
                attraction_count=count,
                lat=float(city.latitude) if city.latitude else None,
                lng=float(city.longitude) if city.longitude else None
            )
            for city, count in popular
        ]
        
        return HomepageResponse(
            featured_attractions=featured_attractions,
            popular_cities=popular_cities,
            total_attractions=total_attractions or 0,
            total_cities=total_cities or 0
        )
    except Exception as e:
        import traceback
        logger.error(f"ERROR in homepage endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


class PaginatedCityResponse(BaseModel):
    """Paginated response for cities."""
    items: List[CitySummary]
    total: int
    skip: int = 0
    limit: Optional[int] = None


@router.get("/cities", response_model=PaginatedCityResponse)
async def get_cities(
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of results (max 1000)")
):
    """Get all cities with attraction counts.

    Default returns all cities, but limited to max 1000 to prevent memory issues.
    """
    session = SessionLocal()
    try:
        # Apply max limit for safety (prevent OOM if cities table grows large)
        MAX_CITIES_LIMIT = 1000
        effective_limit = min(limit, MAX_CITIES_LIMIT) if limit else MAX_CITIES_LIMIT

        query = (
            session.query(
                models.City,
                func.count(models.Attraction.id).label('count')
            )
            .outerjoin(models.Attraction, models.City.id == models.Attraction.city_id)
            .group_by(models.City.id)
            .order_by(models.City.name)
            .limit(effective_limit)
        )

        cities = query.all()
        
        # Get total count
        total = session.query(func.count(models.City.id)).scalar() or 0
        
        items = [
            CitySummary(
                id=city.id,
                name=city.name,
                slug=city.slug,
                country=city.country,
                attraction_count=count,
                lat=float(city.latitude) if city.latitude else None,
                lng=float(city.longitude) if city.longitude else None
            )
            for city, count in cities
        ]
        
        return PaginatedCityResponse(
            items=items,
            total=total,
            skip=0,
            limit=limit or total
        )
        
    except Exception as e:
        # Log error and return empty result
        import traceback
        logger.error(f"❌ Error fetching cities: {e}", exc_info=True)
        return PaginatedCityResponse(
            items=[],
            total=0,
            skip=0,
            limit=limit
        )
    finally:
        session.close()


@router.get("/cities/{city_slug}", response_model=CityWithAttractions)
async def get_city(city_slug: str):
    """Get city details with summary of attractions."""
    session = SessionLocal()
    try:
        # Find city by slug (convert slug to name)
        city_name = city_slug.replace('-', ' ').title()
        
        city = session.query(models.City).filter(
            func.lower(models.City.name) == city_name.lower()
        ).first()
        
        if not city:
            raise HTTPException(status_code=404, detail=f"City '{city_slug}' not found")
        
        # Get attraction count
        attraction_count = session.query(func.count(models.Attraction.id)).filter(
            models.Attraction.city_id == city.id
        ).scalar()
        
        # Get top attractions for preview
        attractions = (
            session.query(
                models.Attraction,
                case(
                    (models.HeroImage.gcs_url_card.isnot(None), models.HeroImage.gcs_url_card),
                    else_=models.HeroImage.url
                ).label("hero_image")
            )
            .filter(models.Attraction.city_id == city.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) &
                (models.HeroImage.position == 1)
            )
            .order_by(
                desc(models.HeroImage.last_refreshed_at.isnot(None)),
                desc(models.HeroImage.last_refreshed_at),
                models.Attraction.name
            )
            .limit(settings.FEATURED_ATTRACTIONS_LIMIT)
            .all()
        )

        attraction_list = []
        for attr, hero_img in attractions:
            attraction_list.append(AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city.name,
                city_slug=city.slug,
                country=city.country,
                hero_image=hero_img,
                average_rating=float(attr.rating) if attr.rating else None,
                total_reviews=attr.review_count if attr.review_count else 0,
                summary_text=attr.summary_gemini,
                latitude=float(attr.latitude) if attr.latitude else None,
                longitude=float(attr.longitude) if attr.longitude else None
            ))
        
        # Convert coordinates to float or None
        lat = float(city.latitude) if city.latitude else None
        lng = float(city.longitude) if city.longitude else None
        
        return CityWithAttractions(
            id=city.id,
            name=city.name,
            slug=city.slug,
            country=city.country,
            attraction_count=attraction_count or 0,
            latitude=lat,
            longitude=lng,
            lat=lat,
            lng=lng,
            attractions=attraction_list
        )
        
    finally:
        session.close()


class PaginatedAttractions(BaseModel):
    """Paginated attractions response."""
    items: List[AttractionSummary]
    total: int
    skip: int
    limit: Optional[int] = None


@router.get("/cities/{city_slug}/attractions", response_model=PaginatedAttractions)
async def get_city_attractions(
    city_slug: str,
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(12, ge=1, le=1000, description="Number of items to return")
):
    """Get paginated attractions for a city."""
    session = SessionLocal()
    try:
        # Find city by slug
        city_name = city_slug.replace('-', ' ').title()
        
        city = session.query(models.City).filter(
            func.lower(models.City.name) == city_name.lower()
        ).first()
        
        if not city:
            raise HTTPException(status_code=404, detail=f"City '{city_slug}' not found")
        
        # Get total count
        total = session.query(func.count(models.Attraction.id)).filter(
            models.Attraction.city_id == city.id
        ).scalar()
        
        # Get paginated attractions
        attractions = (
            session.query(
                models.Attraction,
                case(
                    (models.HeroImage.gcs_url_card.isnot(None), models.HeroImage.gcs_url_card),
                    else_=models.HeroImage.url
                ).label("hero_image")
            )
            .filter(models.Attraction.city_id == city.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) & (models.HeroImage.position == 1)
            )
            .order_by(
                desc(models.HeroImage.last_refreshed_at.isnot(None)),
                desc(models.HeroImage.last_refreshed_at),
                models.Attraction.name
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

        items = [
            AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city.name,
                city_slug=city.slug,
                country=city.country,
                hero_image=hero_img,
                average_rating=float(attr.rating) if attr.rating else None,
                total_reviews=attr.review_count if attr.review_count else 0,
                summary_text=attr.summary_gemini,
                latitude=float(attr.latitude) if attr.latitude else None,
                longitude=float(attr.longitude) if attr.longitude else None
            )
            for attr, hero_img in attractions
        ]
                
        return PaginatedAttractions(
            items=items,
            total=total or 0,
            skip=skip,
            limit=limit
        )
        
    finally:
        session.close()


class PaginatedAttractionResponse(BaseModel):
    """Paginated response for attractions."""
    items: List[AttractionSummary]
    total: int
    skip: int = 0
    limit: Optional[int] = None


@router.get("/attractions", response_model=PaginatedAttractionResponse)
async def get_attractions(
    city: Optional[str] = Query(None, description="Filter by city name"),
    country: Optional[str] = Query(None, description="Filter by country"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of results (max 1000)")
):
    """Get all attractions with optional filters.

    Default returns all attractions, but limited to max 1000 to prevent memory issues.
    """
    session = SessionLocal()
    try:
        # Apply max limit for safety (prevent OOM if attractions table grows large)
        MAX_ATTRACTIONS_LIMIT = 1000
        effective_limit = min(limit, MAX_ATTRACTIONS_LIMIT) if limit else MAX_ATTRACTIONS_LIMIT

        query = (
            session.query(
                models.Attraction,
                models.City,
                case(
                    (models.HeroImage.gcs_url_card.isnot(None), models.HeroImage.gcs_url_card),
                    else_=models.HeroImage.url
                ).label("hero_image")
            )
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) &
                (models.HeroImage.position == 1)
            )
        )

        # Apply filters
        if city:
            query = query.filter(func.lower(models.City.name) == city.lower())
        if country:
            query = query.filter(func.lower(models.City.country) == country.lower())

        # Get total count
        total = query.count()

        # Get attractions with limit
        query = query.order_by(
            desc(models.HeroImage.last_refreshed_at.isnot(None)),
            desc(models.HeroImage.last_refreshed_at),
            models.Attraction.name
        ).limit(effective_limit)
        attractions = query.all()

        items = []
        for attr, city_obj, hero_image in attractions:
            items.append(AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city_obj.name,
                city_slug=city_obj.slug,
                country=city_obj.country,
                hero_image=hero_image,
                average_rating=float(attr.rating) if attr.rating else None,
                total_reviews=attr.review_count if attr.review_count else 0,
                summary_text=attr.summary_gemini,
                latitude=float(attr.latitude) if attr.latitude else 0.0,
                longitude=float(attr.longitude) if attr.longitude else 0.0
            ))
        
        return PaginatedAttractionResponse(
            items=items,
            total=total,
            skip=0,
            limit=limit or total
        )
    finally:
        session.close()


@router.get("/attractions/{slug}", response_model=dict)
async def get_attraction(
    slug: str,
    reviews_limit: int = Query(default=50, ge=1, le=200, description="Max reviews to return"),
    videos_limit: int = Query(default=20, ge=1, le=100, description="Max videos to return"),
    tips_limit: int = Query(default=50, ge=1, le=100, description="Max tips to return"),
    nearby_limit: int = Query(default=20, ge=1, le=50, description="Max nearby attractions to return")
):
    """Get complete attraction data with all sections.

    Query parameters allow controlling the size of large collections to prevent memory issues.
    """
    session = SessionLocal()
    try:
        # Get attraction
        attraction = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .filter(models.Attraction.slug == slug)
            .first()
        )

        if not attraction:
            raise HTTPException(status_code=404, detail=f"Attraction '{slug}' not found")

        attr, city = attraction

        # Get all data sections with LIMITS to prevent memory exhaustion
        # Hero images: limit to 20 (should be small anyway)
        hero_images = session.query(models.HeroImage).filter_by(
            attraction_id=attr.id
        ).order_by(models.HeroImage.position).limit(20).all()

        # Get best time data - separate regular and special days
        # Regular days: Get one row per day_int (0-6), using the most recent if duplicates exist
        from sqlalchemy import func

        # Subquery to get the max ID for each day_int (most recent entry)
        subquery = session.query(
            func.max(models.BestTimeData.id).label('max_id')
        ).filter_by(
            attraction_id=attr.id,
            day_type='regular'
        ).group_by(models.BestTimeData.day_int).subquery()

        best_time_regular = session.query(models.BestTimeData).filter(
            models.BestTimeData.id.in_(subquery)
        ).order_by(models.BestTimeData.day_int).all()

        # Special days: limit to next 30 days
        best_time_special = session.query(models.BestTimeData).filter_by(
            attraction_id=attr.id, day_type='special'
        ).order_by(models.BestTimeData.date_local).limit(30).all()

        # Weather: get all available data from today onwards based on timezone
        import pytz
        from datetime import datetime

        today_date = None
        if city and city.timezone:
            try:
                city_tz = pytz.timezone(city.timezone)
                today_date = datetime.now(city_tz).date()
            except Exception:
                today_date = datetime.now().date()
        else:
            today_date = datetime.now().date()

        weather = session.query(models.WeatherForecast).filter(
            models.WeatherForecast.attraction_id == attr.id,
            models.WeatherForecast.date_local >= today_date
        ).order_by(models.WeatherForecast.date_local).all()

        map_data = session.query(models.MapSnapshot).filter_by(attraction_id=attr.id).first()
        metadata = session.query(models.AttractionMetadata).filter_by(attraction_id=attr.id).first()

        # CRITICAL FIX: Limit reviews to prevent OOM with popular attractions
        reviews = session.query(models.Review).filter_by(
            attraction_id=attr.id
        ).order_by(models.Review.time.desc()).limit(reviews_limit).all()

        # Limit tips
        tips = session.query(models.Tip).filter_by(
            attraction_id=attr.id
        ).limit(tips_limit).all()

        # Audience profiles
        audience = session.query(models.AudienceProfile).filter_by(
            attraction_id=attr.id
        ).limit(5).all()

        # Social videos
        videos = session.query(models.SocialVideo).filter_by(
            attraction_id=attr.id
        ).limit(videos_limit).all()

        # Query nearby attractions with left join to attractions table for rating/review_count
        nearby = session.query(
            models.NearbyAttraction,
            models.Attraction.rating,
            models.Attraction.review_count,
            models.Attraction.slug
        ).filter(
            models.NearbyAttraction.attraction_id == attr.id
        ).outerjoin(
            models.Attraction,
            models.NearbyAttraction.nearby_attraction_id == models.Attraction.id
        ).limit(nearby_limit).all()
        
        # Build nearby attractions list
        enriched_nearby = []
        for n, attr_rating, attr_review_count, attr_slug in nearby:
            nearby_item = {
                'nearby_attraction': n,
                'slug': n.slug or attr_slug,
                'rating': float(n.rating) if n.rating else (float(attr_rating) if attr_rating else None),
                'review_count': n.review_count if n.review_count else (attr_review_count if attr_review_count else 0),
                'image_url': n.image_url
            }
            enriched_nearby.append(nearby_item)
        
        widgets = session.query(models.WidgetConfig).filter_by(attraction_id=attr.id).first()
        
        # Trigger background refresh for nearby attractions
        try:
            from app.tasks.nearby_attractions_tasks import update_nearby_attractions_for_attraction
            update_nearby_attractions_for_attraction.delay(attr.id, force_refresh=True)
            logger.debug(f"Queued background refresh for nearby attractions of {attr.name}")
        except Exception as e:
            logger.warning(f"Failed to queue background refresh for {attr.name}: {e}")

        # Build response
        response_data = {
            "id": attr.id,
            "slug": attr.slug,
            "name": attr.name,
            "city": city.name,
            "country": city.country,
            "timezone": city.timezone,
            "latitude": float(attr.latitude) if attr.latitude else None,
            "longitude": float(attr.longitude) if attr.longitude else None,
            "place_id": attr.place_id,
            "rating": float(attr.rating) if attr.rating else None,
            "review_count": attr.review_count if attr.review_count else 0,
            
            "visitor_info": {
                "contact_info": metadata.contact_info if metadata and metadata.contact_info else {},
                "accessibility_info": metadata.accessibility_info if metadata else None,
                "best_season": metadata.best_season if metadata else None,
                "opening_hours": metadata.opening_hours if metadata and metadata.opening_hours else [],
                "short_description": metadata.short_description if metadata else None,
                "recommended_duration_minutes": metadata.recommended_duration_minutes if metadata else None,
                "highlights": metadata.highlights if metadata and metadata.highlights else []
            } if metadata else None,
            
            "hero_images": [
                {
                    "url": img.url,
                    "alt_text": img.alt_text
                }
                for img in hero_images
            ],
            
            "best_time": {
                "regular_days": [
                    {
                        "day_int": bt.day_int,
                        "day_name": bt.day_name,
                        "is_open_today": bt.is_open_today,
                        "today_opening_time": str(bt.today_opening_time) if bt.today_opening_time else None,
                        "today_closing_time": str(bt.today_closing_time) if bt.today_closing_time else None,
                        "crowd_level_today": bt.crowd_level_today,
                        "best_time_today": bt.best_time_today,
                        "reason_text": bt.reason_text,
                        "hourly_crowd_levels": bt.hourly_crowd_levels if bt.hourly_crowd_levels else []
                    }
                    for bt in best_time_regular
                ],
                "special_days": [
                    {
                        "date": bt.date_local.isoformat() if bt.date_local else None,
                        "day": bt.day_name,
                        "is_open_today": bt.is_open_today,
                        "today_opening_time": str(bt.today_opening_time) if bt.today_opening_time else None,
                        "today_closing_time": str(bt.today_closing_time) if bt.today_closing_time else None,
                        "crowd_level_today": bt.crowd_level_today,
                        "best_time_today": bt.best_time_today,
                        "reason_text": bt.reason_text,
                        "hourly_crowd_levels": bt.hourly_crowd_levels if bt.hourly_crowd_levels else []
                    }
                    for bt in best_time_special
                ]
            },
            
            "weather": [
                {
                    "date": w.date_local.isoformat() if w.date_local else None,
                    "temperature_c": float(w.temperature_c) if w.temperature_c else None,
                    "feels_like_c": float(w.feels_like_c) if w.feels_like_c else None,
                    "min_temperature_c": float(w.min_temperature_c) if w.min_temperature_c else None,
                    "max_temperature_c": float(w.max_temperature_c) if w.max_temperature_c else None,
                    "summary": w.summary,
                    "precipitation_mm": float(w.precipitation_mm) if w.precipitation_mm else None,
                    "wind_speed_kph": float(w.wind_speed_kph) if w.wind_speed_kph else None,
                    "humidity_percent": w.humidity_percent,
                    "icon_url": w.icon_url
                }
                for w in weather
            ],
            
            "map": {
                "static_map_url": map_data.static_map_url,
                "directions_url": map_data.directions_url,
                "latitude": float(map_data.latitude) if map_data.latitude else None,
                "longitude": float(map_data.longitude) if map_data.longitude else None,
                "address": map_data.address
            } if map_data and map_data.static_map_url else None,
            
            "reviews": {
                "summary": {
                    "average_rating": float(attr.rating) if attr.rating else None,
                    "total_reviews": attr.review_count if attr.review_count else 0,
                    "summary_text": attr.summary_gemini
                },
                "pagination": {
                    "returned": len(reviews),
                    "limit": reviews_limit,
                    "has_more": (attr.review_count or 0) > reviews_limit
                },
                "reviews": [
                    {
                        "author_name": r.author_name,
                        "author_url": r.author_url,
                        "author_photo_url": r.author_photo_url,
                        "rating": r.rating,
                        "text": r.text,
                        "time": r.time.isoformat() if r.time else None,
                        "source": r.source
                    }
                    for r in reviews
                ]
            },
            
            "tips": {
                "safety": [
                    {
                        "id": t.id,
                        "text": t.text,
                        "source": t.source,
                        "scope": t.scope,
                        "position": t.position if hasattr(t, 'position') else 1
                    }
                    for t in tips if t.tip_type == "SAFETY"
                ],
                "insider": [
                    {
                        "id": t.id,
                        "text": t.text,
                        "source": t.source,
                        "scope": t.scope,
                        "position": t.position if hasattr(t, 'position') else 1
                    }
                    for t in tips if t.tip_type == "INSIDER"
                ]
            } if tips else None,
            
            "audience_profiles": [
                {
                    "audience_type": a.audience_type,
                    "description": a.description,
                    "emoji": a.emoji
                }
                for a in audience
            ],
            
            "social_videos": [
                {
                    "video_id": v.video_id,
                    "title": v.title,
                    "thumbnail_url": v.thumbnail_url,
                    "channel_title": v.channel_title,
                    "watch_url": v.watch_url,
                    "view_count": v.view_count,
                    "duration_seconds": v.duration_seconds
                }
                for v in videos
            ],
            
            "nearby_attractions": [
                {
                    "name": item['nearby_attraction'].name,
                    "slug": item['nearby_attraction'].slug,
                    "link": item['nearby_attraction'].link,
                    "distance_km": float(item['nearby_attraction'].distance_km) if item['nearby_attraction'].distance_km else None,
                    "distance_text": item['nearby_attraction'].distance_text,
                    "walking_time_minutes": item['nearby_attraction'].walking_time_minutes,
                    "image_url": item['image_url'],
                    "rating": item['rating'],
                    "review_count": item['review_count'],
                    "vicinity": item['nearby_attraction'].vicinity
                }
                for item in enriched_nearby
            ],
            
            "widgets": {
                "widget_primary": widgets.widget_primary,
                "widget_secondary": widgets.widget_secondary
            } if widgets else None
        }

        # Validate response size to prevent OOM during JSON serialization
        import sys
        response_size_bytes = sys.getsizeof(str(response_data))
        MAX_RESPONSE_SIZE_MB = 10  # 10MB limit
        if response_size_bytes > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            logger.warning(
                f"Response size {response_size_bytes / 1024 / 1024:.2f}MB exceeds limit "
                f"for attraction {slug}. Consider reducing limits."
            )

        return response_data

    except Exception as e:
        import traceback
        logger.error(f"❌ Error fetching attraction '{slug}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error loading attraction: {str(e)}")
    finally:
        session.close()


@router.get("/attractions/{slug}/nearby")
async def get_nearby_attractions(
    slug: str,
    limit: int = Query(default=20, ge=1, le=50, description="Max nearby attractions to return"),
    session: Session = Depends(get_db)
):
    """
    Get nearby attractions for a specific attraction and trigger a background refresh.
    """
    # Get attraction
    attr = (
        session.query(models.Attraction)
        .filter(models.Attraction.slug == slug)
        .first()
    )

    if not attr:
        raise HTTPException(status_code=404, detail=f"Attraction '{slug}' not found")

    # Query current nearby attractions from DB
    nearby = session.query(
        models.NearbyAttraction,
        models.Attraction.rating,
        models.Attraction.review_count,
        models.Attraction.slug
    ).filter(
        models.NearbyAttraction.attraction_id == attr.id
    ).outerjoin(
        models.Attraction,
        models.NearbyAttraction.nearby_attraction_id == models.Attraction.id
    ).limit(limit).all()

    # Build results
    results = []
    for n, attr_rating, attr_review_count, attr_slug in nearby:
        results.append({
            "id": n.id,
            "name": n.name,
            "slug": n.slug or attr_slug,
            "rating": float(n.rating) if n.rating else (float(attr_rating) if attr_rating else None),
            "review_count": n.review_count if n.review_count else (attr_review_count if attr_review_count else 0),
            "image_url": n.image_url,
            "distance_text": n.distance_text,
            "distance_km": float(n.distance_km) if n.distance_km else 0.0,
            "walking_time_minutes": n.walking_time_minutes,
            "vicinity": n.vicinity,
            "link": n.link
        })

    # Trigger background refresh
    try:
        from app.tasks.nearby_attractions_tasks import update_nearby_attractions_for_attraction
        update_nearby_attractions_for_attraction.delay(attr.id, force_refresh=True)
        logger.debug(f"Queued background refresh for nearby attractions of {attr.name}")
    except Exception as e:
        logger.warning(f"Failed to queue background refresh for {attr.name}: {e}")

    return results


# Search Models
class SearchResults(BaseModel):
    """Search results containing cities and attractions."""
    cities: List[CitySummary]
    attractions: List[AttractionSummary]
    stories: List = []  # Placeholder for future feature


# Contact Form Models
class ContactFormRequest(BaseModel):
    """Contact form submission."""
    name: str
    email: str
    subject: str
    message: str


class ContactFormResponse(BaseModel):
    """Contact form response."""
    success: bool
    message: str


@router.post("/contact", response_model=ContactFormResponse)
async def submit_contact_form(form: ContactFormRequest):
    """Submit contact form and save to database."""
    session = SessionLocal()
    try:
        # Create contact submission record
        submission = models.ContactSubmission(
            name=form.name,
            email=form.email,
            subject=form.subject,
            message=form.message,
            status="new",
            created_at=datetime.utcnow()
        )

        session.add(submission)
        session.commit()

        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Contact form saved (ID: {submission.id}) from {form.name} ({form.email})")

        # TODO: Send email notification
        # TODO: Add to CRM system

        return {
            "success": True,
            "message": "Thank you for your message. We'll get back to you soon!"
        }
    except Exception as e:
        session.rollback()
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to save contact form submission: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit form. Please try again later.")
    finally:
        session.close()


@router.get("/search", response_model=SearchResults)
async def search(q: str = Query(..., description="Search query")):
    """Search for cities and attractions by name."""
    session = SessionLocal()
    try:
        search_term = f"%{q}%"
        
        # Search cities
        cities_query = session.query(
            models.City.id,
            models.City.name,
            models.City.slug,
            models.City.country,
            models.City.latitude,
            models.City.longitude,
            func.count(models.Attraction.id).label('attraction_count')
        ).outerjoin(
            models.Attraction,
            models.City.id == models.Attraction.city_id
        ).filter(
            models.City.name.ilike(search_term)
        ).group_by(
            models.City.id,
            models.City.name,
            models.City.slug,
            models.City.country,
            models.City.latitude,
            models.City.longitude
        ).limit(settings.SEARCH_RESULTS_LIMIT)

        cities = []
        for row in cities_query.all():
            cities.append(CitySummary(
                id=row.id,
                name=row.name,
                slug=row.slug,
                country=row.country,
                attraction_count=row.attraction_count,
                lat=row.latitude,
                lng=row.longitude
            ))
        
        # Search attractions
        attractions_query = (
            session.query(
                models.Attraction,
                models.City.name.label('city_name'),
                models.City.country.label('country_name'),
                models.City.slug.label('city_slug'),
                models.HeroImage.url.label("hero_image")
            )
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) &
                (models.HeroImage.position == 1)
            )
            .filter(
                or_(
                    models.Attraction.name.ilike(search_term),
                    models.City.name.ilike(search_term)
                )
            )
            .limit(20)
        )

        attractions = []
        for attr, city_name, country_name, city_slug, hero_image in attractions_query.all():
            attractions.append(AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city_name,
                city_slug=city_slug,
                country=country_name,
                hero_image=hero_image,
                average_rating=float(attr.rating) if attr.rating else None,
                total_reviews=attr.review_count if attr.review_count else 0,
                summary_text=attr.summary_gemini,
                latitude=float(attr.latitude) if attr.latitude else 0.0,
                longitude=float(attr.longitude) if attr.longitude else 0.0
            ))
        
        return SearchResults(
            cities=cities,
            attractions=attractions,
            stories=[]
        )
        
    finally:
        session.close()
