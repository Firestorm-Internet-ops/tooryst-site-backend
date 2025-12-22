"""Frontend API routes for homepage, cities, and attractions."""
import logging
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, desc, or_

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
                models.HeroImage.url.label("hero_image")
            )
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) &
                (models.HeroImage.position == 1)
            )
            .order_by(models.Attraction.name)
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
        print(f"ERROR in homepage endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


class PaginatedCityResponse(BaseModel):
    """Paginated response for cities."""
    items: List[CitySummary]
    total: int
    skip: int = 0
    limit: int


@router.get("/cities", response_model=PaginatedCityResponse)
async def get_cities(
    limit: Optional[int] = Query(None, ge=1, description="Maximum number of results (None for all)")
):
    """Get all cities with attraction counts."""
    session = SessionLocal()
    try:
        query = (
            session.query(
                models.City,
                func.count(models.Attraction.id).label('count')
            )
            .outerjoin(models.Attraction, models.City.id == models.Attraction.city_id)
            .group_by(models.City.id)
            .order_by(models.City.name)
        )
        
        if limit:
            query = query.limit(limit)
        
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
        print(f"❌ Error fetching cities: {e}")
        traceback.print_exc()
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
                models.HeroImage.url.label("hero_image")
            )
            .filter(models.Attraction.city_id == city.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) &
                (models.HeroImage.position == 1)
            )
            .order_by(models.Attraction.name)
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
    limit: int


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
                models.HeroImage.url.label("hero_image")
            )
            .filter(models.Attraction.city_id == city.id)
            .outerjoin(
                models.HeroImage,
                (models.Attraction.id == models.HeroImage.attraction_id) & (models.HeroImage.position == 1)
            )
            .order_by(models.Attraction.name)
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
    limit: int


@router.get("/attractions", response_model=PaginatedAttractionResponse)
async def get_attractions(
    city: Optional[str] = Query(None, description="Filter by city name"),
    country: Optional[str] = Query(None, description="Filter by country"),
    limit: Optional[int] = Query(None, ge=1, description="Maximum number of results (None for all)")
):
    """Get all attractions with optional filters."""
    session = SessionLocal()
    try:
        query = (
            session.query(
                models.Attraction,
                models.City,
                models.HeroImage.url.label("hero_image")
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

        # Get attractions
        query = query.order_by(models.Attraction.name)
        if limit:
            query = query.limit(limit)
        attractions = query.all()

        items = []
        for attr, city_obj, hero_image in attractions:
            items.append(AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city_obj.name,
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
async def get_attraction(slug: str):
    """Get complete attraction data with all sections."""
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
        
        # Get all data sections (using correct field names)
        hero_images = session.query(models.HeroImage).filter_by(attraction_id=attr.id).order_by(models.HeroImage.position).all()

        # Get best time data - separate regular and special days
        best_time_regular = session.query(models.BestTimeData).filter_by(
            attraction_id=attr.id, day_type='regular'
        ).order_by(models.BestTimeData.day_int).all()

        best_time_special = session.query(models.BestTimeData).filter_by(
            attraction_id=attr.id, day_type='special'
        ).order_by(models.BestTimeData.date_local).all()

        weather = session.query(models.WeatherForecast).filter_by(attraction_id=attr.id).order_by(models.WeatherForecast.date_local).all()
        map_data = session.query(models.MapSnapshot).filter_by(attraction_id=attr.id).first()
        metadata = session.query(models.AttractionMetadata).filter_by(attraction_id=attr.id).first()
        reviews = session.query(models.Review).filter_by(attraction_id=attr.id).order_by(models.Review.time.desc()).all()
        tips = session.query(models.Tip).filter_by(attraction_id=attr.id).all()
        audience = session.query(models.AudienceProfile).filter_by(attraction_id=attr.id).all()
        videos = session.query(models.SocialVideo).filter_by(attraction_id=attr.id).all()
        # Query nearby attractions with left join to attractions table for rating/review_count
        nearby = session.query(
            models.NearbyAttraction,
            models.Attraction.rating,
            models.Attraction.review_count
        ).filter(
            models.NearbyAttraction.attraction_id == attr.id
        ).outerjoin(
            models.Attraction,
            models.NearbyAttraction.nearby_attraction_id == models.Attraction.id
        ).all()
        
        # Build nearby attractions list (enrichment happens asynchronously in background)
        enriched_nearby = []
        for n, attr_rating, attr_review_count in nearby:
            nearby_item = {
                'nearby_attraction': n,
                'rating': float(n.rating) if n.rating else (float(attr_rating) if attr_rating else None),
                'review_count': n.review_count if n.review_count else (attr_review_count if attr_review_count else 0),
                'image_url': n.image_url
            }
            
            # If nearby attraction is from Google Places (nearby_attraction_id is NULL) and missing data,
            # queue enrichment task to fetch from Google Places API
            if n.nearby_attraction_id is None and n.place_id and (not n.rating or not n.review_count or not n.image_url):
                try:
                    from app.tasks.nearby_attractions_tasks import enrich_nearby_attraction_from_google
                    enrich_nearby_attraction_from_google.delay(n.id)
                    logger.debug(f"Queued enrichment task for nearby attraction {n.name}")
                except Exception as e:
                    logger.warning(f"Failed to queue enrichment for {n.name}: {e}")
            
            enriched_nearby.append(nearby_item)
        
        widgets = session.query(models.WidgetConfig).filter_by(attraction_id=attr.id).first()
        
        # Build response
        return {
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
            
            "visitor_info": {
                "contact_info": metadata.contact_info if metadata.contact_info else {},
                "accessibility_info": metadata.accessibility_info,
                "best_season": metadata.best_season,
                "opening_hours": metadata.opening_hours if metadata.opening_hours else [],
                "short_description": metadata.short_description,
                "recommended_duration_minutes": metadata.recommended_duration_minutes,
                "highlights": metadata.highlights if metadata.highlights else []
            } if metadata else None,
            
            "reviews": {
                "summary": {
                    "average_rating": float(attr.rating) if attr.rating else None,
                    "total_reviews": attr.review_count if attr.review_count else 0,
                    "summary_text": attr.summary_gemini
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
        
    except Exception as e:
        import traceback
        print(f"❌ Error fetching attraction '{slug}': {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error loading attraction: {str(e)}")
    finally:
        session.close()


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
        for attr, city_name, country_name, hero_image in attractions_query.all():
            attractions.append(AttractionSummary(
                id=attr.id,
                slug=attr.slug,
                name=attr.name,
                city=city_name,
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
