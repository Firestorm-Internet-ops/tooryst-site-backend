"""Celery tasks for refreshing attraction data."""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sqlalchemy import and_, or_

from app.celery_app import celery_app
from app.config import settings
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.external_apis.besttime_fetcher import BestTimeFetcherImpl
from app.infrastructure.external_apis.weather_fetcher import WeatherFetcherImpl
from app.infrastructure.external_apis.metadata_fetcher import MetadataFetcherImpl

logger = logging.getLogger(__name__)


def get_attractions_needing_best_time_refresh() -> List[Dict[str, Any]]:
    """Get attractions that need best time data refresh.
    
    Returns attractions where:
    - No data exists, OR
    - Latest date is within 2 days (need to fetch ahead)
    """
    session = SessionLocal()
    try:
        # Get all attractions with their latest best_time date
        from sqlalchemy import func
        
        subquery = (
            session.query(
                models.BestTimeData.attraction_id,
                func.max(models.BestTimeData.date_local).label('latest_date')
            )
            .group_by(models.BestTimeData.attraction_id)
            .subquery()
        )
        
        # Get attractions that need refresh
        today = datetime.now().date()
        threshold_days = settings.BEST_TIME_REFRESH_THRESHOLD_DAYS
        threshold_date = today + timedelta(days=threshold_days)  # Refresh if latest date is within threshold days
        
        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(subquery, models.Attraction.id == subquery.c.attraction_id)
            .filter(
                or_(
                    subquery.c.latest_date.is_(None),  # No data
                    subquery.c.latest_date <= threshold_date  # Data expiring soon
                )
            )
            .all()
        )
        
        result = []
        for attraction, city in attractions:
            result.append({
                'id': attraction.id,
                'name': attraction.name,
                'place_id': attraction.place_id,
                'city_name': city.name
            })
        
        logger.info(f"Found {len(result)} attractions needing best time refresh")
        return result
        
    finally:
        session.close()


def get_attractions_needing_weather_refresh() -> List[Dict[str, Any]]:
    """Get attractions that need weather data refresh.
    
    Returns attractions where:
    - No data exists, OR
    - Latest date is today or in the past
    """
    session = SessionLocal()
    try:
        from sqlalchemy import func
        
        subquery = (
            session.query(
                models.WeatherForecast.attraction_id,
                func.max(models.WeatherForecast.date_local).label('latest_date')
            )
            .group_by(models.WeatherForecast.attraction_id)
            .subquery()
        )
        
        today = datetime.now().date()
        
        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(subquery, models.Attraction.id == subquery.c.attraction_id)
            .filter(
                or_(
                    subquery.c.latest_date.is_(None),  # No data
                    subquery.c.latest_date <= today  # Data expired
                )
            )
            .filter(models.Attraction.latitude.isnot(None))
            .filter(models.Attraction.longitude.isnot(None))
            .all()
        )
        
        result = []
        for attraction, city in attractions:
            result.append({
                'id': attraction.id,
                'name': attraction.name,
                'place_id': attraction.place_id,
                'city_name': city.name,
                'country': city.country,
                'timezone': city.timezone,
                'latitude': float(attraction.latitude),
                'longitude': float(attraction.longitude)
            })
        
        logger.info(f"Found {len(result)} attractions needing weather refresh")
        return result
        
    finally:
        session.close()


def get_attractions_needing_visitor_info_refresh() -> List[Dict[str, Any]]:
    """Get attractions that need visitor info refresh.
    
    Returns attractions where:
    - No metadata exists, OR
    - Metadata is older than 7 days (opening hours may have changed)
    """
    session = SessionLocal()
    try:
        threshold_date = datetime.now() - timedelta(days=7)
        
        attractions = (
            session.query(models.Attraction, models.City, models.AttractionMetadata)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(
                models.AttractionMetadata,
                models.Attraction.id == models.AttractionMetadata.attraction_id
            )
            .filter(
                or_(
                    models.AttractionMetadata.id.is_(None),  # No metadata
                    models.AttractionMetadata.updated_at <= threshold_date  # Old data
                )
            )
            .all()
        )
        
        result = []
        for attraction, city, metadata in attractions:
            result.append({
                'id': attraction.id,
                'name': attraction.name,
                'place_id': attraction.place_id,
                'city_name': city.name
            })
        
        logger.info(f"Found {len(result)} attractions needing visitor info refresh")
        return result
        
    finally:
        session.close()


def store_best_time_data(attraction_id: int, all_days: List[Dict[str, Any]]) -> bool:
    """Store best time data in database."""
    session = SessionLocal()
    try:
        import json
        
        for day in all_days:
            card = day.get('card', {})
            section = day.get('section', {})
            
            # Convert hourly_crowd_levels to JSON string
            hourly_json = json.dumps(section.get('hourly_crowd_levels', []))
            
            # Use upsert pattern
            existing = (
                session.query(models.BestTimeData)
                .filter(
                    and_(
                        models.BestTimeData.attraction_id == attraction_id,
                        models.BestTimeData.date_local == day.get('date')
                    )
                )
                .first()
            )
            
            if existing:
                # Update
                existing.day_name = day.get('day_name')
                existing.is_open_today = card.get('is_open_today')
                existing.is_open_now = card.get('is_open_now')
                existing.today_opening_time = card.get('today_opening_time')
                existing.today_closing_time = card.get('today_closing_time')
                existing.crowd_level_today = card.get('crowd_level_today')
                existing.best_time_today = card.get('best_time_today')
                existing.reason_text = section.get('reason_text')
                existing.hourly_crowd_levels = hourly_json
                existing.data_source = 'besttime_api'
            else:
                # Insert
                now = datetime.utcnow()
                new_entry = models.BestTimeData(
                    attraction_id=attraction_id,
                    date_local=day.get('date'),
                    day_name=day.get('day_name'),
                    is_open_today=card.get('is_open_today'),
                    is_open_now=card.get('is_open_now'),
                    today_opening_time=card.get('today_opening_time'),
                    today_closing_time=card.get('today_closing_time'),
                    crowd_level_today=card.get('crowd_level_today'),
                    best_time_today=card.get('best_time_today'),
                    reason_text=section.get('reason_text'),
                    hourly_crowd_levels=hourly_json,
                    data_source='besttime_api',
                    created_at=now,
                    updated_at=now
                )
                session.add(new_entry)
        
        session.commit()
        return True
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error storing best time data: {e}")
        return False
    finally:
        session.close()


def store_weather_data(attraction_id: int, forecast_days: List[Dict[str, Any]]) -> bool:
    """Store weather forecast data in database."""
    session = SessionLocal()
    try:
        for day in forecast_days:
            card = day.get('card', {})
            
            # Use upsert pattern
            existing = (
                session.query(models.WeatherForecast)
                .filter(
                    and_(
                        models.WeatherForecast.attraction_id == attraction_id,
                        models.WeatherForecast.date_local == day.get('date')
                    )
                )
                .first()
            )
            
            if existing:
                # Update
                existing.temperature_c = card.get('temperature_c')
                existing.feels_like_c = card.get('feels_like_c')
                existing.min_temperature_c = card.get('min_temperature_c')
                existing.max_temperature_c = card.get('max_temperature_c')
                existing.summary = card.get('summary')
                existing.precipitation_mm = card.get('precipitation_mm')
                existing.wind_speed_kph = card.get('wind_speed_kph')
                existing.humidity_percent = card.get('humidity_percent')
                existing.icon_url = card.get('icon_url')
            else:
                # Insert
                now = datetime.utcnow()
                new_entry = models.WeatherForecast(
                    attraction_id=attraction_id,
                    date_local=day.get('date'),
                    temperature_c=card.get('temperature_c'),
                    feels_like_c=card.get('feels_like_c'),
                    min_temperature_c=card.get('min_temperature_c'),
                    max_temperature_c=card.get('max_temperature_c'),
                    summary=card.get('summary'),
                    precipitation_mm=card.get('precipitation_mm'),
                    wind_speed_kph=card.get('wind_speed_kph'),
                    humidity_percent=card.get('humidity_percent'),
                    icon_url=card.get('icon_url'),
                    created_at=now,
                    updated_at=now
                )
                session.add(new_entry)
        
        session.commit()
        return True
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error storing weather data: {e}")
        return False
    finally:
        session.close()


def store_metadata(attraction_id: int, metadata: Dict[str, Any]) -> bool:
    """Store attraction metadata in database."""
    session = SessionLocal()
    try:
        import json
        
        # Convert JSON fields
        contact_info_json = json.dumps(metadata.get('contact_info', {}))
        opening_hours_json = json.dumps(metadata.get('opening_hours', []))
        highlights_json = json.dumps(metadata.get('highlights', []))
        
        # Use upsert pattern
        existing = (
            session.query(models.AttractionMetadata)
            .filter(models.AttractionMetadata.attraction_id == attraction_id)
            .first()
        )
        
        if existing:
            # Update
            existing.contact_info = contact_info_json
            existing.accessibility_info = metadata.get('accessibility_info')
            existing.best_season = metadata.get('best_season')
            existing.opening_hours = opening_hours_json
            existing.short_description = metadata.get('short_description')
            existing.recommended_duration_minutes = metadata.get('recommended_duration_minutes')
            existing.highlights = highlights_json
        else:
            # Insert
            now = datetime.utcnow()
            new_entry = models.AttractionMetadata(
                attraction_id=attraction_id,
                contact_info=contact_info_json,
                accessibility_info=metadata.get('accessibility_info'),
                best_season=metadata.get('best_season'),
                opening_hours=opening_hours_json,
                short_description=metadata.get('short_description'),
                recommended_duration_minutes=metadata.get('recommended_duration_minutes'),
                highlights=highlights_json,
                created_at=now,
                updated_at=now
            )
            session.add(new_entry)
        
        session.commit()
        return True
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error storing metadata: {e}")
        return False
    finally:
        session.close()


@celery_app.task(name="app.tasks.refresh_tasks.refresh_best_time_data")
def refresh_best_time_data():
    """Celery task to refresh best time data for attractions that need it."""
    logger.info("Starting best time data refresh task")
    
    try:
        # Get attractions that need refresh
        attractions = get_attractions_needing_best_time_refresh()
        
        if not attractions:
            logger.info("No attractions need best time refresh")
            return {"status": "success", "processed": 0}
        
        # Process each attraction
        fetcher = BestTimeFetcherImpl()
        success_count = 0
        error_count = 0
        
        for attraction in attractions:
            try:
                logger.info(f"Refreshing best time for {attraction['name']}")
                
                # Fetch data (async)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction['id'],
                        place_id=attraction['place_id']
                    )
                )
                loop.close()
                
                if result and result.get('all_days'):
                    if store_best_time_data(attraction['id'], result['all_days']):
                        success_count += 1
                        logger.info(f"✓ Refreshed best time for {attraction['name']}")
                    else:
                        error_count += 1
                        logger.error(f"✗ Failed to store best time for {attraction['name']}")
                else:
                    error_count += 1
                    logger.warning(f"✗ No best time data for {attraction['name']}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing {attraction['name']}: {e}")
        
        logger.info(f"Best time refresh complete: {success_count} success, {error_count} errors")
        return {
            "status": "success",
            "processed": len(attractions),
            "success": success_count,
            "errors": error_count
        }
        
    except Exception as e:
        logger.error(f"Best time refresh task failed: {e}")
        return {"status": "error", "error": str(e)}


@celery_app.task(name="app.tasks.refresh_tasks.refresh_weather_data")
def refresh_weather_data():
    """Celery task to refresh weather data for attractions that need it."""
    logger.info("Starting weather data refresh task")
    
    try:
        # Get attractions that need refresh
        attractions = get_attractions_needing_weather_refresh()
        
        if not attractions:
            logger.info("No attractions need weather refresh")
            return {"status": "success", "processed": 0}
        
        # Process each attraction
        fetcher = WeatherFetcherImpl()
        success_count = 0
        error_count = 0
        
        for attraction in attractions:
            try:
                logger.info(f"Refreshing weather for {attraction['name']}")
                
                # Fetch data (async)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction['id'],
                        place_id=attraction.get('place_id'),
                        latitude=attraction['latitude'],
                        longitude=attraction['longitude'],
                        timezone_str=attraction.get('timezone', 'UTC'),
                        attraction_name=attraction['name'],
                        city_name=attraction['city_name'],
                        country=attraction.get('country')
                    )
                )
                loop.close()
                
                if result and result.get('forecast_days'):
                    if store_weather_data(attraction['id'], result['forecast_days']):
                        success_count += 1
                        logger.info(f"✓ Refreshed weather for {attraction['name']}")
                    else:
                        error_count += 1
                        logger.error(f"✗ Failed to store weather for {attraction['name']}")
                else:
                    error_count += 1
                    logger.warning(f"✗ No weather data for {attraction['name']}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing {attraction['name']}: {e}")
        
        logger.info(f"Weather refresh complete: {success_count} success, {error_count} errors")
        return {
            "status": "success",
            "processed": len(attractions),
            "success": success_count,
            "errors": error_count
        }
        
    except Exception as e:
        logger.error(f"Weather refresh task failed: {e}")
        return {"status": "error", "error": str(e)}


@celery_app.task(name="app.tasks.refresh_tasks.refresh_visitor_info")
def refresh_visitor_info():
    """Celery task to refresh visitor info (opening hours) for attractions that need it."""
    logger.info("Starting visitor info refresh task")
    
    try:
        # Get attractions that need refresh
        attractions = get_attractions_needing_visitor_info_refresh()
        
        if not attractions:
            logger.info("No attractions need visitor info refresh")
            return {"status": "success", "processed": 0}
        
        # Process each attraction
        fetcher = MetadataFetcherImpl()
        success_count = 0
        error_count = 0
        
        for attraction in attractions:
            try:
                logger.info(f"Refreshing visitor info for {attraction['name']}")
                
                # Fetch data (async)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction['id'],
                        place_id=attraction.get('place_id'),
                        attraction_name=attraction['name'],
                        city_name=attraction['city_name']
                    )
                )
                loop.close()
                
                if result and result.get('metadata'):
                    if store_metadata(attraction['id'], result['metadata']):
                        success_count += 1
                        logger.info(f"✓ Refreshed visitor info for {attraction['name']}")
                    else:
                        error_count += 1
                        logger.error(f"✗ Failed to store visitor info for {attraction['name']}")
                else:
                    error_count += 1
                    logger.warning(f"✗ No visitor info data for {attraction['name']}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing {attraction['name']}: {e}")
        
        logger.info(f"Visitor info refresh complete: {success_count} success, {error_count} errors")
        return {
            "status": "success",
            "processed": len(attractions),
            "success": success_count,
            "errors": error_count
        }
        
    except Exception as e:
        logger.error(f"Visitor info refresh task failed: {e}")
        return {"status": "error", "error": str(e)}
