"""Celery tasks for updating nearby attractions.

This module handles:
1. Updating nearby attractions when a new attraction is added to a city
2. Periodic refresh of nearby attractions data
3. Backfilling missing nearby attractions data
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import and_, or_

from app.celery_app import celery_app
from app.config import settings
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.persistence.storage_functions import store_nearby_attractions

logger = logging.getLogger(__name__)


def get_attractions_in_city(city_id: int) -> List[Dict[str, Any]]:
    """Get all attractions in a city with their coordinates."""
    session = SessionLocal()
    try:
        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .filter(models.Attraction.city_id == city_id)
            .filter(models.Attraction.latitude.isnot(None))
            .filter(models.Attraction.longitude.isnot(None))
            .all()
        )
        
        result = []
        for attraction, city in attractions:
            result.append({
                'id': attraction.id,
                'name': attraction.name,
                'slug': attraction.slug,
                'place_id': attraction.place_id,
                'city_id': city.id,
                'city_name': city.name,
                'latitude': float(attraction.latitude),
                'longitude': float(attraction.longitude),
            })
        
        return result
    finally:
        session.close()


def get_nearby_attractions_count(attraction_id: int) -> int:
    """Get count of nearby attractions for an attraction."""
    session = SessionLocal()
    try:
        count = (
            session.query(models.NearbyAttraction)
            .filter(models.NearbyAttraction.attraction_id == attraction_id)
            .count()
        )
        return count
    finally:
        session.close()


def get_attractions_needing_nearby_update() -> List[Dict[str, Any]]:
    """Get attractions that need nearby attractions data.
    
    Returns attractions where:
    - No nearby attractions exist, OR
    - Nearby attractions count is below threshold (indicating incomplete data)
    - Updated more than 30 days ago (refresh stale data)
    """
    session = SessionLocal()
    try:
        from sqlalchemy import func
        
        # Subquery to count nearby attractions per attraction
        nearby_count_subquery = (
            session.query(
                models.NearbyAttraction.attraction_id,
                func.count(models.NearbyAttraction.id).label('nearby_count'),
                func.max(models.NearbyAttraction.created_at).label('last_updated')
            )
            .group_by(models.NearbyAttraction.attraction_id)
            .subquery()
        )
        
        threshold_date = datetime.utcnow() - timedelta(days=90)
        min_nearby_threshold = settings.NEARBY_ATTRACTIONS_COUNT  # From config
        
        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(
                nearby_count_subquery,
                models.Attraction.id == nearby_count_subquery.c.attraction_id
            )
            .filter(models.Attraction.latitude.isnot(None))
            .filter(models.Attraction.longitude.isnot(None))
            .filter(
                or_(
                    nearby_count_subquery.c.nearby_count.is_(None),  # No nearby attractions
                    nearby_count_subquery.c.nearby_count < min_nearby_threshold,  # Below threshold
                    nearby_count_subquery.c.last_updated <= threshold_date  # Stale data
                )
            )
            .all()
        )
        
        result = []
        for attraction, city in attractions:
            result.append({
                'id': attraction.id,
                'name': attraction.name,
                'slug': attraction.slug,
                'place_id': attraction.place_id,
                'city_id': city.id,
                'city_name': city.name,
                'latitude': float(attraction.latitude),
                'longitude': float(attraction.longitude),
            })
        
        logger.info(f"Found {len(result)} attractions needing nearby attractions update")
        return result
    finally:
        session.close()


@celery_app.task(name="app.tasks.nearby_attractions_tasks.update_nearby_attractions_for_attraction")
def update_nearby_attractions_for_attraction(attraction_id: int, force_refresh: bool = False) -> Dict[str, Any]:
    """No-op: nearby attractions are served from the database only."""
    logger.info(f"Skipping nearby attractions update for attraction {attraction_id}: Places API removed")
    return {"status": "skipped", "reason": "Places API removed"}


@celery_app.task(name="app.tasks.nearby_attractions_tasks.update_nearby_attractions_for_city")
def update_nearby_attractions_for_city(city_id: int) -> Dict[str, Any]:
    """Update nearby attractions for all attractions in a city.
    
    This task is triggered when:
    1. A new attraction is added to a city (update all attractions in that city)
    2. Periodic refresh task runs for a specific city
    
    Args:
        city_id: ID of the city
    
    Returns:
        Dictionary with status and result details
    """
    logger.info(f"Starting nearby attractions update for city {city_id}")
    
    try:
        # Get all attractions in the city
        attractions = get_attractions_in_city(city_id)
        
        if not attractions:
            logger.warning(f"No attractions found in city {city_id}")
            return {"status": "error", "error": "No attractions in city"}
        
        logger.info(f"Updating nearby attractions for {len(attractions)} attractions in city {city_id}")
        
        # Queue tasks for each attraction
        success_count = 0
        error_count = 0
        
        for attraction in attractions:
            try:
                # Call the single attraction update task
                result = update_nearby_attractions_for_attraction.delay(attraction['id'])
                logger.info(f"Queued nearby attractions update for {attraction['name']} (task_id: {result.id})")
                success_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to queue update for {attraction['name']}: {e}")
        
        logger.info(f"City {city_id} update complete: {success_count} queued, {error_count} errors")
        return {
            "status": "success",
            "city_id": city_id,
            "attractions_count": len(attractions),
            "queued": success_count,
            "errors": error_count
        }
        
    except Exception as e:
        logger.error(f"Error updating nearby attractions for city {city_id}: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


@celery_app.task(name="app.tasks.nearby_attractions_tasks.refresh_all_nearby_attractions")
def refresh_all_nearby_attractions() -> Dict[str, Any]:
    """Periodic task to refresh nearby attractions for all attractions that need it.
    
    This task:
    1. Finds attractions with missing or stale nearby attractions data
    2. Queues update tasks for each attraction
    3. Can be scheduled to run daily/weekly
    
    Returns:
        Dictionary with status and result details
    """
    logger.info("Starting periodic nearby attractions refresh")
    
    try:
        # Get attractions needing update
        attractions = get_attractions_needing_nearby_update()
        
        if not attractions:
            logger.info("No attractions need nearby attractions update")
            return {"status": "success", "processed": 0}
        
        logger.info(f"Found {len(attractions)} attractions needing nearby attractions update")
        
        # Queue tasks for each attraction
        success_count = 0
        error_count = 0
        
        for attraction in attractions:
            try:
                result = update_nearby_attractions_for_attraction.delay(attraction['id'])
                logger.info(f"Queued nearby attractions update for {attraction['name']} (task_id: {result.id})")
                success_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to queue update for {attraction['name']}: {e}")
        
        logger.info(f"Periodic refresh complete: {success_count} queued, {error_count} errors")
        return {
            "status": "success",
            "processed": len(attractions),
            "queued": success_count,
            "errors": error_count
        }
        
    except Exception as e:
        logger.error(f"Periodic nearby attractions refresh failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


@celery_app.task(name="app.tasks.nearby_attractions_tasks.enrich_nearby_attraction_from_google")
def enrich_nearby_attraction_from_google(nearby_attraction_id: int) -> Dict[str, Any]:
    """No-op: Google Places API enrichment has been removed.

    All nearby attraction data is now served from the database.
    """
    logger.info(f"Skipping enrichment for nearby attraction {nearby_attraction_id}: Places API removed")
    return {"status": "skipped", "reason": "Places API removed"}


@celery_app.task(name="app.tasks.nearby_attractions_tasks.backfill_nearby_attractions")
def backfill_nearby_attractions(batch_size: int = 10) -> Dict[str, Any]:
    """Backfill nearby attractions for all attractions that don't have them.
    
    This is a one-time task to populate nearby attractions for existing attractions.
    Can be run in batches to avoid overwhelming the system.
    
    Args:
        batch_size: Number of attractions to process in this batch
    
    Returns:
        Dictionary with status and result details
    """
    logger.info(f"Starting nearby attractions backfill (batch_size={batch_size})")
    
    session = SessionLocal()
    try:
        from sqlalchemy import func
        
        # Find attractions without nearby attractions
        nearby_count_subquery = (
            session.query(
                models.NearbyAttraction.attraction_id,
                func.count(models.NearbyAttraction.id).label('nearby_count')
            )
            .group_by(models.NearbyAttraction.attraction_id)
            .subquery()
        )
        
        attractions = (
            session.query(models.Attraction)
            .outerjoin(
                nearby_count_subquery,
                models.Attraction.id == nearby_count_subquery.c.attraction_id
            )
            .filter(models.Attraction.latitude.isnot(None))
            .filter(models.Attraction.longitude.isnot(None))
            .filter(
                or_(
                    nearby_count_subquery.c.nearby_count.is_(None),
                    nearby_count_subquery.c.nearby_count == 0
                )
            )
            .limit(batch_size)
            .all()
        )
        
        if not attractions:
            logger.info("No attractions need backfill")
            return {"status": "success", "processed": 0}
        
        logger.info(f"Backfilling nearby attractions for {len(attractions)} attractions")
        
        # Queue tasks for each attraction
        success_count = 0
        error_count = 0
        
        for attraction in attractions:
            try:
                result = update_nearby_attractions_for_attraction.delay(attraction.id)
                logger.info(f"Queued backfill for {attraction.name} (task_id: {result.id})")
                success_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to queue backfill for {attraction.name}: {e}")
        
        logger.info(f"Backfill batch complete: {success_count} queued, {error_count} errors")
        
        # If we processed a full batch, queue another batch
        if len(attractions) == batch_size:
            logger.info("Queueing next backfill batch...")
            backfill_nearby_attractions.delay(batch_size=batch_size)
        
        return {
            "status": "success",
            "processed": len(attractions),
            "queued": success_count,
            "errors": error_count,
            "has_more": len(attractions) == batch_size
        }
        
    except Exception as e:
        logger.error(f"Backfill task failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}
    finally:
        session.close()
