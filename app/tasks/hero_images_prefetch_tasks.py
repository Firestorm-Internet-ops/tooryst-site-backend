"""Celery tasks for prefetching hero carousel images to Redis cache.

This module handles:
1. Fetching 10 hero images from Google Places API
2. Converting to WebP format at 1600px
3. Storing as base64 in Redis with 1-hour TTL
4. On-demand fetching for cache misses
"""
import asyncio
import base64
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import redis

from app.celery_app import celery_app
from app.config import settings
from app.core.settings import settings as core_settings
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.external_apis.gcs_client import image_processor

logger = logging.getLogger(__name__)

# Redis key prefix for hero images cache
HERO_IMAGES_CACHE_PREFIX = "hero_images"


def get_redis_client() -> redis.Redis:
    """Get synchronous Redis client for Celery tasks."""
    return redis.from_url(
        core_settings.get_redis_cache_url(),
        encoding="utf-8",
        decode_responses=True
    )


def get_cache_key(attraction_id: int) -> str:
    """Generate Redis cache key for attraction hero images."""
    return f"{HERO_IMAGES_CACHE_PREFIX}:{attraction_id}"


def is_cached(attraction_id: int) -> bool:
    """Check if hero images are already cached for this attraction."""
    try:
        client = get_redis_client()
        return client.exists(get_cache_key(attraction_id)) > 0
    except Exception as e:
        logger.error(f"Error checking cache for attraction {attraction_id}: {e}")
        return False


def cache_hero_images(
    attraction_id: int,
    images: List[Dict[str, Any]],
    ttl_seconds: int = None
) -> bool:
    """Store hero images in Redis cache.

    Args:
        attraction_id: Database ID
        images: List of image dicts with base64 data
        ttl_seconds: Cache TTL (default: from settings)

    Returns:
        True if cached successfully
    """
    if ttl_seconds is None:
        ttl_seconds = settings.HERO_IMAGES_CACHE_TTL

    try:
        client = get_redis_client()
        cache_key = get_cache_key(attraction_id)

        cache_data = {
            "images": images,
            "fetched_at": datetime.utcnow().isoformat(),
            "count": len(images)
        }

        client.setex(
            cache_key,
            ttl_seconds,
            json.dumps(cache_data)
        )

        logger.info(f"Cached {len(images)} hero images for attraction {attraction_id} (TTL: {ttl_seconds}s)")
        return True

    except Exception as e:
        logger.error(f"Error caching hero images for attraction {attraction_id}: {e}")
        return False


def get_cached_hero_images(attraction_id: int) -> Optional[Dict[str, Any]]:
    """Get hero images from Redis cache.

    Args:
        attraction_id: Database ID

    Returns:
        Cached data dict or None if not cached
    """
    try:
        client = get_redis_client()
        cache_key = get_cache_key(attraction_id)

        data = client.get(cache_key)
        if data:
            return json.loads(data)
        return None

    except Exception as e:
        logger.error(f"Error getting cached hero images for attraction {attraction_id}: {e}")
        return None


@celery_app.task(
    name="app.tasks.hero_images_prefetch_tasks.prefetch_hero_images",
    bind=True,
    max_retries=2,
    default_retry_delay=60
)
def prefetch_hero_images(self, attraction_id: int) -> Dict[str, Any]:
    """Prefetch hero images for a single attraction and cache in Redis.

    Called when user views a listing page to pre-populate cache
    for attractions they might click on.

    Args:
        attraction_id: Database ID of attraction

    Returns:
        Dict with status and result
    """
    # Check if already cached
    if is_cached(attraction_id):
        logger.debug(f"Hero images already cached for attraction {attraction_id}")
        return {"status": "already_cached", "attraction_id": attraction_id}

    # Acquire distributed lock to prevent concurrent fetches for same attraction
    redis_client = get_redis_client()
    lock_key = f"prefetch_lock:{attraction_id}"
    if not redis_client.set(lock_key, "1", nx=True, ex=600):
        logger.info(f"Prefetch already in progress for attraction {attraction_id}, skipping")
        return {"status": "locked", "attraction_id": attraction_id}

    # Get attraction details from DB
    session = SessionLocal()
    try:
        attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
        if not attraction:
            return {"status": "error", "error": "Attraction not found"}

        if not attraction.place_id:
            return {"status": "error", "error": "Attraction has no place_id"}

        # 1. Check all positions for permanent GCS images (no Places API call needed)
        db_gcs_images = (
            session.query(models.HeroImage)
            .filter(
                models.HeroImage.attraction_id == attraction_id,
                models.HeroImage.gcs_url_hero.isnot(None)
            )
            .order_by(models.HeroImage.position)
            .all()
        )

        final_images = []
        if db_gcs_images:
            logger.info(f"Using {len(db_gcs_images)} GCS images from DB for attraction {attraction_id}")
            final_images = [
                {
                    "position": img.position,
                    "data": img.gcs_url_hero,
                    "alt": img.alt_text or attraction.name,
                    "width": 1600,
                    "height": 900,
                }
                for img in db_gcs_images
            ]

        if not final_images:
            return {"status": "no_photos", "attraction_id": attraction_id}

        # Cache the combined images
        if cache_hero_images(attraction_id, final_images):
            return {
                "status": "success",
                "attraction_id": attraction_id,
                "count": len(final_images)
            }
        else:
            return {"status": "error", "error": "Failed to cache images"}

    except Exception as e:
        logger.error(f"Error prefetching hero images for attraction {attraction_id}: {e}")
        raise self.retry(exc=e)

    finally:
        session.close()
        redis_client.delete(lock_key)


@celery_app.task(name="app.tasks.hero_images_prefetch_tasks.prefetch_hero_images_batch")
def prefetch_hero_images_batch(attraction_ids: List[int]) -> Dict[str, Any]:
    """Prefetch hero images for multiple attractions.

    Triggered when listing page loads with multiple attractions.

    Args:
        attraction_ids: List of attraction database IDs

    Returns:
        Dict with batch results
    """
    logger.info(f"Starting batch prefetch for {len(attraction_ids)} attractions")

    results = {
        "total": len(attraction_ids),
        "already_cached": 0,
        "fetched": 0,
        "errors": 0,
        "no_photos": 0
    }

    for attraction_id in attraction_ids:
        try:
            result = prefetch_hero_images.delay(attraction_id)
            # Note: This is async - we don't wait for results here
            results["fetched"] += 1
        except Exception as e:
            logger.error(f"Error queuing prefetch for attraction {attraction_id}: {e}")
            results["errors"] += 1

    logger.info(f"Batch prefetch queued: {results}")
    return results


async def fetch_hero_images_on_demand(attraction_id: int) -> Optional[Dict[str, Any]]:
    """Fetch hero images from DB/GCS for cache miss scenario.

    Args:
        attraction_id: Database ID

    Returns:
        Cached data format or None
    """
    session = SessionLocal()
    try:
        attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
        if not attraction:
            return None

        hero_images = (
            session.query(models.HeroImage)
            .filter(
                models.HeroImage.attraction_id == attraction_id,
                models.HeroImage.gcs_url_hero.isnot(None)
            )
            .order_by(models.HeroImage.position)
            .all()
        )

        if not hero_images:
            return None

        final_images = [
            {
                "position": img.position,
                "data": img.gcs_url_hero,
                "alt": img.alt_text or attraction.name,
                "width": 1600,
                "height": 900,
            }
            for img in hero_images
        ]

        cache_hero_images(attraction_id, final_images)

        return {
            "images": final_images,
            "fetched_at": datetime.utcnow().isoformat(),
            "count": len(final_images),
            "source": "fetched"
        }

    finally:
        session.close()
