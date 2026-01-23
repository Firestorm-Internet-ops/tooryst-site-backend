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
from app.infrastructure.external_apis.hero_images_fetcher import GooglePlacesHeroImagesFetcher
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


async def fetch_and_process_hero_images(
    attraction_id: int,
    place_id: str,
    attraction_name: str,
    max_images: int = 10,
    skip_count: int = 0
) -> Optional[List[Dict[str, Any]]]:
    """Fetch hero images from Google Places and process to WebP.

    Args:
        attraction_id: Database ID of attraction
        place_id: Google Place ID
        attraction_name: Name for alt text
        max_images: Maximum number of images to fetch

    Returns:
        List of image dicts with base64 data, or None on error
    """
    fetcher = GooglePlacesHeroImagesFetcher()
    images = []

    try:
        # 1. Fetch photo references
        logger.info(f"Fetching photo references for {attraction_name}")
        photo_refs = await fetcher.fetch_photo_references(place_id)

        if not photo_refs:
            logger.warning(f"No photos found for {attraction_name}")
            return None

        # Apply skip and limit
        if skip_count > 0:
            logger.info(f"Skipping first {skip_count} photo references for {attraction_name}")
            photo_refs = photo_refs[skip_count:]
            
        photo_refs = photo_refs[:max_images]

        # 2. Download and process each photo
        for idx, ref in enumerate(photo_refs):
            try:
                photo_reference = ref["photo_reference"]

                # Download at hero size
                image_bytes = await fetcher.download_photo_from_reference(
                    photo_reference,
                    max_width=settings.IMAGE_SIZE_HERO
                )

                if not image_bytes:
                    logger.warning(f"Failed to download photo {idx + 1} for {attraction_name}")
                    continue

                # Convert to WebP
                webp_bytes, width, height = image_processor.process_image(
                    image_bytes,
                    target_width=settings.IMAGE_SIZE_HERO,
                    quality=settings.IMAGE_QUALITY_WEBP
                )

                # Encode to base64
                base64_data = base64.b64encode(webp_bytes).decode('utf-8')

                images.append({
                    "position": idx + 1 + skip_count,
                    "data": f"data:image/webp;base64,{base64_data}",
                    "alt": f"{attraction_name} - image {idx + 1 + skip_count}",
                    "width": width,
                    "height": height
                })

                # Small delay between downloads
                await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"Error processing photo {idx + 1} for {attraction_name}: {e}")
                continue

        if images:
            logger.info(f"Processed {len(images)} hero images for {attraction_name}")
            return images

        return None

    except Exception as e:
        logger.error(f"Error fetching hero images for {attraction_name}: {e}")
        return None


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

    # Get attraction details from DB
    session = SessionLocal()
    try:
        attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
        if not attraction:
            return {"status": "error", "error": "Attraction not found"}

        if not attraction.place_id:
            return {"status": "error", "error": "Attraction has no place_id"}

        # 1. Start with the existing GCS hero image if available (Position 0)
        final_images = []
        hero_img = session.query(models.HeroImage).filter_by(
            attraction_id=attraction_id, 
            position=0
        ).first()

        if hero_img and hero_img.gcs_url_hero:
            logger.info(f"Using GCS URL for position 0 of attraction {attraction_id}")
            final_images.append({
                "position": 0,
                "data": hero_img.gcs_url_hero,
                "alt": f"{attraction.name} - Official Hero",
                "width": 1600,
                "height": 900
            })

        # 2. Fetch and process remaining images (max 9 more from Google)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # We want total 10 images. If we have 1 already, fetch 9 more.
            # We skip the 1st one if we use it, but typically position 0 in DB
            # might be the same as the first one from Google.
            # For simplicity, we'll skip the first one from Google if we have a GCS hero.
            prefetch_count = 10 - len(final_images)
            skip_count = 1 if len(final_images) > 0 else 0

            prefetched_images = loop.run_until_complete(
                fetch_and_process_hero_images(
                    attraction_id=attraction.id,
                    place_id=attraction.place_id,
                    attraction_name=attraction.name,
                    max_images=prefetch_count,
                    skip_count=skip_count
                )
            )
        finally:
            loop.close()

        if prefetched_images:
            final_images.extend(prefetched_images)

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
    """Fetch hero images synchronously for cache miss scenario.

    Used when user clicks on attraction but images aren't cached.
    This is a blocking call that fetches and caches images immediately.

    Args:
        attraction_id: Database ID

    Returns:
        Cached data format or None
    """
    # Get attraction details
    session = SessionLocal()
    try:
        attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
        if not attraction or not attraction.place_id:
            return None

        # 1. Get GCS hero if exists
        final_images = []
        hero_img = session.query(models.HeroImage).filter_by(
            attraction_id=attraction_id, 
            position=0
        ).first()

        if hero_img and hero_img.gcs_url_hero:
            final_images.append({
                "position": 0,
                "data": hero_img.gcs_url_hero,
                "alt": f"{attraction.name} - Official Hero",
                "width": 1600,
                "height": 900
            })

        # 2. Fetch images from Google (remaining 9)
        prefetch_count = 10 - len(final_images)
        skip_count = 1 if len(final_images) > 0 else 0

        prefetched_images = await fetch_and_process_hero_images(
            attraction_id=attraction.id,
            place_id=attraction.place_id,
            attraction_name=attraction.name,
            max_images=prefetch_count,
            skip_count=skip_count
        )

        if prefetched_images:
            final_images.extend(prefetched_images)

        if not final_images:
            return None

        # Cache for future requests
        cache_hero_images(attraction_id, final_images)

        return {
            "images": final_images,
            "fetched_at": datetime.utcnow().isoformat(),
            "count": len(final_images),
            "source": "fetched"
        }

    finally:
        session.close()
