"""Celery tasks for card image refresh and GCS upload.

This module handles:
1. Daily batch processing of attractions to refresh card images
2. Downloading the primary image from Google Places API
3. Converting to WebP format in 2 sizes (400px card, 1600px hero)
4. Uploading to GCS bucket
5. Updating database with GCS URLs

The staggered approach processes ~11 attractions per day to complete
a full refresh cycle every 29 days.
"""
import asyncio
import math
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from app.celery_app import celery_app
from app.config import settings
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.external_apis.hero_images_fetcher import GooglePlacesHeroImagesFetcher
from app.infrastructure.external_apis.gcs_client import gcs_client, image_processor
from app.infrastructure.external_apis.google_places_client import PlaceIdInvalidError

logger = logging.getLogger(__name__)


def get_attractions_needing_card_refresh(batch_size: int) -> List[Dict[str, Any]]:
    """Get attractions that need card image refresh.

    Priority order:
    1. Attractions with no GCS URLs (never processed)
    2. Attractions with oldest last_refreshed_at

    Args:
        batch_size: Maximum number of attractions to return

    Returns:
        List of attraction dicts with id, place_id, name, city_name
    """
    session = SessionLocal()
    try:
        refresh_threshold = datetime.now() - timedelta(days=settings.CARD_IMAGE_REFRESH_DAYS)

        # Get all attractions with place_id
        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .filter(models.Attraction.place_id.isnot(None))
            .filter(models.Attraction.place_id != "")
            .all()
        )

        # Filter and sort in Python for more control
        attraction_data = []
        for attraction, city in attractions:
            # Check card image (position=1) for this attraction
            card_image = (
                session.query(models.HeroImage)
                .filter(models.HeroImage.attraction_id == attraction.id)
                .filter(models.HeroImage.position == 1)
                .first()
            )

            # Determine if needs refresh
            needs_refresh = False
            last_refresh = None

            if not card_image:
                # No card image at all - needs processing
                needs_refresh = True
            elif not card_image.gcs_url_card or not card_image.gcs_url_hero:
                # Missing GCS URLs
                needs_refresh = True
            elif card_image.last_refreshed_at:
                last_refresh = card_image.last_refreshed_at
                if last_refresh < refresh_threshold:
                    needs_refresh = True
            else:
                # No refresh time recorded
                needs_refresh = True

            if needs_refresh:
                attraction_data.append({
                    'id': attraction.id,
                    'place_id': attraction.place_id,
                    'name': attraction.name,
                    'city_name': city.name,
                    'last_refresh': last_refresh,
                    'has_gcs': bool(card_image and card_image.gcs_url_card)
                })

        # Sort: prioritize those without GCS URLs, then by oldest refresh
        attraction_data.sort(key=lambda x: (
            x['has_gcs'],  # False (no GCS) comes before True
            x['last_refresh'] or datetime.min  # Oldest first, None treated as oldest
        ))

        result = attraction_data[:batch_size]
        logger.info(f"Found {len(result)} attractions needing card refresh (total candidates: {len(attraction_data)})")
        return result

    except Exception as e:
        logger.error(f"Error getting attractions for card refresh: {e}")
        return []
    finally:
        session.close()


async def process_card_image(
    attraction_id: int,
    place_id: str,
    attraction_name: str
) -> Dict[str, Any]:
    """Process card image for a single attraction.

    Pipeline:
    1. Fetch first photo reference from Google Places
    2. Download the photo
    3. Convert to WebP at two sizes (400px card, 1600px hero)
    4. Upload both to GCS
    5. Update database with GCS URLs

    Args:
        attraction_id: Database ID of attraction
        place_id: Google Place ID
        attraction_name: Name for logging and alt text

    Returns:
        Dict with status
    """
    fetcher = GooglePlacesHeroImagesFetcher()
    session = SessionLocal()

    try:
        # 1. Fetch photo references (we only need the first one)
        logger.info(f"Fetching photo reference for {attraction_name}")
        photo_refs = await fetcher.fetch_photo_references(place_id)
        if not photo_refs:
            logger.warning(f"No photos found for {attraction_name}")
            return {"status": "no_photos"}

        # Use the first photo only
        photo_ref = photo_refs[0]["photo_reference"]

        # 2. Download the photo at max size
        logger.debug(f"Downloading photo for {attraction_name}")
        image_bytes = await fetcher.download_photo_from_reference(
            photo_ref,
            max_width=settings.IMAGE_SIZE_HERO
        )

        if not image_bytes:
            return {"status": "error", "error": "Failed to download photo"}

        # 3. Process to WebP at both sizes
        # Hero size (1600px)
        try:
            hero_webp, _, _ = image_processor.process_image(
                image_bytes,
                target_width=settings.IMAGE_SIZE_HERO,
                quality=settings.IMAGE_QUALITY_WEBP
            )
        except ValueError as e:
            return {"status": "error", "error": f"Failed to process hero image: {e}"}

        # Card size (400px)
        try:
            card_webp, _, _ = image_processor.process_image(
                image_bytes,
                target_width=settings.IMAGE_SIZE_CARD,
                quality=settings.IMAGE_QUALITY_WEBP
            )
        except ValueError as e:
            return {"status": "error", "error": f"Failed to process card image: {e}"}

        # 4. Upload to GCS
        # Card: /attractions/{id}/card.webp (400px)
        # Hero: /attractions/{id}/hero.webp (1600px)
        card_path = f"attractions/{attraction_id}/card.webp"
        hero_path = f"attractions/{attraction_id}/hero.webp"

        card_url = gcs_client.upload_image(card_webp, card_path)
        hero_url = gcs_client.upload_image(hero_webp, hero_path)

        if not card_url or not hero_url:
            return {"status": "error", "error": "Failed to upload to GCS"}

        # 5. Update database
        existing = (
            session.query(models.HeroImage)
            .filter(
                models.HeroImage.attraction_id == attraction_id,
                models.HeroImage.position == 1
            )
            .first()
        )

        now = datetime.utcnow()
        alt_text = f"{attraction_name} exterior view"

        if existing:
            existing.google_photo_reference = photo_ref
            existing.gcs_url_card = card_url
            existing.gcs_url_hero = hero_url
            existing.last_refreshed_at = now
            existing.url = hero_url  # Update fallback URL
        else:
            new_image = models.HeroImage(
                attraction_id=attraction_id,
                url=hero_url,
                alt_text=alt_text,
                position=1,
                google_photo_reference=photo_ref,
                gcs_url_card=card_url,
                gcs_url_hero=hero_url,
                last_refreshed_at=now,
                created_at=now
            )
            session.add(new_image)

        session.commit()
        logger.info(f"Processed card image for {attraction_name}")

        return {
            "status": "success",
            "card_url": card_url,
            "hero_url": hero_url
        }

    except PlaceIdInvalidError as e:
        session.rollback()
        logger.warning(f"Invalid place_id for {attraction_name}: {e}")
        return {"status": "invalid_place_id", "error": str(e)}

    except Exception as e:
        session.rollback()
        logger.error(f"Error processing card image for {attraction_name}: {e}")
        return {"status": "error", "error": str(e)}

    finally:
        session.close()


@celery_app.task(
    name="app.tasks.hero_images_refresh_tasks.refresh_card_images_batch",
    bind=True,
    max_retries=3,
    default_retry_delay=300
)
def refresh_card_images_batch(self):
    """Daily Celery task to refresh card images for a batch of attractions.

    Runs once per day, processing ~11 attractions to complete a full
    refresh cycle every 29 days.
    """
    logger.info("=" * 60)
    logger.info("Starting daily card images refresh task")
    logger.info("=" * 60)

    try:
        session = SessionLocal()
        total_attractions = session.query(models.Attraction).filter(models.Attraction.place_id.isnot(None)).count()
        session.close()

        # Calculate batch size to finish all attractions in target days (default 25)
        # Always use at least the configured daily batch size as a floor
        calculated_batch = math.ceil(total_attractions / settings.CARD_IMAGE_REFRESH_TARGET_DAYS)
        batch_size = max(calculated_batch, settings.CARD_IMAGES_DAILY_BATCH_SIZE)

        logger.info(f"Dynamic batch size calculation: {total_attractions} attractions / {settings.CARD_IMAGE_REFRESH_TARGET_DAYS} days = {calculated_batch}")
        logger.info(f"Using batch size: {batch_size}")

        attractions = get_attractions_needing_card_refresh(batch_size)

        if not attractions:
            logger.info("No attractions need card image refresh")
            return {"status": "success", "processed": 0, "message": "No attractions need refresh"}

        success_count = 0
        error_count = 0
        results = []

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            for attraction in attractions:
                try:
                    logger.info(f"Processing: {attraction['name']} (ID: {attraction['id']})")

                    result = loop.run_until_complete(
                        process_card_image(
                            attraction_id=attraction['id'],
                            place_id=attraction['place_id'],
                            attraction_name=attraction['name']
                        )
                    )

                    results.append({
                        "attraction": attraction['name'],
                        "result": result
                    })

                    if result['status'] == 'success':
                        success_count += 1
                    else:
                        error_count += 1

                    # Small delay between attractions
                    loop.run_until_complete(asyncio.sleep(1))

                except Exception as e:
                    logger.error(f"Error processing {attraction['name']}: {e}")
                    error_count += 1
                    results.append({
                        "attraction": attraction['name'],
                        "result": {"status": "error", "error": str(e)}
                    })

        finally:
            loop.close()

        logger.info("=" * 60)
        logger.info(f"Card images refresh complete: {success_count} success, {error_count} errors")
        logger.info("=" * 60)

        return {
            "status": "success",
            "processed": len(attractions),
            "success": success_count,
            "errors": error_count,
            "details": results
        }

    except Exception as e:
        logger.error(f"Card images refresh task failed: {e}")
        raise self.retry(exc=e)


@celery_app.task(name="app.tasks.hero_images_refresh_tasks.refresh_single_card_image")
def refresh_single_card_image(attraction_id: int) -> Dict[str, Any]:
    """Refresh card image for a single attraction (manual trigger).

    Args:
        attraction_id: Database ID of attraction

    Returns:
        Dict with status and result
    """
    session = SessionLocal()

    try:
        attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
        if not attraction:
            return {"status": "error", "error": "Attraction not found"}

        if not attraction.place_id:
            return {"status": "error", "error": "Attraction has no place_id"}

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = loop.run_until_complete(
                process_card_image(
                    attraction_id=attraction.id,
                    place_id=attraction.place_id,
                    attraction_name=attraction.name
                )
            )
            return result
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"Error refreshing card image for attraction {attraction_id}: {e}")
        return {"status": "error", "error": str(e)}

    finally:
        session.close()
