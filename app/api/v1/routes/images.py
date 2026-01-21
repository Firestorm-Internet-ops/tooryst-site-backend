"""Image API routes for hero carousel prefetch and retrieval."""
import asyncio
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from app.tasks.hero_images_prefetch_tasks import (
    prefetch_hero_images,
    prefetch_hero_images_batch,
    get_cached_hero_images,
    fetch_hero_images_on_demand,
)
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.external_apis.gcs_client import gcs_client, image_processor
from app.infrastructure.external_apis.hero_images_fetcher import GooglePlacesHeroImagesFetcher

logger = logging.getLogger(__name__)

router = APIRouter(tags=["images"])


class PrefetchRequest(BaseModel):
    """Request body for prefetching hero images."""
    attraction_ids: List[int]


class PrefetchResponse(BaseModel):
    """Response for prefetch request."""
    status: str
    count: int
    message: str


class HeroImage(BaseModel):
    """Single hero image data."""
    position: int
    data: str  # base64 data URL
    alt: str
    width: Optional[int] = None
    height: Optional[int] = None


class HeroImagesResponse(BaseModel):
    """Response for hero images request."""
    images: List[HeroImage]
    count: int
    source: str  # "cache" or "fetched"
    fetched_at: Optional[str] = None


@router.post("/prefetch-hero-images", response_model=PrefetchResponse)
async def prefetch_hero_images_endpoint(request: PrefetchRequest):
    """
    Trigger background prefetch of hero images for multiple attractions.

    Called by frontend when listing page loads to pre-populate cache
    for attractions the user might click on.

    This is a fire-and-forget operation - images will be fetched
    asynchronously via Celery workers.
    """
    if not request.attraction_ids:
        raise HTTPException(status_code=400, detail="No attraction IDs provided")

    if len(request.attraction_ids) > 50:
        raise HTTPException(
            status_code=400,
            detail="Maximum 50 attractions per prefetch request"
        )

    # Queue background tasks for each attraction
    for attraction_id in request.attraction_ids:
        prefetch_hero_images.delay(attraction_id)

    return PrefetchResponse(
        status="prefetch_started",
        count=len(request.attraction_ids),
        message=f"Prefetch started for {len(request.attraction_ids)} attractions"
    )


@router.get("/hero-images/{attraction_id}", response_model=HeroImagesResponse)
async def get_hero_images_endpoint(attraction_id: int):
    """
    Get hero carousel images for an attraction.

    First checks Redis cache for prefetched images.
    If not cached, fetches on-demand (blocking) and caches for future requests.

    Returns:
        List of hero images with base64 data URLs for carousel display.
    """
    # Try cache first
    cached_data = get_cached_hero_images(attraction_id)

    if cached_data:
        return HeroImagesResponse(
            images=[HeroImage(**img) for img in cached_data["images"]],
            count=cached_data["count"],
            source="cache",
            fetched_at=cached_data.get("fetched_at")
        )

    # Cache miss - fetch on demand
    fetched_data = await fetch_hero_images_on_demand(attraction_id)

    if not fetched_data:
        raise HTTPException(
            status_code=404,
            detail=f"No hero images found for attraction {attraction_id}"
        )

    return HeroImagesResponse(
        images=[HeroImage(**img) for img in fetched_data["images"]],
        count=fetched_data["count"],
        source="fetched",
        fetched_at=fetched_data.get("fetched_at")
    )


@router.get("/image/{attraction_id}/{position}")
async def get_hero_image_proxy(attraction_id: int, position: int):
    """
    Image proxy that serves hero images from GCS CDN.

    For positions 1-9:
    - Checks if image already exists in GCS (via gcs_url_hero in DB)
    - If yes → redirect to CDN URL
    - If no → fetch from Google Places API → upload to GCS → update DB → redirect

    Args:
        attraction_id: The attraction ID
        position: Image position (0-9)

    Returns:
        302 redirect to the CDN URL
    """
    if position < 0 or position > 9:
        raise HTTPException(status_code=400, detail="Position must be between 0 and 9")

    session = SessionLocal()
    try:
        # Get hero image record for this position
        hero = (
            session.query(models.HeroImage)
            .filter(
                models.HeroImage.attraction_id == attraction_id,
                models.HeroImage.position == position
            )
            .first()
        )

        if not hero:
            raise HTTPException(
                status_code=404,
                detail=f"Image not found for attraction {attraction_id} at position {position}"
            )

        # If already in GCS, redirect immediately
        if hero.gcs_url_hero:
            logger.debug(f"Cache hit: redirecting to {hero.gcs_url_hero}")
            return RedirectResponse(hero.gcs_url_hero, status_code=302)

        # Need to fetch from Google Places and cache to GCS
        attraction = (
            session.query(models.Attraction)
            .filter(models.Attraction.id == attraction_id)
            .first()
        )

        if not attraction:
            raise HTTPException(status_code=404, detail=f"Attraction {attraction_id} not found")

        if not attraction.place_id:
            raise HTTPException(
                status_code=404,
                detail=f"Attraction {attraction_id} has no place_id for image fetching"
            )

        # Fetch photo references from Google Places
        fetcher = GooglePlacesHeroImagesFetcher()
        photo_refs = await fetcher.fetch_photo_references(attraction.place_id)

        if not photo_refs:
            raise HTTPException(
                status_code=404,
                detail=f"No photos found for attraction {attraction_id}"
            )

        if position >= len(photo_refs):
            raise HTTPException(
                status_code=404,
                detail=f"Image position {position} not available (only {len(photo_refs)} photos)"
            )

        # Download the photo
        photo_reference = photo_refs[position]["photo_reference"]
        image_bytes = await fetcher.download_photo_from_reference(
            photo_reference,
            max_width=1600
        )

        if not image_bytes:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to download image for attraction {attraction_id}"
            )

        # Convert to WebP
        try:
            webp_bytes, width, height = image_processor.process_image(image_bytes, 1600)
        except ValueError as e:
            logger.error(f"Failed to process image: {e}")
            raise HTTPException(status_code=500, detail="Failed to process image")

        # Upload to GCS
        cdn_url = gcs_client.upload_hero_image(attraction_id, position, webp_bytes)

        if not cdn_url:
            raise HTTPException(status_code=500, detail="Failed to upload image to GCS")

        # Update database with GCS URL
        hero.gcs_url_hero = cdn_url

        # Also store the photo reference for future refreshes
        if not hero.google_photo_reference:
            hero.google_photo_reference = photo_reference

        session.commit()

        logger.info(f"Cached image to GCS: attraction={attraction_id}, position={position}, url={cdn_url}")

        # Redirect to CDN
        return RedirectResponse(cdn_url, status_code=302)

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Error in image proxy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        session.close()
