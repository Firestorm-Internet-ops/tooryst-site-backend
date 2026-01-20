"""Image API routes for hero carousel prefetch and retrieval."""
import asyncio
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.tasks.hero_images_prefetch_tasks import (
    prefetch_hero_images,
    prefetch_hero_images_batch,
    get_cached_hero_images,
    fetch_hero_images_on_demand,
)

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
