"""Hero Images Fetcher implementation using Google Places API."""
import os
from typing import Optional, Dict, Any
import logging
from app.config import settings
from .google_places_client import GooglePlacesClient
from .gemini_hero_images_fallback import GeminiHeroImagesFallback

logger = logging.getLogger(__name__)


class GooglePlacesHeroImagesFetcher:
    """Fetches hero images from Google Places API with Gemini fallback."""
    
    def __init__(
        self,
        client: Optional[GooglePlacesClient] = None,
        fallback: Optional[GeminiHeroImagesFallback] = None
    ):
        self.client = client or GooglePlacesClient()
        self.fallback = fallback or GeminiHeroImagesFallback()
        self.max_images = int(os.getenv("HERO_CAROUSEL_IMAGE_COUNT", str(settings.COLLAGE_IMAGE_LIMIT)))
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str],
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch hero images for an attraction.
        
        Returns:
            {"images": [{"url": str, "alt": str, "position": int}], "source": str} or None
        """
        # Try primary API first
        if place_id:
            try:
                # Fetch photo URLs using Places API (New) photos endpoint
                max_width = settings.HERO_IMAGE_MAX_WIDTH
                photo_urls = await self.client.get_place_photo_urls(
                    place_id=place_id,
                    max_width_px=max_width,
                    limit=self.max_images,
                )
                if photo_urls:
                    return self._build_images_payload(photo_urls, attraction_name or place_id)
                else:
                    logger.warning(f"No photos found for place_id {place_id}")
            except Exception as e:
                logger.error(f"Error fetching from Google Places API: {e}")
        else:
            logger.warning(f"No place_id provided for attraction {attraction_id}")
        
        # Fall back to Gemini
        if attraction_name and city_name:
            logger.info(f"Falling back to Gemini for hero images: {attraction_name}")
            try:
                return await self.fallback.generate_hero_images(
                    attraction_name=attraction_name,
                    city_name=city_name,
                    max_images=self.max_images
                )
            except Exception as e:
                logger.error(f"Gemini fallback failed: {e}")
        
        return None
    
    def _build_images_payload(self, photo_urls: list, attraction_name: str) -> Dict[str, Any]:
        """Build image payload with descriptive alt text."""
        images = []
        alt_variations = [
            "exterior view",
            "architectural detail",
            "panoramic view",
            "close-up perspective",
            "daytime view",
            "evening view",
            "from street level",
            "aerial perspective",
            "surrounding area",
            "entrance view"
        ]
        
        for idx, url in enumerate(photo_urls):
            alt_descriptor = alt_variations[idx % len(alt_variations)]
            alt_text = f"{attraction_name} {alt_descriptor}"
            images.append({
                "url": url,
                "alt": alt_text,
                "position": idx + 1
            })
        
        return {
            "images": images,
            "source": "google_places_api"
        }
