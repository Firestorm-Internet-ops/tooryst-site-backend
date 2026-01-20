"""Hero Images Fetcher implementation using Google Places API."""
import os
from typing import Optional, Dict, Any, List
import logging
import httpx
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
        self.max_images = int(os.getenv("HERO_CAROUSEL_IMAGE_COUNT", "10"))
    
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

    async def fetch_photo_references(
        self,
        place_id: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch photo references only (not URLs) for storage.

        This method returns the photo.name references which can be used later
        to download images. This is useful for the GCS migration where we want
        to store the reference and download on-demand.

        Args:
            place_id: Google Place ID

        Returns:
            List of {"photo_reference": str, "attributions": list} or None
        """
        if not self.client.api_key:
            logger.error("Cannot fetch photo references: API key missing")
            return None

        details = await self.client.get_place_details(place_id)
        if not details:
            logger.warning(f"No place details found for {place_id}")
            return None

        photos = details.get("photos", [])
        if not photos:
            logger.warning(f"No photos found for {place_id}")
            return None

        results = []
        for photo in photos[:self.max_images]:
            photo_name = photo.get("name")  # e.g., "places/ChIJ.../photos/ABC123"
            if photo_name:
                results.append({
                    "photo_reference": photo_name,
                    "attributions": photo.get("authorAttributions", [])
                })

        logger.info(f"Found {len(results)} photo references for {place_id}")
        return results

    async def download_photo_from_reference(
        self,
        photo_reference: str,
        max_width: int = 1600
    ) -> Optional[bytes]:
        """Download photo bytes from a photo reference.

        Args:
            photo_reference: The photo.name from Places API (e.g., "places/.../photos/...")
            max_width: Maximum width in pixels

        Returns:
            Image bytes or None if download failed
        """
        if not self.client.api_key:
            logger.error("Cannot download photo: API key missing")
            return None

        url = f"https://places.googleapis.com/v1/{photo_reference}/media"
        params = {
            "maxWidthPx": max_width,
            "key": self.client.api_key,
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Follow redirect to get the actual image
                response = await client.get(url, params=params, follow_redirects=True)
                response.raise_for_status()

                # Check content type to ensure we got an image
                content_type = response.headers.get("content-type", "")
                if not content_type.startswith("image/"):
                    logger.error(f"Unexpected content type: {content_type}")
                    return None

                logger.debug(f"Downloaded {len(response.content)} bytes for {photo_reference}")
                return response.content

        except httpx.TimeoutException:
            logger.error(f"Timeout downloading photo: {photo_reference}")
            return None
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error downloading photo: {e}")
            return None
        except Exception as e:
            logger.error(f"Error downloading photo: {e}")
            return None
