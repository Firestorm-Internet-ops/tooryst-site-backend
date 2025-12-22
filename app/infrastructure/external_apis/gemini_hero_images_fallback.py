"""Gemini-based fallback for Hero Images when Google Places API fails."""
import logging
from typing import Optional, Dict, Any
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class GeminiHeroImagesFallback:
    """Generate hero images data using Gemini AI when Google Places API is unavailable."""
    
    def __init__(self, client: Optional[GeminiClient] = None):
        self.client = client or GeminiClient()
    
    async def generate_hero_images(
        self,
        attraction_name: str,
        city_name: str,
        max_images: int = 10
    ) -> Optional[Dict[str, Any]]:
        """Generate hero images data using Gemini AI.
        
        Note: This is a fallback used only when Google Places fails.
        We now avoid external stock sources (e.g., Unsplash) and return
        no images so callers can decide to skip/leave empty.
        """
        logger.warning(f"Using Gemini fallback for hero images: {attraction_name}")
        
        # Generate descriptive alt texts using Gemini
        prompt = f"""Generate {max_images} descriptive alt text variations for photos of {attraction_name} in {city_name}.

Return ONLY a JSON array of strings, each describing a different view or aspect of the attraction.

Example format:
["exterior view from street level", "architectural detail of facade", "panoramic view", ...]

Be specific and descriptive. Return ONLY the JSON array, no other text."""

        result = await self.client.generate_json(prompt)
        
        if not result or not isinstance(result, list):
            logger.error("Failed to generate alt texts with Gemini")
            # Return basic fallback
            alt_texts = [
                f"{attraction_name} exterior view",
                f"{attraction_name} architectural detail",
                f"{attraction_name} panoramic view",
                f"{attraction_name} close-up perspective",
                f"{attraction_name} from street level"
            ]
        else:
            alt_texts = result[:max_images]
        
        # Do not use stock/Unsplash; return empty so upstream can skip.
        logger.info("Skipping stock images; returning empty hero images payload.")
        return {
            "images": [],
            "source": "gemini_fallback"
        }
