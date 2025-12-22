"""Gemini-based fallback for Map data (minimal fallback as map data is static)."""
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class GeminiMapFallback:
    """Generate map data fallback (map data doesn't really need AI fallback)."""
    
    async def generate_map_data(
        self,
        attraction_name: str,
        city_name: str,
        latitude: float,
        longitude: float
    ) -> Optional[Dict[str, Any]]:
        """Generate map data fallback.
        
        Note: Map data is static and generated from coordinates,
        so this fallback just returns the same data without API key.
        """
        logger.warning(f"Using fallback for map data: {attraction_name}")
        
        # Generate URLs without API key
        static_map_image_url = (
            f"https://maps.googleapis.com/maps/api/staticmap?"
            f"center={latitude},{longitude}&"
            f"zoom=15&"
            f"size=800x600&"
            f"markers=color:red%7C{latitude},{longitude}"
        )
        
        maps_link_url = f"https://maps.google.com/?q={latitude},{longitude}"
        directions_url = f"https://www.google.com/maps/dir/?api=1&destination={latitude},{longitude}"
        
        # Generate basic address
        address = f"{attraction_name}, {city_name}"
        
        return {
            "card": {
                "latitude": latitude,
                "longitude": longitude,
                "static_map_image_url": static_map_image_url,
                "maps_link_url": maps_link_url,
                "address": address
            },
            "section": {
                "latitude": latitude,
                "longitude": longitude,
                "address": address,
                "directions_url": directions_url,
                "zoom_level": 15
            },
            "source": "fallback"
        }
