"""Map Fetcher implementation using Google Maps Static API."""
import os
from typing import Optional, Dict, Any
import logging
from app.config import settings
from .gemini_map_fallback import GeminiMapFallback

logger = logging.getLogger(__name__)


class MapFetcherImpl:
    """Fetches map data using Google Maps Static API with fallback."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        fallback: Optional[GeminiMapFallback] = None
    ):
        self.api_key = api_key or os.getenv("GOOGLE_MAPS_API_KEY")
        self.fallback = fallback or GeminiMapFallback()
        if not self.api_key:
            logger.warning("GOOGLE_MAPS_API_KEY not set")
    
    def _generate_static_map_url(
        self,
        latitude: float,
        longitude: float,
        zoom: int = None,
        width: int = None,
        height: int = None
    ) -> str:
        """Generate Google Maps Static API URL."""
        if zoom is None:
            zoom = settings.MAP_DEFAULT_ZOOM
        if width is None:
            width = settings.MAP_SNAPSHOT_WIDTH
        if height is None:
            height = settings.MAP_SNAPSHOT_HEIGHT
            
        if not self.api_key:
            return f"https://maps.googleapis.com/maps/api/staticmap?center={latitude},{longitude}&zoom={zoom}&size={width}x{height}"
        
        # Add marker for the location
        marker = f"markers=color:red%7C{latitude},{longitude}"
        
        return (
            f"https://maps.googleapis.com/maps/api/staticmap?"
            f"center={latitude},{longitude}&"
            f"zoom={zoom}&"
            f"size={width}x{height}&"
            f"{marker}&"
            f"key={self.api_key}"
        )
    
    def _generate_maps_link_url(self, latitude: float, longitude: float) -> str:
        """Generate Google Maps link URL."""
        return f"https://maps.google.com/?q={latitude},{longitude}"
    
    def _generate_directions_url(self, latitude: float, longitude: float) -> str:
        """Generate Google Maps directions URL."""
        return f"https://www.google.com/maps/dir/?api=1&destination={latitude},{longitude}"
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str],
        latitude: float = None,
        longitude: float = None,
        address: Optional[str] = None,
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None,
        zoom_level: int = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch map data for an attraction.
        
        Returns:
        - card: Map data for the card view
        - section: Map data for the section view
        - source: "google_maps_api" or "fallback"
        """
        # Use defaults if not provided
        if latitude is None:
            latitude = settings.DEFAULT_LATITUDE
        if longitude is None:
            longitude = settings.DEFAULT_LONGITUDE
        if zoom_level is None:
            zoom_level = settings.MAP_DEFAULT_ZOOM
            
        # Try with API key first
        if self.api_key:
            try:
                # Generate URLs with API key
                static_map_image_url = self._generate_static_map_url(latitude, longitude, zoom_level)
                maps_link_url = self._generate_maps_link_url(latitude, longitude)
                directions_url = self._generate_directions_url(latitude, longitude)
                
                # Use provided address or generate basic one
                if not address and attraction_name and city_name:
                    address = f"{attraction_name}, {city_name}"
                elif not address:
                    address = f"Location at {latitude}, {longitude}"
                
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
                        "zoom_level": zoom_level
                    },
                    "source": "google_maps_api"
                }
            except Exception as e:
                logger.error(f"Error generating map URLs with API key: {e}")
        
        # Fall back to no API key
        logger.info(f"Using fallback for map data (no API key)")
        try:
            return await self.fallback.generate_map_data(
                attraction_name=attraction_name or "Attraction",
                city_name=city_name or "City",
                latitude=latitude,
                longitude=longitude
            )
        except Exception as e:
            logger.error(f"Map fallback failed: {e}")
        
        return None
