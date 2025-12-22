"""Google Places API client for fetching attraction data."""
import os
import httpx
import math
from typing import Optional, Dict, Any, List
import logging

from app.constants import EARTH_RADIUS_KM

logger = logging.getLogger(__name__)


class GooglePlacesClient:
    """Client for Google Places API (Place Details, Photos, Nearby Search)."""
    
    BASE_URL = "https://maps.googleapis.com/maps/api"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
        if not self.api_key:
            logger.warning("GOOGLE_PLACES_API_KEY not set")
    
    async def find_place(
        self,
        query: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None
    ) -> Optional[Dict[str, Any]]:
        """Find a place by text query.
        
        Args:
            query: Search query (e.g., "Eiffel Tower Paris")
            latitude: Optional latitude for location bias
            longitude: Optional longitude for location bias
        
        Returns:
            First candidate with place_id or None if not found
        """
        if not self.api_key:
            logger.error("Cannot find place: API key missing")
            return None
        
        url = f"{self.BASE_URL}/place/findplacefromtext/json"
        params = {
            "input": query,
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address,geometry,rating,user_ratings_total",
            "key": self.api_key
        }
        
        if latitude is not None and longitude is not None:
            params["locationbias"] = f"point:{latitude},{longitude}"
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") not in ["OK", "ZERO_RESULTS"]:
                    logger.error(f"Google Places API error: {data.get('status')} - {data.get('error_message')}")
                    return None
                
                candidates = data.get("candidates", [])
                if candidates:
                    return candidates[0]  # Return first match
                
                return None
        except Exception as e:
            logger.error(f"Error finding place: {e}")
            return None
    
    async def get_place_details(self, place_id: str) -> Optional[Dict[str, Any]]:
        """Fetch place details using the latest Places API (v1 Place Details).
        
        Returns raw API response or None if error.
        """
        if not self.api_key:
            logger.error("Cannot fetch place details: API key missing")
            return None
        
        # Places API (New) endpoint
        url = f"https://places.googleapis.com/v1/places/{place_id}"
        
        # Request only the fields we need to keep costs down
        field_mask = ",".join([
            "displayName",
            "formattedAddress",
            "location",
            "internationalPhoneNumber",
            "nationalPhoneNumber",
            "websiteUri",
            "regularOpeningHours",
            "currentOpeningHours",
            "editorialSummary",
            "types",
            "businessStatus",
            "rating",
            "userRatingCount",
            "photos",  # include photos for downstream image fetching
            "reviews",  # include reviews for reviews fetcher
            "timeZone"  # include timezone for best time calculations
        ])
        
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": field_mask
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                return data
        except Exception as e:
            logger.error(f"Error fetching place details: {e}")
            return None
    
    async def get_place_photo_urls(
        self,
        place_id: str,
        max_width_px: int = 800,
        limit: int = 5
    ) -> List[str]:
        """Get direct photo URLs for a place using Places API (New).
        
        Uses the v1 Photos endpoint with skipHttpRedirect=true to return photoUri.
        """
        if not self.api_key:
            logger.error("Cannot fetch place photos: API key missing")
            return []
        
        details = await self.get_place_details(place_id)
        if not details:
            return []
        
        photos = details.get("photos", [])
        if not photos:
            return []
        
        photo_urls: List[str] = []
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                for photo in photos[:limit]:
                    photo_name = photo.get("name")
                    if not photo_name:
                        continue
                    
                    url = f"https://places.googleapis.com/v1/{photo_name}/media"
                    params = {
                        "maxWidthPx": max_width_px,
                        "skipHttpRedirect": "true",
                        "key": self.api_key,
                    }
                    resp = await client.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    photo_uri = data.get("photoUri")
                    if photo_uri:
                        photo_urls.append(photo_uri)
        except Exception as e:
            logger.error(f"Error fetching place photos: {e}")
            return photo_urls  # return whatever we got so far
        
        return photo_urls
    
    def get_photo_url(self, photo_reference: str, max_width: int = 1600) -> Optional[str]:
        """Get photo URL from photo reference.
        
        Note: This returns the URL, not the actual image data.
        This is a synchronous method as it just constructs a URL string.
        """
        if not self.api_key:
            return None
        
        return f"{self.BASE_URL}/place/photo?maxwidth={max_width}&photo_reference={photo_reference}&key={self.api_key}"
    
    async def get_nearby_places(
        self,
        latitude: float,
        longitude: float,
        radius: int = 5000,
        place_type: Optional[str] = "tourist_attraction"
    ) -> Optional[List[Dict[str, Any]]]:
        """Search for nearby places.
        
        Returns list of places or None if error.
        """
        if not self.api_key:
            logger.error("Cannot fetch nearby places: API key missing")
            return None
        
        url = f"{self.BASE_URL}/place/nearbysearch/json"
        params = {
            "location": f"{latitude},{longitude}",
            "radius": radius,
            "type": place_type,
            "key": self.api_key
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") not in ["OK", "ZERO_RESULTS"]:
                    logger.error(f"Google Places API error: {data.get('status')}")
                    return None
                
                return data.get("results", [])
        except Exception as e:
            logger.error(f"Error fetching nearby places: {e}")
            return None

    
    async def nearby_search(
        self,
        latitude: float,
        longitude: float,
        radius: int = 5000,
        place_type: str = "tourist_attraction",
        max_results: int = 20
    ) -> List[Dict[str, Any]]:
        """Search for nearby places with distance calculation.
        
        Args:
            latitude: Center latitude
            longitude: Center longitude
            radius: Search radius in meters
            place_type: Type of place to search for
            max_results: Maximum number of results
        
        Returns:
            List of places with distance_km added
        """
        places = await self.get_nearby_places(
            latitude=latitude,
            longitude=longitude,
            radius=radius,
            place_type=place_type
        )
        
        if not places:
            return []
        
        # Add distance calculation
        import math
        
        for place in places:
            if "geometry" in place and "location" in place["geometry"]:
                place_lat = place["geometry"]["location"]["lat"]
                place_lng = place["geometry"]["location"]["lng"]

                # Haversine formula
                R = EARTH_RADIUS_KM
                lat1 = math.radians(latitude)
                lat2 = math.radians(place_lat)
                delta_lat = math.radians(place_lat - latitude)
                delta_lng = math.radians(place_lng - longitude)
                
                a = (
                    math.sin(delta_lat / 2) ** 2 +
                    math.cos(lat1) * math.cos(lat2) * math.sin(delta_lng / 2) ** 2
                )
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                distance_km = R * c
                
                place["distance_km"] = round(distance_km, 2)
        
        # Sort by distance
        places.sort(key=lambda p: p.get("distance_km", float('inf')))
        
        return places[:max_results]
    
    def get_photo_url(self, photo_reference: str, max_width: int = 400) -> str:
        """Get photo URL from photo reference (synchronous version)."""
        if not self.api_key:
            return None
        
        return f"{self.BASE_URL}/place/photo?maxwidth={max_width}&photo_reference={photo_reference}&key={self.api_key}"
