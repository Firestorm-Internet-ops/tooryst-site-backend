"""Nearby Attractions Fetcher using database and Google Places API."""
import os
import logging
from typing import Optional, Dict, Any, List

from app.config import settings
from app.constants import EARTH_RADIUS_KM
from .google_places_client import GooglePlacesClient
from ..persistence.db import SessionLocal
from ..persistence import models
from sqlalchemy import func

logger = logging.getLogger(__name__)


class NearbyAttractionsFetcherImpl:
    """Fetches nearby attractions from database first, then Google Places."""
    
    def __init__(self, places_client: Optional[GooglePlacesClient] = None):
        self.places_client = places_client or GooglePlacesClient()
        base_count = int(os.getenv("NEARBY_ATTRACTIONS_COUNT", "10"))
        multiplier = settings.NEARBY_ATTRACTIONS_MULTIPLIER
        self.target_count = base_count * multiplier
    
    async def fetch(
        self,
        attraction_id: int,
        attraction_name: str,
        city_name: str,
        latitude: float,
        longitude: float,
        place_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch nearby attractions.
        
        Strategy:
        1. Get nearby attractions from our database (same city, within radius)
        2. Fill remaining slots with Google Places nearby search
        3. Calculate distances and create links
        
        Args:
            attraction_id: ID of the current attraction
            attraction_name: Name of the current attraction
            city_name: City name
            latitude: Latitude of the attraction
            longitude: Longitude of the attraction
            place_id: Google Place ID (optional)
        
        Returns:
            Dictionary with:
            - section: Section data for API response
            - nearby: List of nearby attractions for DB storage
        """
        logger.info(f"Fetching nearby attractions for {attraction_name}")
        
        all_nearby = []
        seen_place_ids = set()
        
        # Strategy 1: Get from our database
        db_nearby = await self._get_from_database(
            attraction_id=attraction_id,
            city_name=city_name,
            latitude=latitude,
            longitude=longitude,
            max_results=self.target_count
        )
        
        for nearby in db_nearby:
            if nearby.get("place_id"):
                seen_place_ids.add(nearby["place_id"])
            all_nearby.append(nearby)
        
        logger.info(f"Found {len(all_nearby)} nearby attractions in database")
        
        # Strategy 2: Fill remaining with Google Places
        if len(all_nearby) < self.target_count:
            remaining = self.target_count - len(all_nearby)
            logger.info(f"Fetching {remaining} more from Google Places")
            
            google_nearby = await self._get_from_google(
                latitude=latitude,
                longitude=longitude,
                max_results=remaining * 2,  # Get more to filter
                exclude_place_ids=seen_place_ids,
                exclude_place_id=place_id  # Exclude current attraction
            )
            
            for nearby in google_nearby:
                if len(all_nearby) >= self.target_count:
                    break
                
                nearby_place_id = nearby.get("place_id")
                
                # Skip if it's the current attraction or already seen
                if nearby_place_id == place_id:
                    continue
                
                if nearby_place_id and nearby_place_id not in seen_place_ids:
                    seen_place_ids.add(nearby_place_id)
                    all_nearby.append(nearby)
        
        if not all_nearby:
            logger.warning(f"No nearby attractions found for {attraction_name}")
            return None
        
        logger.info(f"Total nearby attractions: {len(all_nearby)}")
        
        # Format for section (filter out items where both image and link are null)
        section_items = []
        for nearby in all_nearby:
            image_url = nearby.get("image_url")
            link = nearby.get("link")
            
            # Skip if both image and link are null
            if image_url is None and link is None:
                continue
            
            section_items.append({
                "id": nearby.get("id"),
                "slug": nearby.get("slug"),
                "name": nearby.get("name"),
                "distance_text": nearby.get("distance_text"),
                "distance_km": nearby.get("distance_km"),
                "rating": nearby.get("rating"),
                "user_ratings_total": nearby.get("user_ratings_total"),
                "review_count": nearby.get("review_count"),
                "image_url": image_url,
                "link": link,
                "vicinity": nearby.get("vicinity"),
                "audience_type": nearby.get("audience_type"),
                "audience_text": nearby.get("audience_text")
            })
        
        return {
            "section": {
                "items": section_items
            },
            "nearby": all_nearby
        }
    
    async def _get_from_database(
        self,
        attraction_id: int,
        city_name: str,
        latitude: float,
        longitude: float,
        max_results: int
    ) -> List[Dict[str, Any]]:
        """Get nearby attractions from our database."""
        session = SessionLocal()
        try:
            
            # Get attractions in the same city, excluding current one
            # Calculate distance using Haversine formula
            # Note: distance_km in km; filter within 10km in Python (SQL HAVING doesn't work with computed columns)
            # Eagerly load hero_image to avoid N+1 query - get any available image, not just position 1
            nearby_query = (
                session.query(
                    models.Attraction,
                    models.City,
                    models.HeroImage.url.label('hero_image_url'),
                    (
                        EARTH_RADIUS_KM * func.acos(
                            func.cos(func.radians(latitude)) *
                            func.cos(func.radians(models.Attraction.latitude)) *
                            func.cos(func.radians(models.Attraction.longitude) - func.radians(longitude)) +
                            func.sin(func.radians(latitude)) *
                            func.sin(func.radians(models.Attraction.latitude))
                        )
                    ).label('distance_km')
                )
                .join(models.City, models.Attraction.city_id == models.City.id)
                .outerjoin(
                    models.HeroImage,
                    (models.Attraction.id == models.HeroImage.attraction_id)
                )
                .filter(models.City.name == city_name)
                .filter(models.Attraction.id != attraction_id)
                .filter(models.Attraction.latitude.isnot(None))
                .filter(models.Attraction.longitude.isnot(None))
                .order_by('distance_km', models.HeroImage.position)
                .limit(max_results * 3)  # Get more results to filter in Python
            )

            results = nearby_query.all()

            # Filter to attractions within configured max distance
            results = [r for r in results if r.distance_km and r.distance_km <= settings.NEARBY_MAX_DISTANCE_KM][:max_results]
            
            # Group results by attraction to get first available image
            attraction_map = {}
            for attraction, city, hero_image_url, distance_km in results:
                if attraction.id not in attraction_map:
                    attraction_map[attraction.id] = (attraction, city, hero_image_url, distance_km)
                elif hero_image_url and not attraction_map[attraction.id][2]:
                    # Update if we found an image and the current entry doesn't have one
                    attraction_map[attraction.id] = (attraction, city, hero_image_url, distance_km)
            
            nearby_list = []
            for attraction, city, hero_image_url, distance_km in attraction_map.values():
                # Validate distance_km - cap at max to fit DECIMAL(6,3) constraint
                if distance_km is not None:
                    if distance_km > settings.DISTANCE_MAX_KM:
                        logger.warning(f"Distance {distance_km}km for {attraction.name} exceeds max, capping to {settings.DISTANCE_MAX_KM}km")
                        distance_km = settings.DISTANCE_MAX_KM
                    if distance_km < 0:
                        logger.warning(f"Invalid negative distance {distance_km}km for {attraction.name}, setting to 0")
                        distance_km = 0.0
                
                # Use hero_image_url from the query (already loaded via outerjoin)
                image_url = hero_image_url

                # Calculate walking time based on configured walking speed
                walking_time_minutes = int((distance_km / settings.WALKING_SPEED_KMH) * 60) if distance_km else None
                
                # Format distance text (ensure it's never null)
                if distance_km is not None:
                    if distance_km < 1:
                        distance_text = f"{int(distance_km * 1000)}m"
                    else:
                        distance_text = f"{distance_km:.1f}km"
                else:
                    distance_text = "Nearby"
                    distance_km = 0.0
                
                # Generate link - only if slug exists
                link = f"/attractions/{attraction.slug}" if attraction.slug else None
                
                # Skip if both image and link are null
                if image_url is None and link is None:
                    logger.warning(f"Skipping nearby attraction {attraction.name} (id: {attraction.id}) - both image_url and link are null (slug: {attraction.slug})")
                    continue
                
                nearby_list.append({
                    "id": attraction.id,
                    "nearby_attraction_id": attraction.id,  # Set to attraction.id for DB attractions
                    "slug": attraction.slug,
                    "name": attraction.name,
                    "place_id": attraction.place_id,
                    "rating": float(attraction.rating) if attraction.rating else None,
                    "user_ratings_total": attraction.review_count,
                    "review_count": attraction.review_count,
                    "image_url": image_url,
                    "link": link,
                    "vicinity": f"{city.name}",
                    "distance_text": distance_text,
                    "distance_km": min(float(distance_km), 999.999) if distance_km is not None else None,
                    "walking_time_minutes": walking_time_minutes,
                    "audience_type": None,
                    "audience_text": None
                })

            return nearby_list

        except Exception as e:
            logger.error(f"Error getting nearby attractions from database: {e}")
            return []
        finally:
            session.close()
    
    async def _get_from_google(
        self,
        latitude: float,
        longitude: float,
        max_results: int,
        exclude_place_ids: set,
        exclude_place_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get nearby attractions from Google Places."""
        try:
            # Search for tourist attractions nearby
            # Use 10km radius and later filter to <=10km
            places = await self.places_client.nearby_search(
                latitude=latitude,
                longitude=longitude,
                radius=10000,  # 10km radius
                place_type="tourist_attraction",
                max_results=max_results
            )
            
            if not places:
                return []
            
            nearby_list = []
            for place in places:
                place_id = place.get("place_id")
                
                # Skip if it's the current attraction or already seen
                if place_id == exclude_place_id or place_id in exclude_place_ids:
                    continue
                
                # Get photo URL
                photo_reference = None
                if place.get("photos") and len(place["photos"]) > 0:
                    photo_reference = place["photos"][0].get("photo_reference")
                
                image_url = None
                if photo_reference:
                    image_url = self.places_client.get_photo_url(
                        photo_reference=photo_reference,
                        max_width=400
                    )
                
                # Calculate distance
                distance_km = place.get("distance_km")
                if distance_km is not None:
                    # Enforce 10km cap for returned items
                    if distance_km > 10:
                        continue
                    # Validate and cap distance_km to fit DECIMAL(6,3) constraint (max 999.999)
                    if distance_km > 999.999:
                        logger.warning(f"Distance {distance_km}km exceeds max, capping to 999.999km")
                        distance_km = 999.999
                    # Ensure non-negative
                    if distance_km < 0:
                        logger.warning(f"Invalid negative distance {distance_km}km, setting to 0")
                        distance_km = 0.0
                    
                    if distance_km < 1:
                        distance_text = f"{int(distance_km * 1000)}m"
                    else:
                        distance_text = f"{distance_km:.1f}km"
                    
                    walking_time_minutes = int((distance_km / 5.0) * 60)
                else:
                    # Fallback if distance calculation failed
                    distance_text = "Nearby"
                    distance_km = 0.0
                    walking_time_minutes = None
                
                # Create Google Maps link
                maps_link = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

                nearby_list.append({
                    "id": None,  # Not in our database
                    "nearby_attraction_id": None,  # Null for Google attractions
                    "slug": None,
                    "name": place.get("name"),
                    "place_id": place_id,
                    "rating": place.get("rating"),
                    "user_ratings_total": place.get("user_ratings_total"),
                    "review_count": place.get("user_ratings_total"),
                    "image_url": image_url,
                    "link": maps_link,  # Link to Google Maps
                    "vicinity": place.get("vicinity"),
                    "distance_text": distance_text,
                    "distance_km": distance_km,
                    "walking_time_minutes": walking_time_minutes,
                    "audience_type": None,
                    "audience_text": None
                })
            
            return nearby_list
            
        except Exception as e:
            logger.error(f"Error getting nearby attractions from Google: {e}")
            return []
