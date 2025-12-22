"""Attraction Metadata Fetcher using Google Places API and Gemini."""
import os
from typing import Optional, Dict, Any, List
import logging
from .google_places_client import GooglePlacesClient
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class MetadataFetcherImpl:
    """Fetches attraction metadata from Google Places API with Gemini enhancement."""
    
    def __init__(
        self,
        places_client: Optional[GooglePlacesClient] = None,
        gemini_client: Optional[GeminiClient] = None
    ):
        self.places_client = places_client or GooglePlacesClient()
        self.gemini_client = gemini_client or GeminiClient()
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str],
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch attraction metadata.
        
        Returns:
        - section: Visitor info section data
        - metadata: Metadata for DB storage
        - source: "google_places_api" or "gemini_fallback"
        """
        if not attraction_name or not city_name:
            logger.warning(f"Missing attraction_name or city_name for attraction {attraction_id}")
            return None
        
        # Try Google Places API first (v1 Place Details)
        if place_id:
            try:
                logger.info(f"Fetching metadata from Google Places for {attraction_name}")
                place_data = await self.places_client.get_place_details(place_id)
                
                if place_data:
                    # Process Google Places data
                    return await self._process_places_data(
                        attraction_name=attraction_name,
                        city_name=city_name,
                        place_data=place_data
                    )
            except Exception as e:
                logger.error(f"Error fetching from Google Places: {e}")
        
        # Fall back to Gemini
        logger.info(f"Using Gemini to generate metadata for {attraction_name}")
        try:
            return await self._generate_with_gemini(
                attraction_name=attraction_name,
                city_name=city_name
            )
        except Exception as e:
            logger.error(f"Failed to generate metadata: {e}")
            return None
    
    async def _process_places_data(
        self,
        attraction_name: str,
        city_name: str,
        place_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process Google Places data and enhance with Gemini."""
        
        # Extract contact info from new Places API fields
        contact_info = {
            "phone": None,
            "email": None,   # Gemini only
            "website": None
        }
        
        phone = place_data.get('internationalPhoneNumber') or place_data.get('nationalPhoneNumber')
        if phone:
            contact_info["phone"] = {
                "value": phone,
                "url": f"tel:{phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')}"
            }
        
        website = place_data.get('websiteUri')
        if website:
            contact_info["website"] = {
                "value": website.replace('https://', '').replace('http://', '').rstrip('/'),
                "url": website
            }
        
        # Extract opening hours from regularOpeningHours.periods
        opening_hours_data = []

        regular_hours = place_data.get('regularOpeningHours', {})
        periods = regular_hours.get('periods', [])

        if periods:
            # Map day numbers to names (Google uses 0=Sunday, 1=Monday, etc.)
            day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

            # Create a map of day -> hours
            day_hours = {}
            for period in periods:
                open_info = period.get('open', {})
                close_info = period.get('close', {})

                day_num = open_info.get('day')
                if day_num is not None:
                    open_hour = open_info.get('hour')
                    open_min = open_info.get('minute', 0)
                    close_hour = close_info.get('hour')
                    close_min = close_info.get('minute', 0)

                    if open_hour is not None and close_hour is not None:
                        day_hours[day_num] = {
                            'open_time': f"{open_hour:02d}:{open_min:02d}",
                            'close_time': f"{close_hour:02d}:{close_min:02d}"
                        }

            # Build opening hours for all 7 days (Monday first)
            for idx in range(7):
                # Convert to Google's day numbering (1=Monday, 0=Sunday)
                google_day = (idx + 1) % 7
                day_name = day_names[google_day]

                if google_day in day_hours:
                    opening_hours_data.append({
                        "day": day_name,
                        "open_time": day_hours[google_day]['open_time'],
                        "close_time": day_hours[google_day]['close_time'],
                        "is_closed": False
                    })
                else:
                    # Day not in periods = closed
                    opening_hours_data.append({
                        "day": day_name,
                        "open_time": None,
                        "close_time": None,
                        "is_closed": True
                    })
        
        # Use Gemini to generate additional metadata
        gemini_data = await self._generate_with_gemini(
            attraction_name=attraction_name,
            city_name=city_name,
            existing_data={
                "contact_info": contact_info,
                "opening_hours": opening_hours_data
            }
        )
        
        # Merge Google Places data with Gemini-generated data
        if gemini_data:
            metadata = gemini_data.get('metadata', {})
            
            # Merge Google Places contact info with Gemini data
            gemini_contact = metadata.get('contact_info', {})
            merged_contact = {
                "phone": contact_info.get("phone") or gemini_contact.get("phone"),
                "email": gemini_contact.get("email"),  # Email only from Gemini
                "website": contact_info.get("website") or gemini_contact.get("website")
            }
            metadata['contact_info'] = merged_contact
            
            # Use Google Places opening hours if available and valid
            # Otherwise use Gemini's opening hours
            if opening_hours_data:
                metadata['opening_hours'] = opening_hours_data
            else:
                # Use Gemini's opening hours (already in metadata from Gemini response)
                logger.info("Using Gemini-generated opening hours")
            
            return {
                "section": gemini_data.get('section', {}),
                "metadata": metadata,
                "source": "google_places_api"
            }
        
        return None

    async def _generate_with_gemini(
        self,
        attraction_name: str,
        city_name: str,
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Generate metadata using Gemini."""
        
        prompt = f"""Generate comprehensive visitor information and metadata for {attraction_name} in {city_name}.

Return ONLY a JSON object with this structure:

{{
  "contact_info": {{
    "phone": {{"value": "+XX X XX XX XX XX", "url": "tel:+XXXXXXXXXXX"}},
    "email": {{"value": "info@example.com", "url": "mailto:info@example.com"}},
    "website": {{"value": "www.example.com", "url": "https://www.example.com"}}
  }},
  "accessibility_info": "<1-2 sentences about wheelchair access, elevators, facilities for disabled visitors>",
  "best_season": "<Best season to visit: Spring/Summer/Fall/Winter or specific months>",
  "opening_hours": [
    {{"day": "Monday", "open_time": "09:00", "close_time": "18:00", "is_closed": false}},
    {{"day": "Tuesday", "open_time": "09:00", "close_time": "18:00", "is_closed": false}},
    ... (all 7 days)
  ],
  "short_description": "<3-line description of the attraction, its significance, and what makes it special>",
  "recommended_duration_minutes": <integer: typical visit duration in minutes>,
  "highlights": [
    "<highlight 1: key thing to see/do>",
    "<highlight 2: key thing to see/do>",
    "<highlight 3: key thing to see/do>",
    ... (5-8 highlights)
  ]
}}

Guidelines:
- Contact info: Provide phone, email, and website. Use real/realistic info if known, or set to null if unavailable
- Phone: Include country code, format: "+XX X XX XX XX XX"
- Email: Use official contact email if known, or null
- Website: Domain only in value (e.g., "www.example.com"), full URL in url field
- Accessibility: Be specific about wheelchair access, elevators, ramps, accessible restrooms
- Best season: Consider weather, crowds, and optimal visiting conditions
- Opening hours: Provide realistic hours for all 7 days (Monday-Sunday)
- Short description: 3 lines max, capture essence and significance
- Duration: Realistic time needed (e.g., 120 for 2 hours, 180 for 3 hours)
- Highlights: 5-8 specific things visitors should see or do

Return ONLY the JSON, no other text."""

        result = await self.gemini_client.generate_json(prompt)
        
        if not result:
            logger.error("Failed to generate metadata with Gemini")
            return None
        
        # If we have existing data from Google Places, merge it
        if existing_data:
            if existing_data.get('contact_info'):
                # Merge contact info (Google Places data takes precedence for phone/website)
                gemini_contact = result.get('contact_info', {})
                existing_contact = existing_data['contact_info']
                result['contact_info'] = {
                    "phone": existing_contact.get("phone") or gemini_contact.get("phone"),
                    "email": gemini_contact.get("email"),  # Email only from Gemini
                    "website": existing_contact.get("website") or gemini_contact.get("website")
                }
            if existing_data.get('opening_hours'):
                result['opening_hours'] = existing_data['opening_hours']
        
        return {
            "section": {
                "contact_info": result.get('contact_info', {}),
                "opening_hours": result.get('opening_hours', []),
                "accessibility_info": result.get('accessibility_info', ''),
                "best_season": result.get('best_season', '')
            },
            "metadata": result,
            "source": "gemini_fallback" if not existing_data else "google_places_api"
        }
