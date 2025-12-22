"""Audience Profile Fetcher using Gemini."""
import logging
from typing import Optional, Dict, Any, List
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class AudienceFetcherImpl:
    """Fetches audience profiles using Gemini AI."""
    
    def __init__(self, gemini_client: Optional[GeminiClient] = None):
        self.gemini_client = gemini_client or GeminiClient()
    
    async def fetch(
        self,
        attraction_id: int,
        attraction_name: str,
        city_name: str,
        attraction_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch audience profiles for an attraction.
        
        Args:
            attraction_id: ID of the attraction
            attraction_name: Name of the attraction
            city_name: City name
            attraction_type: Optional type hint (e.g., "museum", "park", "monument")
        
        Returns:
            Dictionary with:
            - section: Section data for API response
            - profiles: List of profiles for DB storage
        """
        logger.info(f"Generating audience profiles for {attraction_name}")
        
        type_hint = f" (a {attraction_type})" if attraction_type else ""
        
        prompt = f"""Generate audience profiles for {attraction_name}{type_hint} in {city_name}.

Identify 3-5 different audience types who would enjoy visiting this attraction.

Return ONLY a JSON object with this structure:

{{
  "profiles": [
    {{
      "audience_type": "families",
      "description": "Great for families with children of all ages",
      "emoji": "👨‍👩‍👧‍👦"
    }},
    {{
      "audience_type": "couples",
      "description": "Romantic spot perfect for date nights",
      "emoji": "💑"
    }},
    {{
      "audience_type": "solo_travelers",
      "description": "Ideal for solo exploration and reflection",
      "emoji": "🚶"
    }}
  ]
}}

Guidelines:
- Provide 3-5 audience profiles
- audience_type: Use lowercase with underscores (e.g., "families", "couples", "solo_travelers", "history_buffs", "photographers", "adventure_seekers", "culture_enthusiasts", "nature_lovers", "art_lovers", "architecture_fans")
- description: 6-8 lines explaining why this audience would enjoy it, what makes it special for them, and what they can expect
- emoji: Single relevant emoji that represents the audience type
- Be specific to what the attraction offers
- Consider different interests, age groups, and travel styles

Common audience types to consider:
- families (👨‍👩‍👧‍👦)
- couples (💑)
- solo_travelers (🚶)
- history_buffs (📚)
- photographers (📸)
- adventure_seekers (🏔️)
- culture_enthusiasts (🎭)
- nature_lovers (🌿)
- art_lovers (🎨)
- architecture_fans (🏛️)
- foodies (🍽️)
- students (🎓)
- seniors (👴)

Return ONLY the JSON, no other text."""

        result = await self.gemini_client.generate_json(prompt)
        
        if not result or 'profiles' not in result:
            logger.error(f"Failed to generate audience profiles for {attraction_name}")
            return None
        
        profiles = result['profiles']
        
        # Validate profiles
        if not isinstance(profiles, list) or len(profiles) == 0:
            logger.error(f"Invalid profiles format for {attraction_name}")
            return None
        
        # Ensure each profile has required fields
        valid_profiles = []
        for profile in profiles:
            if all(k in profile for k in ['audience_type', 'description', 'emoji']):
                valid_profiles.append(profile)
            else:
                logger.warning(f"Skipping invalid profile: {profile}")
        
        if not valid_profiles:
            logger.error(f"No valid profiles generated for {attraction_name}")
            return None
        
        logger.info(f"Generated {len(valid_profiles)} audience profiles for {attraction_name}")
        
        return {
            "section": {
                "items": valid_profiles
            },
            "profiles": valid_profiles
        }
