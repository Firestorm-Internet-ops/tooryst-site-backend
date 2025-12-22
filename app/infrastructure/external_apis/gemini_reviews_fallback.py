"""Gemini-based fallback for Reviews when Google Places API fails."""
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import random
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class GeminiReviewsFallback:
    """Generate reviews data using Gemini AI when Google Places API is unavailable."""
    
    def __init__(self, client: Optional[GeminiClient] = None):
        self.client = client or GeminiClient()
    
    async def generate_reviews(
        self,
        attraction_name: str,
        city_name: str,
        max_reviews: int = 5
    ) -> Optional[Dict[str, Any]]:
        """Generate reviews using Gemini AI.
        
        Args:
            attraction_name: Name of the attraction
            city_name: City name
            max_reviews: Number of reviews to generate
        
        Returns:
            Dict with card, section, and reviews data
        """
        logger.warning(f"Using Gemini fallback for reviews: {attraction_name}")
        
        prompt = f"""Generate {max_reviews} realistic visitor reviews for {attraction_name} in {city_name}.

Return ONLY a JSON object with this structure:

{{
  "overall_rating": <float between 4.0-5.0>,
  "total_reviews": <integer between 10000-200000>,
  "summary": "<2-3 line summary capturing what visitors love most, 2-3 sentences>",
  "reviews": [
    {{
      "author_name": "<realistic name>",
      "author_url": "<realistic URL like https://plus.google.com/... or similar>",
      "author_photo_url": "<realistic photo URL like https://lh3.googleusercontent.com/...>",
      "rating": <integer 1-5>,
      "text": "<realistic review text, 1-3 sentences>",
      "relative_time": "<e.g., '2 weeks ago', '1 month ago', '3 days ago'>"
    }}
  ]
}}

Guidelines:
- Make reviews realistic and varied (mix of ratings, mostly 4-5 stars)
- Include specific details about the attraction
- Vary review lengths and perspectives
- Use realistic author names from different cultures
- Include realistic author URLs and photo URLs
- Make relative times varied (days, weeks, months ago)
- Summary should be 2-3 sentences highlighting the most common positive themes

Return ONLY the JSON, no other text."""

        result = await self.client.generate_json(prompt)
        
        if not result:
            logger.error("Failed to generate reviews with Gemini")
            return None
        
        # Extract data
        overall_rating = result.get("overall_rating", 4.5)
        total_reviews = result.get("total_reviews", 50000)
        summary = result.get("summary", f"Visitors love {attraction_name}")
        reviews_data = result.get("reviews", [])
        
        # Process reviews for DB storage
        processed_reviews = []
        for review in reviews_data[:max_reviews]:
            # Generate approximate datetime based on relative time
            relative_time = review.get("relative_time", "1 month ago")
            review_time = self._approximate_datetime_from_relative(relative_time)
            
            processed_reviews.append({
                "author_name": review.get("author_name", "Anonymous"),
                "author_url": review.get("author_url"),
                "author_photo_url": review.get("author_photo_url"),
                "rating": review.get("rating", 5),
                "text": review.get("text", "Great experience!"),
                "time": review_time,
                "relative_time": relative_time,
                "source": "Gemini"
            })
        
        return {
            "card": {
                "overall_rating": overall_rating,
                "rating_scale_max": 5,
                "total_reviews": total_reviews,
                "summary": summary
            },
            "section": {
                "overall_rating": overall_rating,
                "rating_scale_max": 5,
                "total_reviews": total_reviews,
                "summary": summary,
                "items": [
                    {
                        "author_name": r["author_name"],
                        "author_url": r["author_url"],
                        "author_photo_url": r["author_photo_url"],
                        "rating": r["rating"],
                        "text": r["text"],
                        "time": r["relative_time"],
                        "source": r["source"]
                    }
                    for r in processed_reviews
                ]
            },
            "reviews": processed_reviews,
            "source": "gemini_fallback"
        }
    
    def _approximate_datetime_from_relative(self, relative_time: str) -> Optional[datetime]:
        """Convert relative time string to approximate datetime."""
        try:
            import re
            now = datetime.now()
            relative_lower = relative_time.lower()

            if "day" in relative_lower:
                # Extract number of days
                match = re.search(r'(\d+)\s*day', relative_lower)
                if match:
                    days = int(match.group(1))
                    return now - timedelta(days=days)

            elif "week" in relative_lower:
                match = re.search(r'(\d+)\s*week', relative_lower)
                if match:
                    weeks = int(match.group(1))
                    return now - timedelta(weeks=weeks)

            elif "month" in relative_lower:
                match = re.search(r'(\d+)\s*month', relative_lower)
                if match:
                    months = int(match.group(1))
                    return now - timedelta(days=months * 30)

            elif "year" in relative_lower:
                match = re.search(r'(\d+)\s*year', relative_lower)
                if match:
                    years = int(match.group(1))
                    return now - timedelta(days=years * 365)

            # Default to 1 month ago
            return now - timedelta(days=30)

        except Exception as e:
            logger.warning(f"Failed to parse relative time '{relative_time}': {e}")
            return datetime.now() - timedelta(days=30)
