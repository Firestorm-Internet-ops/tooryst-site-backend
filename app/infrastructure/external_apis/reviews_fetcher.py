"""Reviews Fetcher implementation using Google Places API."""
import os
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
from .google_places_client import GooglePlacesClient
from .gemini_reviews_fallback import GeminiReviewsFallback

logger = logging.getLogger(__name__)


class ReviewsFetcherImpl:
    """Fetches reviews from Google Places API with Gemini fallback."""
    
    def __init__(
        self,
        client: Optional[GooglePlacesClient] = None,
        fallback: Optional[GeminiReviewsFallback] = None
    ):
        self.client = client or GooglePlacesClient()
        self.fallback = fallback or GeminiReviewsFallback()
        self.max_reviews = 5  # Get 5 reviews
    
    def _parse_relative_time(self, relative_time: str) -> Optional[datetime]:
        """Parse relative time string to datetime (approximate)."""
        # This is approximate - Google returns relative time like "2 weeks ago"
        # For now, we'll return None and store the relative time string
        # In production, you might want to calculate approximate dates
        return None
    
    async def _generate_summary(
        self,
        attraction_name: str,
        reviews: List[Dict[str, Any]],
        overall_rating: float
    ) -> str:
        """Generate a 2-3 line summary of reviews using Gemini."""
        try:
            # Extract review texts
            review_texts = [r.get('text', '') for r in reviews if r.get('text')]
            
            if not review_texts:
                return f"Visitors rate {attraction_name} {overall_rating}/5 stars."
            
            # Create prompt for Gemini
            prompt = f"""Based on these visitor reviews for {attraction_name}, write a 2-3 line summary that captures what visitors love most.

Reviews:
{chr(10).join(f'- "{text}"' for text in review_texts[:5])}

Write a natural, engaging 2-3 line summary (2-3 sentences max). Focus on the most common positive themes.

Return ONLY the summary text, no quotes or extra formatting."""

            summary = await self.fallback.client.generate_text(prompt)
            
            if summary and len(summary.strip()) > 0:
                return summary.strip()
            else:
                return f"Visitors rate {attraction_name} {overall_rating}/5 stars based on their experiences."
        
        except Exception as e:
            logger.warning(f"Failed to generate summary with Gemini: {e}")
            return f"Visitors rate {attraction_name} {overall_rating}/5 stars based on their experiences."
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str],
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch reviews for an attraction.
        
        Returns:
        - card: Overall rating data for the card view
        - section: Reviews section data with individual reviews
        - reviews: List of reviews for DB storage
        - source: "google_places_api" or "gemini_fallback"
        """
        # Try primary API first
        if place_id:
            try:
                # Fetch place details to get reviews
                place_data = await self.client.get_place_details(place_id)
                if place_data:
                    reviews = place_data.get('reviews', [])
                    logger.info(f"Google Places API response for place_id {place_id}: reviews field type={type(reviews).__name__}, length={len(reviews) if isinstance(reviews, list) else 'N/A'}")
                    if reviews and isinstance(reviews, list) and len(reviews) > 0:
                        # Successfully got reviews from API
                        logger.info(f"Found {len(reviews)} reviews from Google Places API")
                        return await self._process_api_reviews(place_data, reviews)
                    else:
                        logger.warning(f"No reviews found in API response for place_id {place_id} (reviews field: {type(reviews).__name__}, value: {reviews})")
            except Exception as e:
                logger.error(f"Error fetching from Google Places API: {e}")
        else:
            logger.warning(f"No place_id provided for attraction {attraction_id}")
        
        # Fall back to Gemini
        if attraction_name and city_name:
            logger.info(f"Falling back to Gemini for reviews: {attraction_name}")
            try:
                return await self.fallback.generate_reviews(
                    attraction_name=attraction_name,
                    city_name=city_name,
                    max_reviews=self.max_reviews
                )
            except Exception as e:
                logger.error(f"Gemini fallback failed: {e}")
        
        return None
    
    async def _process_api_reviews(
        self,
        place_data: Dict[str, Any],
        reviews: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Process reviews from Google Places API (New) v1."""
        # Google Places API (New) uses displayName instead of name
        attraction_name = place_data.get("displayName") or place_data.get("name", "Attraction")
        
        # Get overall rating data
        overall_rating = place_data.get("rating", 0)
        # Google Places API (New) uses userRatingCount instead of user_ratings_total
        total_reviews = place_data.get("userRatingCount") or place_data.get("user_ratings_total", 0)
        
        # Process individual reviews (limit to max_reviews)
        processed_reviews = []
        for idx, review in enumerate(reviews[:self.max_reviews]):
            # Google Places API (New) v1 structure:
            # - authorAttribution.displayName (not author_name)
            # - authorAttribution.uri (not author_url)
            # - authorAttribution.photoUri (camelCase, not photoURI or profile_photo_url)
            # - text.text (text is an object with text and languageCode fields)
            # - rating (same)
            # - publishTime (ISO 8601 timestamp, not Unix timestamp)
            # - relativePublishTimeDescription (not relative_time_description)
            author_attribution = review.get("authorAttribution", {})
            author_name = author_attribution.get("displayName") or review.get("author_name", "Anonymous")
            author_url = author_attribution.get("uri") or review.get("author_url")
            # Note: API uses "photoUri" (camelCase), not "photoURI"
            author_photo_url = author_attribution.get("photoUri") or author_attribution.get("photoURI") or review.get("profile_photo_url")
            rating = review.get("rating", 0)
            
            # Text is an object with 'text' and 'languageCode' fields in new API
            text_obj = review.get("text", {})
            if isinstance(text_obj, dict):
                text = text_obj.get("text", "")
            else:
                # Fallback for old API format (plain string)
                text = text_obj if isinstance(text_obj, str) else ""
            
            # Try to get timestamp - Google Places API (New) uses publishTime (ISO 8601)
            time_value = review.get("publishTime") or review.get("time")
            review_time = None
            # New API uses relativePublishTimeDescription, old API uses relative_time_description
            relative_time = review.get("relativePublishTimeDescription") or review.get("relative_time_description", "")
            
            if time_value:
                try:
                    # Try ISO 8601 format first (new API)
                    if isinstance(time_value, str):
                        # Handle ISO 8601 with timezone
                        time_str = time_value.replace('Z', '+00:00')
                        try:
                            review_time = datetime.fromisoformat(time_str)
                        except ValueError:
                            # Try without timezone suffix
                            review_time = datetime.fromisoformat(time_str.split('+')[0].split('Z')[0])
                    # Fallback to Unix timestamp (old API)
                    elif isinstance(time_value, (int, float)):
                        review_time = datetime.fromtimestamp(time_value)
                    
                    # Calculate relative time if we have a valid timestamp
                    if review_time:
                        # Convert to UTC naive datetime for comparison
                        from datetime import timezone
                        if review_time.tzinfo:
                            review_time = review_time.astimezone(timezone.utc).replace(tzinfo=None)
                        now = datetime.utcnow()
                        delta = now - review_time
                        
                        if delta.days > 365:
                            relative_time = f"{delta.days // 365} year{'s' if delta.days // 365 > 1 else ''} ago"
                        elif delta.days > 30:
                            relative_time = f"{delta.days // 30} month{'s' if delta.days // 30 > 1 else ''} ago"
                        elif delta.days > 0:
                            relative_time = f"{delta.days} day{'s' if delta.days > 1 else ''} ago"
                        elif delta.seconds > 3600:
                            relative_time = f"{delta.seconds // 3600} hour{'s' if delta.seconds // 3600 > 1 else ''} ago"
                        else:
                            relative_time = "Just now"
                except Exception as e:
                    logger.debug(f"Failed to parse review time: {e}")
                    # Keep fallback relative_time_description if available
            
            processed_reviews.append({
                "author_name": author_name,
                "author_url": author_url,
                "author_photo_url": author_photo_url,
                "rating": rating,
                "text": text,
                "time": review_time,
                "relative_time": relative_time,
                "source": "Google"
            })
        
        # Generate summary using Gemini
        summary = await self._generate_summary(attraction_name, processed_reviews, overall_rating)
        
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
            "reviews": processed_reviews,  # For DB storage
            "source": "google_places_api"
        }
