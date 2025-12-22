"""YouTube API client for fetching Shorts videos."""
import os
import logging
from typing import Optional, List, Dict, Any
import httpx

from app.core.notifications import notification_manager, AlertType, AlertSeverity
from app.core.quota_manager import quota_manager

logger = logging.getLogger(__name__)


class YouTubeClient:
    """Client for YouTube Data API v3."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        self.base_url = "https://www.googleapis.com/youtube/v3"
        
        if not self.api_key:
            logger.warning("YouTube API key not set")
    
    async def search_shorts(
        self,
        query: str,
        max_results: int = 10,
        region_code: str = "US"
    ) -> List[Dict[str, Any]]:
        """Search for YouTube Shorts videos.
        
        Args:
            query: Search query
            max_results: Maximum number of results (1-50)
            region_code: Region code for localized results
        
        Returns:
            List of video dictionaries with id, title, thumbnail, etc.
        """
        if not self.api_key:
            logger.error("YouTube API key not configured")
            return []
        
        # Check if quota is exceeded - if so, skip API call
        if quota_manager.is_quota_exceeded("youtube"):
            logger.warning(f"⏭️  Skipping YouTube API call for '{query}' - quota exceeded")
            return []
        
        # Check Redis cache first (permanent cache for YouTube)
        from app.infrastructure.external_apis.cache_client import get_cache
        cache = get_cache()
        
        cached_result = await cache.get(
            'youtube_search',
            query=query,
            max_results=max_results,
            region_code=region_code
        )
        
        if cached_result:
            logger.info(f"✓ YouTube cache HIT for: {query}")
            return cached_result
        
        logger.info(f"⚠ YouTube cache MISS for: {query} - using API quota")
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Search for videos
                search_params = {
                    "part": "snippet",
                    "q": query,
                    "type": "video",
                    "videoDuration": "short",  # Videos less than 4 minutes
                    "maxResults": min(max_results, 50),
                    "regionCode": region_code,
                    "relevanceLanguage": "en",
                    "key": self.api_key
                }
                
                search_response = await client.get(
                    f"{self.base_url}/search",
                    params=search_params
                )
                search_response.raise_for_status()
                search_data = search_response.json()
                
                if "items" not in search_data or len(search_data["items"]) == 0:
                    logger.info(f"No videos found for query: {query}")
                    return []
                
                # Extract video IDs
                video_ids = [item["id"]["videoId"] for item in search_data["items"] if "videoId" in item["id"]]
                
                if not video_ids:
                    return []
                
                # Get video details (duration, etc.)
                videos_params = {
                    "part": "snippet,contentDetails,statistics,status",
                    "id": ",".join(video_ids),
                    "key": self.api_key
                }
                
                videos_response = await client.get(
                    f"{self.base_url}/videos",
                    params=videos_params
                )
                videos_response.raise_for_status()
                videos_data = videos_response.json()
                
                # Process videos
                videos = []
                for item in videos_data.get("items", []):
                    video_id = item["id"]
                    snippet = item.get("snippet", {})
                    status = item.get("status", {})
                    content_details = item.get("contentDetails", {})
                    statistics = item.get("statistics", {})
                    region_restriction = status.get("regionRestriction", {})
                    
                    # Parse duration (ISO 8601 format: PT1M30S)
                    duration_str = content_details.get("duration", "PT0S")
                    duration_seconds = self._parse_duration(duration_str)
                    
                    # Only include shorts (< 60 seconds)
                    if duration_seconds > 60:
                        continue

                    # Respect embeddable flag and regional restrictions
                    embeddable = status.get("embeddable", True)
                    if embeddable is False:
                        continue

                    # If regionRestriction allows/blocks, honor current region_code
                    blocked_regions = set(region_restriction.get("blocked", []) or [])
                    allowed_regions = set(region_restriction.get("allowed", []) or [])
                    region = (region_code or "US").upper()
                    if blocked_regions and region in blocked_regions:
                        continue
                    if allowed_regions and region not in allowed_regions:
                        continue
                    
                    # Get best thumbnail
                    thumbnails = snippet.get("thumbnails", {})
                    thumbnail_url = (
                        thumbnails.get("maxres", {}).get("url") or
                        thumbnails.get("high", {}).get("url") or
                        thumbnails.get("medium", {}).get("url") or
                        thumbnails.get("default", {}).get("url")
                    )
                    
                    videos.append({
                        "video_id": video_id,
                        "title": snippet.get("title", ""),
                        "description": snippet.get("description", ""),
                        "thumbnail_url": thumbnail_url,
                        "embed_url": f"https://www.youtube.com/embed/{video_id}",
                        "watch_url": f"https://www.youtube.com/watch?v={video_id}",
                        "duration_seconds": duration_seconds,
                        "view_count": int(statistics.get("viewCount", 0)),
                        "like_count": int(statistics.get("likeCount", 0)),
                        "channel_title": snippet.get("channelTitle", ""),
                        "published_at": snippet.get("publishedAt", ""),
                        "embeddable": embeddable
                    })
                
                logger.info(f"Found {len(videos)} YouTube Shorts for query: {query}")
                
                # Cache result PERMANENTLY (videos don't change)
                # Use 1 year TTL (effectively permanent)
                await cache.set(
                    videos,
                    ttl_seconds=365 * 24 * 60 * 60,  # 1 year
                    prefix='youtube_search',
                    query=query,
                    max_results=max_results,
                    region_code=region_code
                )
                logger.info(f"✓ Cached YouTube results for: {query}")
                
                return videos
                
        except httpx.HTTPStatusError as e:
            logger.error(f"YouTube API HTTP error: {e.response.status_code} - {e.response.text}")
            
            # Check if this is a quota exceeded error (403 with quota message)
            if e.response.status_code == 403:
                response_text = e.response.text.lower()
                is_quota_error = "quota" in response_text or "exceeded" in response_text
                
                if is_quota_error:
                    # Mark quota as exceeded to prevent further API calls
                    quota_manager.mark_quota_exceeded("youtube")
                    
                    # Send notification (only once when quota is first exceeded)
                    notification_manager.send_alert(
                        alert_type=AlertType.QUOTA_EXCEEDED,
                        severity=AlertSeverity.CRITICAL,
                        title="YouTube API Quota Exceeded",
                        message=f"YouTube API quota exceeded while searching for: {query}\n\nAll YouTube API calls will be skipped until quota resets (tomorrow at midnight PT).\n\nResponse: {e.response.text}",
                        metadata={
                            "query": query,
                            "status_code": e.response.status_code,
                            "max_results": max_results,
                            "region_code": region_code,
                            "quota_status": quota_manager.get_quota_status("youtube")
                        }
                    )
                else:
                    # Other 403 error (not quota)
                    notification_manager.send_alert(
                        alert_type=AlertType.API_ERROR,
                        severity=AlertSeverity.ERROR,
                        title="YouTube API Permission Error",
                        message=f"YouTube API permission error while searching for: {query}\n\nStatus: {e.response.status_code}\nResponse: {e.response.text}",
                        metadata={
                            "query": query,
                            "status_code": e.response.status_code,
                            "api": "YouTube Data API v3"
                        }
                    )
            # Send notification for other API errors
            else:
                notification_manager.send_alert(
                    alert_type=AlertType.API_ERROR,
                    severity=AlertSeverity.ERROR,
                    title="YouTube API Error",
                    message=f"YouTube API error while searching for: {query}\n\nStatus: {e.response.status_code}\nResponse: {e.response.text}",
                    metadata={
                        "query": query,
                        "status_code": e.response.status_code,
                        "api": "YouTube Data API v3"
                    }
                )
            
            return []
        except Exception as e:
            logger.error(f"Error searching YouTube: {e}")
            
            # Send notification for unexpected errors
            notification_manager.send_alert(
                alert_type=AlertType.API_ERROR,
                severity=AlertSeverity.ERROR,
                title="YouTube API Unexpected Error",
                message=f"Unexpected error while searching YouTube for: {query}\n\nError: {str(e)}",
                metadata={
                    "query": query,
                    "error_type": type(e).__name__,
                    "api": "YouTube Data API v3"
                }
            )
            
            return []
    
    def _parse_duration(self, duration_str: str) -> int:
        """Parse ISO 8601 duration to seconds.
        
        Args:
            duration_str: Duration string like "PT1M30S" or "PT45S"
        
        Returns:
            Duration in seconds
        """
        try:
            # Remove PT prefix
            duration_str = duration_str.replace("PT", "")
            
            hours = 0
            minutes = 0
            seconds = 0
            
            # Parse hours
            if "H" in duration_str:
                hours_str, duration_str = duration_str.split("H")
                hours = int(hours_str)
            
            # Parse minutes
            if "M" in duration_str:
                minutes_str, duration_str = duration_str.split("M")
                minutes = int(minutes_str)
            
            # Parse seconds
            if "S" in duration_str:
                seconds_str = duration_str.replace("S", "")
                seconds = int(seconds_str)
            
            return hours * 3600 + minutes * 60 + seconds
        except Exception as e:
            logger.warning(f"Failed to parse duration '{duration_str}': {e}")
            return 0
