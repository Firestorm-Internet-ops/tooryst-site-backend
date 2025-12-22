"""Social Videos Fetcher using YouTube API."""
import os
import logging
from typing import Optional, Dict, Any, List
from .youtube_client import YouTubeClient
from app.core.quota_manager import quota_manager

logger = logging.getLogger(__name__)


class SocialVideosFetcherImpl:
    """Fetches YouTube Shorts videos for attractions."""

    def __init__(self, youtube_client: Optional[YouTubeClient] = None):
        self.youtube_client = youtube_client or YouTubeClient()
        self.target_count = int(os.getenv("YOUTUBE_SHORTS_COUNT", "5"))
        self.region_code = os.getenv("YOUTUBE_REGION_CODE", "US")

    def is_quota_exceeded(self) -> bool:
        """Check if YouTube API quota is exceeded."""
        return quota_manager.is_quota_exceeded("youtube")
    
    async def fetch(
        self,
        attraction_id: int,
        attraction_name: str,
        city_name: str,
        country: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch YouTube Shorts videos for an attraction.
        
        Strategy:
        1. Search for travel/tourism specific content
        2. If not enough, search for general attraction content
        3. Return up to target_count videos
        
        Args:
            attraction_id: ID of the attraction
            attraction_name: Name of the attraction
            city_name: City name
            country: Country name (optional)
        
        Returns:
            Dictionary with:
            - section: Section data for API response
            - videos: List of videos for DB storage
        """
        logger.info(f"Fetching YouTube Shorts for {attraction_name}")
        
        all_videos = []
        seen_video_ids = set()
        
        # Strategy 1: Travel/tourism focused queries (optimized for quota)
        # Use only 2 queries instead of 4 to save API quota
        travel_queries = [
            f"{attraction_name} {city_name} travel",
            f"visiting {attraction_name}"
        ]
        
        for query in travel_queries:
            if len(all_videos) >= self.target_count:
                break
            
            videos = await self.youtube_client.search_shorts(
                query=query,
                max_results=self.target_count,
                region_code=self.region_code
            )
            
            # Deduplicate and add
            for video in videos:
                if video["video_id"] not in seen_video_ids:
                    seen_video_ids.add(video["video_id"])
                    all_videos.append(video)
                    
                    if len(all_videos) >= self.target_count:
                        break
        
        # Strategy 2: General attraction content (if still need more)
        if len(all_videos) < self.target_count:
            logger.info(f"Only found {len(all_videos)} travel videos, searching for general content")
            
            # Use only 1 fallback query to save quota
            general_queries = [
                f"{attraction_name} {city_name}"
            ]
            
            for query in general_queries:
                if len(all_videos) >= self.target_count:
                    break
                
                videos = await self.youtube_client.search_shorts(
                    query=query,
                    max_results=self.target_count - len(all_videos),
                    region_code=self.region_code
                )
                
                # Deduplicate and add
                for video in videos:
                    if video["video_id"] not in seen_video_ids:
                        seen_video_ids.add(video["video_id"])
                        all_videos.append(video)
                        
                        if len(all_videos) >= self.target_count:
                            break
        
        if not all_videos:
            logger.warning(f"No YouTube Shorts found for {attraction_name}")
            return None
        
        # Limit to target count
        all_videos = all_videos[:self.target_count]
        
        # Sort by view count (most popular first)
        all_videos.sort(key=lambda v: v.get("view_count", 0), reverse=True)
        
        logger.info(f"Found {len(all_videos)} YouTube Shorts for {attraction_name}")
        
        # Format for section
        section_items = []
        for idx, video in enumerate(all_videos, 1):
            section_items.append({
                "id": idx,
                "platform": "youtube",
                "title": video["title"],
                "embed_url": video["embed_url"],
                "thumbnail_url": video["thumbnail_url"],
                "duration_seconds": video["duration_seconds"]
            })
        
        # Format for DB storage
        db_videos = []
        for video in all_videos:
            db_videos.append({
                "video_id": video["video_id"],
                "platform": "youtube",
                "title": video["title"],
                "embed_url": video["embed_url"],
                "thumbnail_url": video["thumbnail_url"],
                "watch_url": video["watch_url"],
                "duration_seconds": video["duration_seconds"],
                "view_count": video["view_count"],
                "channel_title": video["channel_title"]
            })
        
        return {
            "section": {
                "items": section_items
            },
            "videos": db_videos
        }

    
    async def fetch_single_video(
        self,
        attraction_id: int,
        attraction_name: str,
        city_name: str,
        country: Optional[str] = None,
        skip_count: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single YouTube video for progressive fetching.
        
        This method fetches videos in batches and returns only the video
        at position skip_count (0-indexed).
        
        Args:
            attraction_id: ID of the attraction
            attraction_name: Name of the attraction
            city_name: City name
            country: Country name (optional)
            skip_count: Number of videos to skip (0 = first video, 1 = second, etc.)
        
        Returns:
            Single video dictionary or None if no video found at that position
        """
        logger.info(f"Fetching video #{skip_count + 1} for {attraction_name}")
        
        all_videos = []
        seen_video_ids = set()
        
        # Use same query strategy but fetch more to ensure we have enough
        # Fetch up to skip_count + 3 to have buffer
        needed_count = skip_count + 3
        
        # Travel/tourism focused queries
        travel_queries = [
            f"{attraction_name} {city_name} travel",
            f"visiting {attraction_name}"
        ]
        
        for query in travel_queries:
            if len(all_videos) >= needed_count:
                break
            
            videos = await self.youtube_client.search_shorts(
                query=query,
                max_results=needed_count
            )
            
            # Deduplicate and add
            for video in videos:
                if video["video_id"] not in seen_video_ids:
                    seen_video_ids.add(video["video_id"])
                    all_videos.append(video)
                    
                    if len(all_videos) >= needed_count:
                        break
        
        # General content fallback
        if len(all_videos) < needed_count:
            general_query = f"{attraction_name} {city_name}"
            videos = await self.youtube_client.search_shorts(
                query=general_query,
                max_results=needed_count - len(all_videos),
                region_code=self.region_code
            )
            
            for video in videos:
                if video["video_id"] not in seen_video_ids:
                    seen_video_ids.add(video["video_id"])
                    all_videos.append(video)
                    
                    if len(all_videos) >= needed_count:
                        break
        
        # Sort by view count
        all_videos.sort(key=lambda v: v.get("view_count", 0), reverse=True)
        
        # Check if we have the video at skip_count position
        if len(all_videos) <= skip_count:
            logger.warning(f"No video found at position {skip_count} for {attraction_name}")
            return None
        
        # Return the video at skip_count position
        video = all_videos[skip_count]
        
        logger.info(f"Found video #{skip_count + 1} for {attraction_name}: {video['title']}")
        
        return {
            "video_id": video["video_id"],
            "platform": "youtube",
            "title": video["title"],
            "embed_url": video["embed_url"],
            "thumbnail_url": video["thumbnail_url"],
            "watch_url": video["watch_url"],
            "duration_seconds": video["duration_seconds"],
            "view_count": video["view_count"],
            "channel_title": video["channel_title"]
        }
