"""Quota management for external APIs."""
import os
import logging
from datetime import datetime, timedelta
from typing import Optional
import redis

logger = logging.getLogger(__name__)


class QuotaManager:
    """Manages API quota limits and prevents excessive calls when quota is exceeded."""
    
    def __init__(self):
        """Initialize quota manager with Redis connection."""
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_client = None
        
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=3,  # Use separate DB for quota management
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("✓ Quota manager connected to Redis")
        except Exception as e:
            logger.warning(f"⚠ Quota manager: Redis not available, using in-memory fallback: {e}")
            self.redis_client = None
            # Fallback to in-memory storage
            self._memory_store = {}
    
    def is_quota_exceeded(self, api_name: str) -> bool:
        """Check if API quota is exceeded.
        
        Args:
            api_name: Name of the API (e.g., 'youtube', 'google_places')
            
        Returns:
            True if quota is exceeded and we should skip API calls
        """
        key = f"quota_exceeded:{api_name}"
        
        try:
            if self.redis_client:
                return self.redis_client.get(key) == "1"
            else:
                # Fallback to in-memory
                return self._memory_store.get(key, False)
        except Exception as e:
            logger.error(f"Error checking quota status: {e}")
            return False
    
    def mark_quota_exceeded(self, api_name: str, reset_at: Optional[datetime] = None):
        """Mark API quota as exceeded.
        
        Args:
            api_name: Name of the API
            reset_at: When the quota resets (defaults to next day at midnight PT)
        """
        key = f"quota_exceeded:{api_name}"
        
        # Calculate TTL until quota reset
        if reset_at is None:
            # YouTube quota resets at midnight Pacific Time (12:00 AM PT)
            # PT is UTC-8 (or UTC-7 during daylight saving)
            # So midnight PT = 8 AM UTC (standard time) or 7 AM UTC (daylight saving)
            # We'll use 8 AM UTC as the reset time (conservative estimate)
            now = datetime.utcnow()
            
            # Calculate next midnight PT (8 AM UTC)
            reset_at = now.replace(hour=8, minute=0, second=0, microsecond=0)
            
            # If we're already past 8 AM UTC today, reset is tomorrow at 8 AM UTC
            if now.hour >= 8:
                reset_at = reset_at + timedelta(days=1)
        
        ttl_seconds = int((reset_at - datetime.utcnow()).total_seconds())
        
        try:
            if self.redis_client:
                self.redis_client.setex(key, ttl_seconds, "1")
                logger.warning(f"🚫 {api_name} quota exceeded - API calls disabled for {ttl_seconds // 3600} hours")
            else:
                # Fallback to in-memory (no TTL, will reset on restart)
                self._memory_store[key] = True
                logger.warning(f"🚫 {api_name} quota exceeded - API calls disabled until restart")
        except Exception as e:
            logger.error(f"Error marking quota exceeded: {e}")
    
    def reset_quota(self, api_name: str):
        """Manually reset quota flag (for testing or manual override).
        
        Args:
            api_name: Name of the API
        """
        key = f"quota_exceeded:{api_name}"
        
        try:
            if self.redis_client:
                self.redis_client.delete(key)
                logger.info(f"✓ {api_name} quota flag reset")
            else:
                self._memory_store.pop(key, None)
                logger.info(f"✓ {api_name} quota flag reset")
        except Exception as e:
            logger.error(f"Error resetting quota: {e}")
    
    def get_quota_status(self, api_name: str) -> dict:
        """Get detailed quota status for an API.
        
        Args:
            api_name: Name of the API
            
        Returns:
            Dictionary with quota status information
        """
        key = f"quota_exceeded:{api_name}"
        
        try:
            if self.redis_client:
                is_exceeded = self.redis_client.get(key) == "1"
                ttl = self.redis_client.ttl(key) if is_exceeded else 0
                
                return {
                    "api": api_name,
                    "quota_exceeded": is_exceeded,
                    "resets_in_seconds": ttl if ttl > 0 else 0,
                    "resets_in_hours": round(ttl / 3600, 1) if ttl > 0 else 0
                }
            else:
                is_exceeded = self._memory_store.get(key, False)
                return {
                    "api": api_name,
                    "quota_exceeded": is_exceeded,
                    "resets_in_seconds": 0,
                    "resets_in_hours": 0,
                    "note": "Using in-memory storage, resets on restart"
                }
        except Exception as e:
            logger.error(f"Error getting quota status: {e}")
            return {
                "api": api_name,
                "quota_exceeded": False,
                "error": str(e)
            }


# Global quota manager instance
quota_manager = QuotaManager()
