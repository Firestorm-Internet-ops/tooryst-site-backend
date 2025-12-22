"""Redis-based cache for API responses to reduce redundant calls."""
import hashlib
import json
import logging
from typing import Optional, Any
import redis.asyncio as redis
from app.core.settings import settings

logger = logging.getLogger(__name__)


class RedisCache:
    """Redis-based cache with TTL support.
    
    Use this to cache API responses that don't change frequently:
    - Google Places details (change rarely)
    - YouTube search results (change slowly)
    - Weather forecasts (valid for hours)
    - BestTime data (stable patterns)
    """
    
    def __init__(self):
        self._redis: Optional[redis.Redis] = None
        self._enabled = settings.REDIS_CACHE_ENABLED
        
        if self._enabled:
            try:
                self._redis = redis.from_url(
                    settings.get_redis_cache_url(),
                    encoding="utf-8",
                    decode_responses=True
                )
                logger.info(f"Redis cache initialized: {settings.REDIS_CACHE_HOST}:{settings.REDIS_CACHE_PORT}/{settings.REDIS_CACHE_DB}")
            except Exception as e:
                logger.error(f"Failed to initialize Redis cache: {e}")
                self._enabled = False
    
    def _make_key(self, prefix: str, **kwargs) -> str:
        """Create cache key from prefix and kwargs."""
        # Sort kwargs for consistent keys
        sorted_items = sorted(kwargs.items())
        key_str = f"{prefix}:{json.dumps(sorted_items, sort_keys=True)}"
        # Hash for shorter keys
        key_hash = hashlib.md5(key_str.encode()).hexdigest()
        return f"cache:{prefix}:{key_hash}"
    
    async def get(self, prefix: str, **kwargs) -> Optional[Any]:
        """Get cached value if exists and not expired."""
        if not self._enabled or not self._redis:
            return None
        
        try:
            key = self._make_key(prefix, **kwargs)
            value = await self._redis.get(key)
            
            if value:
                # Deserialize JSON
                return json.loads(value)
            
            return None
        except Exception as e:
            logger.warning(f"Cache get error for {prefix}: {e}")
            return None
    
    async def set(self, value: Any, ttl_seconds: int, prefix: str, **kwargs):
        """Set cached value with TTL."""
        if not self._enabled or not self._redis:
            return
        
        try:
            key = self._make_key(prefix, **kwargs)
            # Serialize to JSON
            serialized = json.dumps(value)
            await self._redis.setex(key, ttl_seconds, serialized)
        except Exception as e:
            logger.warning(f"Cache set error for {prefix}: {e}")
    
    async def delete(self, prefix: str, **kwargs):
        """Delete cached value."""
        if not self._enabled or not self._redis:
            return
        
        try:
            key = self._make_key(prefix, **kwargs)
            await self._redis.delete(key)
        except Exception as e:
            logger.warning(f"Cache delete error for {prefix}: {e}")
    
    async def clear_prefix(self, prefix: str):
        """Clear all keys with given prefix."""
        if not self._enabled or not self._redis:
            return
        
        try:
            pattern = f"cache:{prefix}:*"
            cursor = 0
            while True:
                cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)
                if keys:
                    await self._redis.delete(*keys)
                if cursor == 0:
                    break
            logger.info(f"Cleared cache for prefix: {prefix}")
        except Exception as e:
            logger.warning(f"Cache clear error for {prefix}: {e}")
    
    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            logger.info("Redis cache connection closed")


# Global cache instance
_cache: Optional[RedisCache] = None


def get_cache() -> RedisCache:
    """Get the global cache instance."""
    global _cache
    if _cache is None:
        _cache = RedisCache()
    return _cache


async def close_cache():
    """Close the global cache instance."""
    global _cache
    if _cache:
        await _cache.close()
        _cache = None
