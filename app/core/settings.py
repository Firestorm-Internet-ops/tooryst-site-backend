"""Application settings loaded from environment variables."""
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings."""
    
    # ============================================================================
    # DATABASE
    # ============================================================================
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "localhost")
    DATABASE_PORT: int = int(os.getenv("DATABASE_PORT", "3306"))
    DATABASE_USER: str = os.getenv("DATABASE_USER", "root")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "")
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "storyboard")
    
    # ============================================================================
    # API KEYS
    # ============================================================================
    GOOGLE_PLACES_API_KEY: Optional[str] = os.getenv("GOOGLE_PLACES_API_KEY")
    GOOGLE_MAPS_API_KEY: Optional[str] = os.getenv("GOOGLE_MAPS_API_KEY")
    YOUTUBE_API_KEY: Optional[str] = os.getenv("YOUTUBE_API_KEY")
    REDDIT_CLIENT_ID: Optional[str] = os.getenv("REDDIT_CLIENT_ID")
    REDDIT_CLIENT_SECRET: Optional[str] = os.getenv("REDDIT_CLIENT_SECRET")
    OPENWEATHERMAP_API_KEY: Optional[str] = os.getenv("OPENWEATHERMAP_API_KEY")
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
    BESTTIME_API_PRIVATE_KEY: Optional[str] = os.getenv("BESTTIME_API_PRIVATE_KEY")
    
    # ============================================================================
    # PERFORMANCE SETTINGS
    # ============================================================================
    
    # Parallel Processing
    PARALLEL_BATCH_SIZE: int = int(os.getenv("PARALLEL_BATCH_SIZE", "10"))
    PARALLEL_MAX_WORKERS: int = int(os.getenv("PARALLEL_MAX_WORKERS", "20"))
    
    # Redis Cache
    REDIS_CACHE_ENABLED: bool = os.getenv("REDIS_CACHE_ENABLED", "true").lower() == "true"
    REDIS_CACHE_HOST: str = os.getenv("REDIS_CACHE_HOST", "localhost")
    REDIS_CACHE_PORT: int = int(os.getenv("REDIS_CACHE_PORT", "6379"))
    REDIS_CACHE_DB: int = int(os.getenv("REDIS_CACHE_DB", "2"))
    REDIS_CACHE_PASSWORD: Optional[str] = os.getenv("REDIS_CACHE_PASSWORD") or None
    
    # Cache TTLs (in seconds)
    REDIS_CACHE_TTL_GOOGLE_PLACES: int = int(os.getenv("REDIS_CACHE_TTL_GOOGLE_PLACES", "604800"))  # 7 days
    REDIS_CACHE_TTL_YOUTUBE: int = int(os.getenv("REDIS_CACHE_TTL_YOUTUBE", "259200"))  # 3 days
    REDIS_CACHE_TTL_WEATHER: int = int(os.getenv("REDIS_CACHE_TTL_WEATHER", "10800"))  # 3 hours
    REDIS_CACHE_TTL_BESTTIME: int = int(os.getenv("REDIS_CACHE_TTL_BESTTIME", "86400"))  # 1 day
    
    # Connection Pooling
    HTTP_MAX_CONNECTIONS: int = int(os.getenv("HTTP_MAX_CONNECTIONS", "100"))
    HTTP_MAX_KEEPALIVE: int = int(os.getenv("HTTP_MAX_KEEPALIVE", "50"))
    HTTP_ENABLE_HTTP2: bool = os.getenv("HTTP_ENABLE_HTTP2", "true").lower() == "true"
    
    # ============================================================================
    # CELERY
    # ============================================================================
    CELERY_ENABLED: bool = os.getenv("CELERY_ENABLED", "true").lower() == "true"
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", f"redis://{REDIS_HOST}:{REDIS_PORT}/1")
    CELERY_WORKER_CONCURRENCY: int = int(os.getenv("CELERY_WORKER_CONCURRENCY", "4"))
    
    # ============================================================================
    # DATA COUNTS
    # ============================================================================
    HERO_CAROUSEL_IMAGE_COUNT: int = int(os.getenv("HERO_CAROUSEL_IMAGE_COUNT", "10"))
    REVIEW_CARD_COUNT: int = int(os.getenv("REVIEW_CARD_COUNT", "5"))
    YOUTUBE_SHORTS_COUNT: int = int(os.getenv("YOUTUBE_SHORTS_COUNT", "5"))
    NEARBY_ATTRACTIONS_COUNT: int = int(os.getenv("NEARBY_ATTRACTIONS_COUNT", "10"))
    
    @classmethod
    def get_redis_cache_url(cls) -> str:
        """Get Redis cache connection URL."""
        if cls.REDIS_CACHE_PASSWORD:
            return f"redis://:{cls.REDIS_CACHE_PASSWORD}@{cls.REDIS_CACHE_HOST}:{cls.REDIS_CACHE_PORT}/{cls.REDIS_CACHE_DB}"
        return f"redis://{cls.REDIS_CACHE_HOST}:{cls.REDIS_CACHE_PORT}/{cls.REDIS_CACHE_DB}"


# Global settings instance
settings = Settings()
