"""Application configuration using Pydantic Settings.

This module centralizes all configuration values that may vary between environments.
Values can be overridden via environment variables or .env file.
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # ===== Image & Media =====
    PHOTO_WIDTH_SMALL: int = int(os.getenv("PHOTO_WIDTH_SMALL", "400"))
    PHOTO_WIDTH_LARGE: int = int(os.getenv("PHOTO_WIDTH_LARGE", "800"))
    PHOTO_WIDTH_THUMBNAIL: int = int(os.getenv("PHOTO_WIDTH_THUMBNAIL", "200"))
    MAP_DEFAULT_ZOOM: int = int(os.getenv("MAP_DEFAULT_ZOOM", "15"))
    MAP_SNAPSHOT_WIDTH: int = int(os.getenv("MAP_SNAPSHOT_WIDTH", "800"))
    MAP_SNAPSHOT_HEIGHT: int = int(os.getenv("MAP_SNAPSHOT_HEIGHT", "600"))
    HERO_IMAGE_MAX_WIDTH: int = int(os.getenv("HERO_IMAGE_MAX_WIDTH", "1600"))

    # ===== Distance & Location =====
    NEARBY_MAX_DISTANCE_KM: float = float(os.getenv("NEARBY_MAX_DISTANCE_KM", "10.0"))
    DISTANCE_MAX_KM: float = float(os.getenv("DISTANCE_MAX_KM", "999.999"))
    WALKING_SPEED_KMH: float = float(os.getenv("WALKING_SPEED_KMH", "5.0"))
    DEFAULT_LATITUDE: float = float(os.getenv("DEFAULT_LATITUDE", "48.8584"))
    DEFAULT_LONGITUDE: float = float(os.getenv("DEFAULT_LONGITUDE", "2.2945"))

    # ===== Retry & Timeouts =====
    DEFAULT_RETRY_COUNT: int = int(os.getenv("DEFAULT_RETRY_COUNT", "3"))
    MAX_RETRY_COUNT: int = int(os.getenv("MAX_RETRY_COUNT", "5"))
    API_TIMEOUT_SECONDS: int = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
    REDIS_RETRY_DELAY_SECONDS: int = int(os.getenv("REDIS_RETRY_DELAY_SECONDS", "1"))
    GEMINI_API_TIMEOUT_SECONDS: int = int(os.getenv("GEMINI_API_TIMEOUT_SECONDS", "30"))
    BACKUP_TIMEOUT_SECONDS: int = int(os.getenv("BACKUP_TIMEOUT_SECONDS", "300"))
    REDIS_SOCKET_TIMEOUT_SECONDS: int = int(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "5"))
    CELERY_INSPECT_TIMEOUT_SECONDS: float = float(os.getenv("CELERY_INSPECT_TIMEOUT_SECONDS", "5.0"))
    STAGE_SLOT_TIMEOUT_SECONDS: int = int(os.getenv("STAGE_SLOT_TIMEOUT_SECONDS", "60"))

    # ===== Rate Limiting & Quotas =====
    MAX_ATTRACTIONS_PER_CITY: int = int(os.getenv("MAX_ATTRACTIONS_PER_CITY", "100"))
    MIN_VIDEO_COUNT_THRESHOLD: int = int(os.getenv("MIN_VIDEO_COUNT_THRESHOLD", "5"))
    NEARBY_ATTRACTIONS_MULTIPLIER: int = int(os.getenv("NEARBY_ATTRACTIONS_MULTIPLIER", "2"))

    # ===== Pagination Defaults =====
    DEFAULT_PAGE_SIZE: int = int(os.getenv("DEFAULT_PAGE_SIZE", "20"))
    MAX_PAGE_SIZE: int = int(os.getenv("MAX_PAGE_SIZE", "100"))
    FEATURED_ATTRACTIONS_LIMIT: int = int(os.getenv("FEATURED_ATTRACTIONS_LIMIT", "12"))
    SEARCH_RESULTS_LIMIT: int = int(os.getenv("SEARCH_RESULTS_LIMIT", "20"))
    SEARCH_SUGGESTIONS_LIMIT: int = int(os.getenv("SEARCH_SUGGESTIONS_LIMIT", "5"))
    COLLAGE_IMAGE_LIMIT: int = int(os.getenv("COLLAGE_IMAGE_LIMIT", "6"))

    # ===== Redis Configuration =====
    REDIS_MAX_STAGE_ATTEMPTS: int = int(os.getenv("REDIS_MAX_STAGE_ATTEMPTS", "3"))
    REDIS_SEMAPHORE_TIMEOUT: int = int(os.getenv("REDIS_SEMAPHORE_TIMEOUT_SECONDS", "300"))
    REDIS_QUEUE_PRIORITY_HIGH: int = int(os.getenv("REDIS_QUEUE_PRIORITY_HIGH", "1"))
    REDIS_QUEUE_PRIORITY_NORMAL: int = int(os.getenv("REDIS_QUEUE_PRIORITY_NORMAL", "5"))

    # ===== Cache TTLs (seconds) =====
    CACHE_TTL_WEATHER: int = int(os.getenv("CACHE_TTL_WEATHER", "10800"))
    CACHE_TTL_BEST_TIME: int = int(os.getenv("CACHE_TTL_BEST_TIME", "432000"))
    CACHE_TTL_HERO_IMAGES: int = int(os.getenv("CACHE_TTL_HERO_IMAGES", "604800"))
    CACHE_TTL_REVIEWS: int = int(os.getenv("CACHE_TTL_REVIEWS", "86400"))
    CACHE_TTL_REDDIT: int = int(os.getenv("CACHE_TTL_REDDIT", "21600"))

    # ===== Batch Processing =====
    PARALLEL_BATCH_SIZE: int = int(os.getenv("PARALLEL_BATCH_SIZE", "10"))
    PIPELINE_STAGE_COUNT: int = int(os.getenv("PIPELINE_STAGE_COUNT", "10"))

    # ===== Data Refresh Intervals (days) =====
    REFRESH_INTERVAL_BEST_TIME: int = int(os.getenv("REFRESH_INTERVAL_BEST_TIME", "5"))
    REFRESH_INTERVAL_WEATHER: int = int(os.getenv("REFRESH_INTERVAL_WEATHER", "3"))
    REFRESH_INTERVAL_VISITOR_INFO: int = int(os.getenv("REFRESH_INTERVAL_VISITOR_INFO", "7"))
    BEST_TIME_REFRESH_THRESHOLD_DAYS: int = int(os.getenv("BEST_TIME_REFRESH_THRESHOLD_DAYS", "2"))

    # ===== Sitemap Settings =====
    SITE_URL: str = os.getenv("SITE_URL", "https://storyboard.com")
    API_BASE_URL: str = os.getenv("API_BASE_URL", "http://localhost:8000")
    SITEMAP_CACHE_TTL: int = int(os.getenv("SITEMAP_CACHE_TTL", "3600"))  # 1 hour
    SITEMAP_INDEX_CACHE_TTL: int = int(os.getenv("SITEMAP_INDEX_CACHE_TTL", "7200"))  # 2 hours

    # ===== Best Time Settings =====
    BEST_TIME_WINDOW_HOURS: int = int(os.getenv("BEST_TIME_WINDOW_HOURS", "2"))
    BEST_TIME_CLOSING_HOUR_DEFAULT: int = int(os.getenv("BEST_TIME_CLOSING_HOUR_DEFAULT", "23"))
    BEST_TIME_MORNING_THRESHOLD_HOUR: int = int(os.getenv("BEST_TIME_MORNING_THRESHOLD_HOUR", "12"))
    BEST_TIME_CROWD_LEVEL_MIN: int = int(os.getenv("BEST_TIME_CROWD_LEVEL_MIN", "0"))
    BEST_TIME_CROWD_LEVEL_MAX: int = int(os.getenv("BEST_TIME_CROWD_LEVEL_MAX", "5"))
    BEST_TIME_INTENSITY_CLOSED: int = int(os.getenv("BEST_TIME_INTENSITY_CLOSED", "999"))

    # ===== YouTube & Video Settings =====
    YOUTUBE_RETRY_DELAY_SECONDS: int = int(os.getenv("YOUTUBE_RETRY_DELAY_SECONDS", "1"))
    YOUTUBE_RETRY_MAX_ATTEMPTS: int = int(os.getenv("YOUTUBE_RETRY_MAX_ATTEMPTS", "2"))

    # ===== Debounce & Timing Settings =====
    REQUEST_DELAY_BETWEEN_CALLS_SECONDS: int = int(os.getenv("REQUEST_DELAY_BETWEEN_CALLS_SECONDS", "1"))

    # ===== Logging & Debug =====
    LOG_SEPARATOR_LENGTH: int = int(os.getenv("LOG_SEPARATOR_LENGTH", "80"))
    RESPONSE_TEXT_PREVIEW_LENGTH: int = int(os.getenv("RESPONSE_TEXT_PREVIEW_LENGTH", "200"))
    ERROR_MESSAGE_TRUNCATION_LENGTH: int = int(os.getenv("ERROR_MESSAGE_TRUNCATION_LENGTH", "400"))
    RESPONSE_TEXT_TRUNCATION_LENGTH: int = int(os.getenv("RESPONSE_TEXT_TRUNCATION_LENGTH", "500"))

    # ===== Validation Limits =====
    MIN_RATING: float = 0.0
    MAX_RATING: float = 5.0
    MIN_LATITUDE: float = -90.0
    MAX_LATITUDE: float = 90.0
    MIN_LONGITUDE: float = -180.0
    MAX_LONGITUDE: float = 180.0

    class Config:
        env_file = ".env"
        case_sensitive = True
        # Allow extra fields from .env that aren't defined here
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance.

    Uses lru_cache to ensure singleton pattern - settings are loaded once.
    """
    return Settings()


# Global settings instance for easy import
settings = get_settings()
