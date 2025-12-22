"""Constants for metadata service."""
from enum import Enum


class PageType(str, Enum):
    """Page type enumeration."""
    ATTRACTION = "attraction"
    CITY = "city"
    HOME = "home"


class AuthorType(str, Enum):
    """Author type enumeration."""
    SYSTEM = "system"
    USER = "user"


class METADATA_CONSTANTS:
    """Constants for metadata generation and validation."""
    
    # Character limits for SEO content
    TITLE_MIN_LENGTH = 30
    TITLE_MAX_LENGTH = 60
    TITLE_RECOMMENDED_LENGTH = 55
    
    DESCRIPTION_MIN_LENGTH = 120
    DESCRIPTION_MAX_LENGTH = 160
    DESCRIPTION_RECOMMENDED_LENGTH = 155
    
    # SEO title templates
    ATTRACTION_TITLE_TEMPLATE = "Everything You Need to Know about {name} – Photos, Weather & Real Visitor Reviews"
    CITY_TITLE_TEMPLATE = "Plan Your Perfect Day at {name} with Crowd Data, Maps & Insider Tips"
    HOME_TITLE = "Travel Guide with Crowd Data, Weather & Real Visitor Reviews"
    
    # Meta description templates
    ATTRACTION_DESCRIPTION_TEMPLATE = (
        "From stunning photos to today's crowd forecasts, weather, tips and reviews, "
        "our guide brings together trusted insights and real visitor stories to help you explore {name} with confidence."
    )
    CITY_DESCRIPTION_TEMPLATE = (
        "Get an immersive look at {name}—photos, hourly crowd levels, today's weather, "
        "insider tips and verified reviews—so you can plan a perfect visit based on trusted sources and tourists' experiences."
    )
    HOME_DESCRIPTION = (
        "Your all-in-one travel guide with crowd data, weather forecasts, insider tips, "
        "and verified reviews to help you plan the perfect visit to any destination."
    )
    
    # JSON-LD schema context
    SCHEMA_CONTEXT = "https://schema.org"
    
    # Placeholder values for missing data
    PLACEHOLDER_IMAGE_URL = "https://example.com/images/placeholder.jpg"
    PLACEHOLDER_RATING = 0.0
    PLACEHOLDER_REVIEW_COUNT = 0
    
    # File paths
    METADATA_FILE_NAME = "metadata.json"
    BACKUP_DIR_NAME = "backups"
    BACKUP_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
    
    # Validation rules
    REQUIRED_METADATA_FIELDS = {
        "id",
        "page_name",
        "page_type",
        "title",
        "meta_description",
        "hero_image_url",
        "author",
        "created_at",
        "updated_at",
    }
    
    # Valid page types
    VALID_PAGE_TYPES = {PageType.ATTRACTION.value, PageType.CITY.value, PageType.HOME.value}
    
    # Valid author types
    VALID_AUTHOR_TYPES = {AuthorType.SYSTEM.value, AuthorType.USER.value}


# Export enums for convenience
PAGE_TYPES = PageType
AUTHOR_TYPES = AuthorType
