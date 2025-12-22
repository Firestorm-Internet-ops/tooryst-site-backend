"""Pydantic schemas for API responses - match frontend contract exactly."""
from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Dict, Any


# Page Response Schemas
class HeroImageSchema(BaseModel):
    """Hero image schema."""
    url: str
    alt: Optional[str] = None
    position: Optional[int] = None


class BestTimeTodaySchema(BaseModel):
    """Best time today schema."""
    start_local_time: str
    end_local_time: str


class BestTimeCardSchema(BaseModel):
    """Best time card schema."""
    is_open_today: bool
    today_local_date: str = ""
    today_opening_hours_local: Optional[str] = None
    today_opening_time: Optional[str] = None
    today_closing_time: Optional[str] = None
    crowd_level_today: Optional[int] = None
    crowd_level_label_today: Optional[str] = None
    best_time_today: Optional[BestTimeTodaySchema] = None
    best_time_text: Optional[str] = None
    summary_text: Optional[str] = None


class WeatherCardSchema(BaseModel):
    """Weather card schema."""
    date_local: str
    temperature_c: Optional[float] = None
    feels_like_c: Optional[float] = None
    min_temperature_c: Optional[float] = None
    max_temperature_c: Optional[float] = None
    condition: Optional[str] = None
    precipitation_mm: Optional[float] = None
    wind_speed_kph: Optional[float] = None
    humidity_percent: Optional[int] = None
    icon_url: Optional[str] = None


class SocialVideoCardSchema(BaseModel):
    """Social video card schema."""
    platform: str
    title: str
    embed_url: str
    thumbnail_url: Optional[str] = None
    source_url: Optional[str] = None


class MapCardSchema(BaseModel):
    """Map card schema."""
    latitude: float
    longitude: float
    static_map_image_url: Optional[str] = None
    maps_link_url: Optional[str] = None
    address: Optional[str] = None


class ReviewCardSchema(BaseModel):
    """Review card schema."""
    overall_rating: Optional[float] = None
    rating_scale_max: int = 5
    review_count: Optional[int] = None
    summary_gemini: Optional[str] = None


class TipSchema(BaseModel):
    """Tip schema."""
    id: int
    text: str
    source: Optional[str] = None
    scope: Optional[str] = None  # 'attraction' or 'city'
    position: Optional[int] = None  # 0 for prominent, 1 for detailed


class TipsCardSchema(BaseModel):
    """Tips card schema."""
    safety: List[TipSchema]
    insider: List[TipSchema]


class AboutCardSchema(BaseModel):
    """About card schema."""
    short_description: Optional[str] = None
    recommended_duration_minutes: Optional[int] = None
    highlights: Optional[List[str]] = None


class NearbyAttractionCardSchema(BaseModel):
    """Nearby attraction card schema."""
    id: int
    slug: str
    name: str
    distance_km: Optional[float] = None
    walking_time_minutes: Optional[int] = None
    hero_image_url: Optional[str] = None


class NearbyAttractionItemSchema(BaseModel):
    """Nearby attraction item schema for list."""
    name: str
    slug: Optional[str] = None
    link: Optional[str] = None
    distance_km: Optional[float] = None
    distance_text: Optional[str] = None
    walking_time_minutes: Optional[int] = None
    image_url: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    vicinity: Optional[str] = None


class AttractionCardsSchema(BaseModel):
    """All cards schema."""
    hero_images: Optional[Dict[str, List[HeroImageSchema]]] = None
    best_time: Optional[BestTimeCardSchema] = None
    weather: Optional[WeatherCardSchema] = None
    social_video: Optional[SocialVideoCardSchema] = None
    map: Optional[MapCardSchema] = None
    review: Optional[ReviewCardSchema] = None
    tips: Optional[TipsCardSchema] = None
    about: Optional[AboutCardSchema] = None
    nearby_attraction: Optional[NearbyAttractionCardSchema] = None


class AttractionPageResponseSchema(BaseModel):
    """Attraction page response schema - matches frontend contract."""
    attraction_id: int
    slug: str
    name: str
    city: Optional[str] = None
    country: Optional[str] = None
    timezone: Optional[str] = None
    cards: AttractionCardsSchema
    nearby_attractions: Optional[List[NearbyAttractionItemSchema]] = None
    social_videos: Optional[List[Dict[str, Any]]] = None
    audience_profiles: Optional[List[Dict[str, Any]]] = None
    best_time: Optional[List[Dict[str, Any]]] = None
    visitor_info: Optional[Dict[str, Any]] = None
    
    model_config = ConfigDict(from_attributes=True)

