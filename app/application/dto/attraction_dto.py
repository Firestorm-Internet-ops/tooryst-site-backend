"""Data Transfer Objects for Attraction API responses.
These match the frontend contract exactly."""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class HeroImageDTO:
    """Hero image DTO."""
    url: str
    alt: Optional[str] = None
    position: Optional[int] = None


@dataclass
class BestTimeCardDTO:
    """Best time card DTO."""
    is_open_today: bool
    today_local_date: str = ""
    today_opening_hours_local: Optional[str] = None
    today_opening_time: Optional[str] = None
    today_closing_time: Optional[str] = None
    crowd_level_today: Optional[int] = None
    crowd_level_label_today: Optional[str] = None
    best_time_today: Optional[Dict[str, str]] = None
    best_time_text: Optional[str] = None
    summary_text: Optional[str] = None


@dataclass
class WeatherCardDTO:
    """Weather card DTO."""
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


@dataclass
class SocialVideoCardDTO:
    """Social video card DTO."""
    platform: str
    title: str
    embed_url: str
    thumbnail_url: Optional[str] = None
    source_url: Optional[str] = None


@dataclass
class MapCardDTO:
    """Map card DTO."""
    latitude: float
    longitude: float
    static_map_image_url: Optional[str] = None
    maps_link_url: Optional[str] = None
    address: Optional[str] = None


@dataclass
class ReviewCardDTO:
    """Review card DTO."""
    overall_rating: Optional[float] = None
    rating_scale_max: int = 5
    review_count: Optional[int] = None
    summary_gemini: Optional[str] = None


@dataclass
class TipDTO:
    """Tip DTO."""
    id: int
    text: str
    source: Optional[str] = None


@dataclass
class TipsCardDTO:
    """Tips card DTO."""
    safety: List[TipDTO]
    insider: List[TipDTO]


@dataclass
class AboutCardDTO:
    """About card DTO."""
    short_description: Optional[str] = None
    recommended_duration_minutes: Optional[int] = None
    highlights: Optional[List[str]] = None


@dataclass
class NearbyAttractionCardDTO:
    """Nearby attraction card DTO."""
    id: int
    slug: str
    name: str
    distance_km: Optional[float] = None
    walking_time_minutes: Optional[int] = None
    hero_image_url: Optional[str] = None


@dataclass
class AttractionCardsDTO:
    """All cards for an attraction."""
    hero_images: Optional[Dict[str, List[HeroImageDTO]]] = None
    best_time: Optional[BestTimeCardDTO] = None
    weather: Optional[WeatherCardDTO] = None
    social_video: Optional[SocialVideoCardDTO] = None
    map: Optional[MapCardDTO] = None
    review: Optional[ReviewCardDTO] = None
    tips: Optional[TipsCardDTO] = None
    about: Optional[AboutCardDTO] = None
    nearby_attraction: Optional[NearbyAttractionCardDTO] = None


@dataclass
class AttractionPageDTO:
    """Complete attraction page DTO matching frontend contract."""
    attraction_id: int
    slug: str
    name: str
    city: Optional[str] = None
    country: Optional[str] = None
    timezone: Optional[str] = None
    cards: AttractionCardsDTO = None
    nearby_attractions: Optional[list] = None
    social_videos: Optional[list] = None
    audience_profiles: Optional[list] = None
    best_time: Optional[list] = None
    visitor_info: Optional[dict] = None
    
    def __post_init__(self):
        """Initialize cards if not provided."""
        if self.cards is None:
            self.cards = AttractionCardsDTO()

