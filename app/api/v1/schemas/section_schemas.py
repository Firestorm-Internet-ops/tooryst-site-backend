"""Pydantic schemas for sections API responses."""
from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Dict, Any


class BestTimeTabSchema(BaseModel):
    """Best time tab schema."""
    label: str
    date: str
    chart_json: Optional[str] = None
    summary: Optional[str] = None


class BestTimeSectionContentSchema(BaseModel):
    """Best time section content schema."""
    tabs: List[BestTimeTabSchema]
    default_tab: str


class ReviewItemSchema(BaseModel):
    """Review item schema."""
    author_name: str
    author_url: Optional[str] = None
    author_photo_url: Optional[str] = None
    rating: int
    text: Optional[str] = None
    time: Optional[str] = None
    source: str = "Google"


class ReviewsSectionContentSchema(BaseModel):
    """Reviews section content schema."""
    overall_rating: Optional[float] = None
    rating_scale_max: int = 5
    total_reviews: Optional[int] = None
    summary: Optional[str] = None
    items: List[ReviewItemSchema] = []


class WidgetSectionContentSchema(BaseModel):
    """Widget section content schema."""
    html: Optional[str] = None
    custom_config: Optional[Dict[str, Any]] = None


class MapSectionContentSchema(BaseModel):
    """Map section content schema."""
    latitude: float
    longitude: float
    address: Optional[str] = None
    directions_url: Optional[str] = None
    zoom_level: Optional[int] = None


class VisitorInfoItemSchema(BaseModel):
    """Visitor info item schema."""
    label: str
    value: str
    url: Optional[str] = None


class OpeningHoursSchema(BaseModel):
    """Opening hours schema."""
    day: str
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    is_closed: bool = False


class VisitorInfoSectionContentSchema(BaseModel):
    """Visitor info section content schema."""
    contact_items: List[VisitorInfoItemSchema] = []
    opening_hours: List[OpeningHoursSchema] = []
    accessibility_info: Optional[str] = None
    best_season: Optional[str] = None


class TipItemSchema(BaseModel):
    """Tip item schema."""
    id: int
    text: str
    source: Optional[str] = None


class TipsSectionContentSchema(BaseModel):
    """Tips section content schema."""
    safety: List[TipItemSchema] = []
    insider: List[TipItemSchema] = []


class SocialVideoItemSchema(BaseModel):
    """Social video item schema."""
    id: int
    platform: str
    title: str
    embed_url: str
    thumbnail_url: Optional[str] = None
    duration_seconds: Optional[int] = None


class SocialVideosSectionContentSchema(BaseModel):
    """Social videos section content schema."""
    items: List[SocialVideoItemSchema] = []


class NearbyAttractionItemSchema(BaseModel):
    """Nearby attraction item schema."""
    id: int
    slug: Optional[str] = None
    name: str
    distance_text: Optional[str] = None
    distance_km: Optional[float] = None
    rating: Optional[float] = None
    user_ratings_total: Optional[int] = None
    review_count: Optional[int] = None
    image_url: Optional[str] = None
    link: Optional[str] = None
    vicinity: Optional[str] = None
    audience_type: Optional[str] = None
    audience_text: Optional[str] = None


class NearbyAttractionsSectionContentSchema(BaseModel):
    """Nearby attractions section content schema."""
    items: List[NearbyAttractionItemSchema] = []


class AudienceProfileItemSchema(BaseModel):
    """Audience profile item schema."""
    audience_type: str
    description: str
    emoji: Optional[str] = None


class AudienceProfileSectionContentSchema(BaseModel):
    """Audience profile section content schema."""
    items: List[AudienceProfileItemSchema] = []


# Union type for section content (using Any for now, Pydantic handles validation)
SectionContentSchema = (
    BestTimeSectionContentSchema |
    ReviewsSectionContentSchema |
    WidgetSectionContentSchema |
    MapSectionContentSchema |
    VisitorInfoSectionContentSchema |
    TipsSectionContentSchema |
    SocialVideosSectionContentSchema |
    NearbyAttractionsSectionContentSchema |
    AudienceProfileSectionContentSchema
)


class SectionSchema(BaseModel):
    """Section schema."""
    section_type: str
    title: str
    subtitle: Optional[str] = None
    layout: str = "default"
    is_visible: bool = True
    order: int = 0
    content: Dict[str, Any]  # Will be validated based on section_type


class AttractionSectionsResponseSchema(BaseModel):
    """Attraction sections response schema - matches frontend contract."""
    attraction_id: int
    slug: str
    name: str
    city: str
    country: Optional[str] = None
    sections: List[SectionSchema]
    
    model_config = ConfigDict(from_attributes=True)

