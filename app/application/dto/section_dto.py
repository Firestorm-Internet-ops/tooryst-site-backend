"""Data Transfer Objects for Section API responses."""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class BestTimeTabDTO:
    """Best time tab DTO."""
    label: str
    date: str
    chart_json: Optional[str] = None
    summary: Optional[str] = None


@dataclass
class BestTimeSectionContentDTO:
    """Best time section content DTO."""
    tabs: List[BestTimeTabDTO]
    default_tab: str


@dataclass
class ReviewItemDTO:
    """Review item DTO."""
    author_name: str
    rating: int
    source: str = "Google"
    author_url: Optional[str] = None
    author_photo_url: Optional[str] = None
    text: Optional[str] = None
    time: Optional[str] = None


@dataclass
class ReviewsSectionContentDTO:
    """Reviews section content DTO."""
    overall_rating: Optional[float] = None
    rating_scale_max: int = 5
    total_reviews: Optional[int] = None
    summary: Optional[str] = None
    items: List[ReviewItemDTO] = None
    
    def __post_init__(self):
        """Initialize items if not provided."""
        if self.items is None:
            self.items = []


@dataclass
class WidgetSectionContentDTO:
    """Widget section content DTO."""
    html: Optional[str] = None
    custom_config: Optional[Dict[str, Any]] = None


@dataclass
class MapSectionContentDTO:
    """Map section content DTO."""
    latitude: float
    longitude: float
    address: Optional[str] = None
    directions_url: Optional[str] = None
    zoom_level: Optional[int] = None


@dataclass
class VisitorInfoItemDTO:
    """Visitor info item DTO."""
    label: str
    value: str
    url: Optional[str] = None


@dataclass
class OpeningHoursDTO:
    """Opening hours DTO."""
    day: str
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    is_closed: bool = False


@dataclass
class VisitorInfoSectionContentDTO:
    """Visitor info section content DTO."""
    contact_items: List[VisitorInfoItemDTO] = None
    opening_hours: List[OpeningHoursDTO] = None
    accessibility_info: Optional[str] = None
    best_season: Optional[str] = None
    
    def __post_init__(self):
        """Initialize lists if not provided."""
        if self.contact_items is None:
            self.contact_items = []
        if self.opening_hours is None:
            self.opening_hours = []


@dataclass
class TipItemDTO:
    """Tip item DTO for sections."""
    id: int
    text: str
    source: Optional[str] = None


@dataclass
class TipsSectionContentDTO:
    """Tips section content DTO."""
    safety: List[TipItemDTO] = None
    insider: List[TipItemDTO] = None
    
    def __post_init__(self):
        """Initialize lists if not provided."""
        if self.safety is None:
            self.safety = []
        if self.insider is None:
            self.insider = []


@dataclass
class SocialVideoItemDTO:
    """Social video item DTO."""
    id: int
    platform: str
    title: str
    embed_url: str
    thumbnail_url: Optional[str] = None
    duration_seconds: Optional[int] = None


@dataclass
class SocialVideosSectionContentDTO:
    """Social videos section content DTO."""
    items: List[SocialVideoItemDTO] = None
    
    def __post_init__(self):
        """Initialize items if not provided."""
        if self.items is None:
            self.items = []


@dataclass
class NearbyAttractionItemDTO:
    """Nearby attraction item DTO."""
    id: int
    name: str
    slug: Optional[str] = None
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


@dataclass
class NearbyAttractionsSectionContentDTO:
    """Nearby attractions section content DTO."""
    items: List[NearbyAttractionItemDTO] = None
    
    def __post_init__(self):
        """Initialize items if not provided."""
        if self.items is None:
            self.items = []


@dataclass
class AudienceProfileItemDTO:
    """Audience profile item DTO."""
    audience_type: str
    description: str
    emoji: Optional[str] = None


@dataclass
class AudienceProfileSectionContentDTO:
    """Audience profile section content DTO."""
    items: List[AudienceProfileItemDTO] = None
    
    def __post_init__(self):
        """Initialize items if not provided."""
        if self.items is None:
            self.items = []


# Union type for section content
SectionContentDTO = (
    BestTimeSectionContentDTO |
    ReviewsSectionContentDTO |
    WidgetSectionContentDTO |
    MapSectionContentDTO |
    VisitorInfoSectionContentDTO |
    TipsSectionContentDTO |
    SocialVideosSectionContentDTO |
    NearbyAttractionsSectionContentDTO |
    AudienceProfileSectionContentDTO
)


@dataclass
class SectionDTO:
    """Section DTO."""
    section_type: str
    title: str
    subtitle: Optional[str] = None
    layout: str = "default"
    is_visible: bool = True
    order: int = 0
    content: SectionContentDTO = None


@dataclass
class AttractionSectionsDTO:
    """Complete attraction sections DTO matching frontend contract."""
    attraction_id: int
    slug: str
    name: str
    city: str
    country: Optional[str] = None
    sections: List[SectionDTO] = None
    
    def __post_init__(self):
        """Initialize sections if not provided."""
        if self.sections is None:
            self.sections = []

