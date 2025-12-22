"""Schemas for admin endpoints."""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel


class HasDataSchema(BaseModel):
    """Count of related data for an attraction."""
    hero_images: int = 0
    reviews: int = 0
    best_time: int = 0
    weather: int = 0
    tips: int = 0
    social_videos: int = 0
    nearby: int = 0
    audiences: int = 0
    has_map: bool = False
    has_metadata: bool = False
    has_widget: bool = False


class CompletenessSchema(BaseModel):
    """Data completeness information for an attraction."""
    score: int  # 0-10
    percentage: float  # 0-100
    status: str  # "complete", "partial", "incomplete"


class AttractionListItemSchema(BaseModel):
    """Attraction item in list view."""
    id: int
    slug: str
    name: str
    city: str
    country: Optional[str] = None
    has_data: HasDataSchema
    completeness: CompletenessSchema


class PaginationSchema(BaseModel):
    """Pagination metadata."""
    page: int
    per_page: int
    total: int
    pages: int


class AttractionListResponseSchema(BaseModel):
    """Response for list attractions endpoint."""
    attractions: List[AttractionListItemSchema]
    pagination: PaginationSchema


class AttractionCompleteDataSchema(BaseModel):
    """Complete attraction data grouped by table."""
    attractions: Optional[Dict[str, Any]] = None
    cities: Optional[Dict[str, Any]] = None
    hero_images: Optional[List[Dict[str, Any]]] = None
    best_time_data: Optional[List[Dict[str, Any]]] = None
    weather_forecast: Optional[List[Dict[str, Any]]] = None
    reviews: Optional[List[Dict[str, Any]]] = None
    tips: Optional[List[Dict[str, Any]]] = None
    map_snapshot: Optional[Dict[str, Any]] = None
    attraction_metadata: Optional[Dict[str, Any]] = None
    social_videos: Optional[List[Dict[str, Any]]] = None
    nearby_attractions: Optional[List[Dict[str, Any]]] = None
    audience_profiles: Optional[List[Dict[str, Any]]] = None
    widget_config: Optional[Dict[str, Any]] = None

    class Config:
        """Pydantic config."""
        from_attributes = True
