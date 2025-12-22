"""Attraction domain entity - pure business logic."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from app.domain.value_objects.coordinates import Coordinates


@dataclass
class Attraction:
    """Attraction domain entity."""
    id: Optional[int]
    city_id: int
    name: str
    slug: str
    coordinates: Coordinates
    place_id: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    address: Optional[str] = None
    resolved_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def is_valid(self) -> bool:
        """Validate attraction business rules."""
        return bool(
            self.name and
            self.name.strip() and
            self.slug and
            self.slug.strip() and
            self.coordinates.is_valid() and
            self.city_id > 0
        )
    
    def update_rating(self, rating: float, review_count: int):
        """Update rating with business rule validation."""
        if rating < 0 or rating > 5:
            raise ValueError("Rating must be between 0 and 5")
        if review_count < 0:
            raise ValueError("Review count cannot be negative")
        self.rating = rating
        self.review_count = review_count
    
    def update_place_id(self, place_id: str):
        """Update Google Place ID."""
        if not place_id or not place_id.strip():
            raise ValueError("Place ID cannot be empty")
        self.place_id = place_id.strip()
    


