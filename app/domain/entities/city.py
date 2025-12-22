"""City domain entity - pure business logic."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from app.domain.value_objects.coordinates import Coordinates


@dataclass
class City:
    """City domain entity."""
    id: Optional[int]
    slug: str
    name: str
    country: Optional[str] = None
    coordinates: Optional[Coordinates] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def is_valid(self) -> bool:
        """Validate city business rules."""
        return bool(
            self.name and
            self.name.strip() and
            self.slug and
            self.slug.strip()
        )
    
    def update_coordinates(self, coordinates: Coordinates):
        """Update city coordinates."""
        if not coordinates.is_valid():
            raise ValueError("Invalid coordinates")
        self.coordinates = coordinates

