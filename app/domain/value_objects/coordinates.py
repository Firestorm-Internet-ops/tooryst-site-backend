"""Coordinate value object - immutable and validated."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Coordinates:
    """Immutable coordinate value object."""
    latitude: float
    longitude: float
    
    def __post_init__(self):
        """Validate coordinates."""
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"Latitude must be between -90 and 90, got {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"Longitude must be between -180 and 180, got {self.longitude}")
    
    def is_valid(self) -> bool:
        """Check if coordinates are valid."""
        return -90 <= self.latitude <= 90 and -180 <= self.longitude <= 180
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {"latitude": self.latitude, "longitude": self.longitude}

