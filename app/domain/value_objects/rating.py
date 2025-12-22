"""Rating value object - immutable and validated."""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Rating:
    """Immutable rating value object."""
    value: float
    scale_max: float = 5.0
    review_count: Optional[int] = None
    
    def __post_init__(self):
        """Validate rating."""
        if self.value < 0:
            raise ValueError(f"Rating cannot be negative, got {self.value}")
        if self.value > self.scale_max:
            raise ValueError(f"Rating cannot exceed scale_max ({self.scale_max}), got {self.value}")
        if self.review_count is not None and self.review_count < 0:
            raise ValueError(f"Review count cannot be negative, got {self.review_count}")
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        result = {
            "overall_rating": self.value,
            "rating_scale_max": self.scale_max,
        }
        if self.review_count is not None:
            result["review_count"] = self.review_count
        return result

