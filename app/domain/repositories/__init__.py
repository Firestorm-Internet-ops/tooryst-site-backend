"""Repository interfaces."""
from app.domain.repositories.attraction_repository import AttractionRepository
from app.domain.repositories.city_repository import CityRepository

__all__ = [
    "AttractionRepository",
    "CityRepository",
]

