"""City repository interface - abstraction for data access."""
from abc import ABC, abstractmethod
from typing import Optional, List
from app.domain.entities.city import City


class CityRepository(ABC):
    """Repository interface for City entity.
    
    Follows Interface Segregation Principle - focused interface.
    """
    
    @abstractmethod
    async def get_by_id(self, city_id: int) -> Optional[City]:
        """Get city by ID."""
        pass
    
    @abstractmethod
    async def get_by_slug(self, slug: str) -> Optional[City]:
        """Get city by slug."""
        pass
    
    @abstractmethod
    async def create(self, city: City) -> City:
        """Create new city."""
        pass
    
    @abstractmethod
    async def list_all(self, skip: int = 0, limit: int = 100) -> List[City]:
        """List all cities with pagination."""
        pass
    
    @abstractmethod
    async def count_all(self) -> int:
        """Count all cities."""
        pass

