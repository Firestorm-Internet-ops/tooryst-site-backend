"""Attraction repository interface - abstraction for data access."""
from abc import ABC, abstractmethod
from typing import Optional, List
from app.domain.entities.attraction import Attraction


class AttractionRepository(ABC):
    """Repository interface for Attraction entity.
    
    Follows Interface Segregation Principle - focused interface.
    Follows Dependency Inversion Principle - depends on abstraction.
    """
    
    @abstractmethod
    async def get_by_id(self, attraction_id: int) -> Optional[Attraction]:
        """Get attraction by ID."""
        pass
    
    @abstractmethod
    async def get_by_slug(self, slug: str) -> Optional[Attraction]:
        """Get attraction by slug."""
        pass
    
    @abstractmethod
    async def create(self, attraction: Attraction) -> Attraction:
        """Create new attraction."""
        pass
    
    @abstractmethod
    async def update(self, attraction: Attraction) -> Attraction:
        """Update existing attraction."""
        pass
    
    @abstractmethod
    async def list_active(self, skip: int = 0, limit: int = 100) -> List[Attraction]:
        """List active attractions with pagination."""
        pass
    
    @abstractmethod
    async def count_active(self) -> int:
        """Count active attractions."""
        pass

