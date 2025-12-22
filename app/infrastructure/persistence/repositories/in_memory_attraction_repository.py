"""In-memory implementation of AttractionRepository for testing.
Follows Liskov Substitution Principle - can replace any AttractionRepository."""
from typing import Optional, List, Dict
from app.domain.entities.attraction import Attraction
from app.domain.repositories.attraction_repository import AttractionRepository


class InMemoryAttractionRepository(AttractionRepository):
    """In-memory implementation for testing.
    
    Follows Liskov Substitution Principle - fully implements interface.
    Follows Single Responsibility Principle - only handles attraction storage.
    """
    
    def __init__(self):
        self._attractions: Dict[int, Attraction] = {}
        self._by_slug: Dict[str, Attraction] = {}
        self._next_id = 1
    
    async def get_by_id(self, attraction_id: int) -> Optional[Attraction]:
        """Get attraction by ID."""
        return self._attractions.get(attraction_id)
    
    async def get_by_slug(self, slug: str) -> Optional[Attraction]:
        """Get attraction by slug."""
        return self._by_slug.get(slug)
    
    async def create(self, attraction: Attraction) -> Attraction:
        """Create new attraction."""
        if not attraction.is_valid():
            raise ValueError("Invalid attraction")
        
        # Assign ID if not set
        if attraction.id is None:
            attraction.id = self._next_id
            self._next_id += 1
        
        # Check for duplicate slug
        if attraction.slug in self._by_slug:
            raise ValueError(f"Attraction with slug '{attraction.slug}' already exists")
        
        # Store
        self._attractions[attraction.id] = attraction
        self._by_slug[attraction.slug] = attraction
        
        return attraction
    
    async def update(self, attraction: Attraction) -> Attraction:
        """Update existing attraction."""
        if attraction.id is None or attraction.id not in self._attractions:
            raise ValueError("Attraction not found")
        
        if not attraction.is_valid():
            raise ValueError("Invalid attraction")
        
        # Update slug mapping if changed
        old_attraction = self._attractions[attraction.id]
        if old_attraction.slug != attraction.slug:
            del self._by_slug[old_attraction.slug]
            self._by_slug[attraction.slug] = attraction
        
        self._attractions[attraction.id] = attraction
        return attraction
    
    async def list_active(self, skip: int = 0, limit: int = 100) -> List[Attraction]:
        """List all attractions with pagination."""
        all_attractions = list(self._attractions.values())
        return all_attractions[skip:skip + limit]
    
    async def count_active(self) -> int:
        """Count all attractions."""
        return len(self._attractions)

