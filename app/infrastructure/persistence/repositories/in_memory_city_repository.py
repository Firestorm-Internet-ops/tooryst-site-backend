"""In-memory implementation of CityRepository for testing.
Follows Liskov Substitution Principle - can replace any CityRepository."""
from typing import Optional, List, Dict
from app.domain.entities.city import City
from app.domain.repositories.city_repository import CityRepository


class InMemoryCityRepository(CityRepository):
    """In-memory implementation for testing.
    
    Follows Liskov Substitution Principle - fully implements interface.
    Follows Single Responsibility Principle - only handles city storage.
    """
    
    def __init__(self):
        self._cities: Dict[int, City] = {}
        self._by_slug: Dict[str, City] = {}
        self._next_id = 1
    
    async def get_by_id(self, city_id: int) -> Optional[City]:
        """Get city by ID."""
        return self._cities.get(city_id)
    
    async def get_by_slug(self, slug: str) -> Optional[City]:
        """Get city by slug."""
        return self._by_slug.get(slug)
    
    async def create(self, city: City) -> City:
        """Create new city."""
        if not city.is_valid():
            raise ValueError("Invalid city")
        
        # Assign ID if not set
        if city.id is None:
            city.id = self._next_id
            self._next_id += 1
        
        # Check for duplicate slug
        if city.slug in self._by_slug:
            raise ValueError(f"City with slug '{city.slug}' already exists")
        
        # Store
        self._cities[city.id] = city
        self._by_slug[city.slug] = city
        
        return city
    
    async def list_all(self, skip: int = 0, limit: int = 100) -> List[City]:
        """List all cities with pagination."""
        cities = list(self._cities.values())
        return cities[skip:skip + limit]
    
    async def count_all(self) -> int:
        """Count all cities."""
        return len(self._cities)

