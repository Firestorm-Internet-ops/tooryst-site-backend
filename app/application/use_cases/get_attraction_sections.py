"""Use case: Get attraction sections data.
Follows Single Responsibility Principle - one use case, one responsibility."""
from typing import Optional
from app.domain.repositories.attraction_repository import AttractionRepository
from app.domain.repositories.city_repository import CityRepository
from app.application.dto.section_dto import AttractionSectionsDTO
from app.application.services.attraction_data_service import AttractionDataService


class GetAttractionSectionsUseCase:
    """Use case to get attraction sections data.
    
    Follows Dependency Inversion Principle - depends on repository abstractions.
    Follows Single Responsibility Principle - only handles getting sections data.
    """
    
    def __init__(
        self,
        attraction_repository: AttractionRepository,
        city_repository: CityRepository,
        data_service: AttractionDataService,
    ):
        self._attraction_repo = attraction_repository
        self._city_repo = city_repository
        self._data_service = data_service
    
    async def execute(self, slug: str) -> Optional[AttractionSectionsDTO]:
        """Execute use case to get attraction sections.
        
        Args:
            slug: Attraction slug
            
        Returns:
            AttractionSectionsDTO if found, None otherwise
        """
        # Get attraction
        attraction = await self._attraction_repo.get_by_slug(slug)
        if not attraction:
            return None
        
        # Get city
        city = await self._city_repo.get_by_id(attraction.city_id)
        city_name = city.name if city else ""
        country = city.country if city else None
        
        # Build DTO (for now, minimal - will be expanded with section data)
        return self._data_service.build_sections_dto(
            attraction=attraction,
            city_name=city_name,
            country=country,
        )

