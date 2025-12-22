"""Metadata generator service for creating SEO/AEO content."""
from datetime import datetime
from typing import Optional, Dict, Any
from app.services.metadata.models import MetadataEntry, PageType, AuthorType
from app.services.metadata.constants import METADATA_CONSTANTS


class MetadataGenerator:
    """Service for generating SEO titles, descriptions, and structured data.
    
    This service creates metadata entries for attractions, cities, and home page
    following predefined templates and patterns.
    """
    
    def __init__(self):
        """Initialize the metadata generator."""
        self.constants = METADATA_CONSTANTS
    
    def generate_attraction_title(self, attraction_name: str) -> str:
        """Generate SEO title for an attraction.
        
        Follows the pattern: "Everything You Need to Know about {name} – Photos, Weather & Real Visitor Reviews"
        
        Args:
            attraction_name: Name of the attraction
            
        Returns:
            Generated SEO title
            
        Raises:
            ValueError: If attraction_name is empty or None
        """
        if not attraction_name or not attraction_name.strip():
            raise ValueError("Attraction name cannot be empty")
        
        clean_name = attraction_name.strip()
        title = self.constants.ATTRACTION_TITLE_TEMPLATE.format(name=clean_name)
        
        # Validate length and truncate if necessary
        while len(title) > self.constants.TITLE_MAX_LENGTH and len(clean_name) > 0:
            clean_name = clean_name[:-1].strip()
            if clean_name:
                title = self.constants.ATTRACTION_TITLE_TEMPLATE.format(name=clean_name)
            else:
                # If name becomes empty, use a minimal name
                title = self.constants.ATTRACTION_TITLE_TEMPLATE.format(name="...")
                break
        
        return title
    
    def generate_attraction_description(self, attraction_name: str) -> str:
        """Generate meta description for an attraction.
        
        Follows the pattern: "From stunning photos to today's crowd forecasts, weather, tips and reviews, 
        our guide brings together trusted insights and real visitor stories to help you explore {name} with confidence."
        
        Args:
            attraction_name: Name of the attraction
            
        Returns:
            Generated meta description
            
        Raises:
            ValueError: If attraction_name is empty or None
        """
        if not attraction_name or not attraction_name.strip():
            raise ValueError("Attraction name cannot be empty")
        
        clean_name = attraction_name.strip()
        description = self.constants.ATTRACTION_DESCRIPTION_TEMPLATE.format(name=clean_name)
        
        # Validate length and truncate if necessary
        while len(description) > self.constants.DESCRIPTION_MAX_LENGTH and len(clean_name) > 0:
            clean_name = clean_name[:-1].strip()
            if clean_name:
                description = self.constants.ATTRACTION_DESCRIPTION_TEMPLATE.format(name=clean_name)
            else:
                # If name becomes empty, use a minimal name
                description = self.constants.ATTRACTION_DESCRIPTION_TEMPLATE.format(name="...")
                break
        
        return description
    
    def generate_city_title(self, city_name: str) -> str:
        """Generate SEO title for a city.
        
        Follows the pattern: "Plan Your Perfect Day at {name} with Crowd Data, Maps & Insider Tips"
        
        Args:
            city_name: Name of the city
            
        Returns:
            Generated SEO title
            
        Raises:
            ValueError: If city_name is empty or None
        """
        if not city_name or not city_name.strip():
            raise ValueError("City name cannot be empty")
        
        clean_name = city_name.strip()
        title = self.constants.CITY_TITLE_TEMPLATE.format(name=clean_name)
        
        # Validate length and truncate if necessary
        while len(title) > self.constants.TITLE_MAX_LENGTH and len(clean_name) > 0:
            clean_name = clean_name[:-1].strip()
            if clean_name:
                title = self.constants.CITY_TITLE_TEMPLATE.format(name=clean_name)
            else:
                # If name becomes empty, use a minimal name
                title = self.constants.CITY_TITLE_TEMPLATE.format(name="...")
                break
        
        return title
    
    def generate_city_description(self, city_name: str) -> str:
        """Generate meta description for a city.
        
        Follows the pattern: "Get an immersive look at {name}—photos, hourly crowd levels, today's weather, 
        insider tips and verified reviews—so you can plan a perfect visit based on trusted sources and tourists' experiences."
        
        Args:
            city_name: Name of the city
            
        Returns:
            Generated meta description
            
        Raises:
            ValueError: If city_name is empty or None
        """
        if not city_name or not city_name.strip():
            raise ValueError("City name cannot be empty")
        
        clean_name = city_name.strip()
        description = self.constants.CITY_DESCRIPTION_TEMPLATE.format(name=clean_name)
        
        # Validate length and truncate if necessary
        while len(description) > self.constants.DESCRIPTION_MAX_LENGTH and len(clean_name) > 0:
            clean_name = clean_name[:-1].strip()
            if clean_name:
                description = self.constants.CITY_DESCRIPTION_TEMPLATE.format(name=clean_name)
            else:
                # If name becomes empty, use a minimal name
                description = self.constants.CITY_DESCRIPTION_TEMPLATE.format(name="...")
                break
        
        return description
    
    def generate_home_title(self) -> str:
        """Generate SEO title for the home page.
        
        Returns:
            Generated SEO title for home page
        """
        return self.constants.HOME_TITLE
    
    def generate_home_description(self) -> str:
        """Generate meta description for the home page.
        
        Returns:
            Generated meta description for home page
        """
        return self.constants.HOME_DESCRIPTION
    
    def generate_attraction_metadata(
        self,
        attraction_id: str,
        attraction_name: str,
        hero_image_url: Optional[str] = None,
        author: AuthorType = AuthorType.SYSTEM,
    ) -> MetadataEntry:
        """Generate complete metadata entry for an attraction.
        
        Args:
            attraction_id: Unique identifier for the attraction
            attraction_name: Name of the attraction
            hero_image_url: URL to the hero image (uses placeholder if not provided)
            author: Author type (default: system)
            
        Returns:
            MetadataEntry with generated metadata
            
        Raises:
            ValueError: If required parameters are invalid
        """
        if not attraction_id or not attraction_id.strip():
            raise ValueError("Attraction ID cannot be empty")
        
        title = self.generate_attraction_title(attraction_name)
        description = self.generate_attraction_description(attraction_name)
        image_url = hero_image_url or self.constants.PLACEHOLDER_IMAGE_URL
        now = datetime.utcnow()
        
        return MetadataEntry(
            id=attraction_id.strip(),
            page_name=attraction_name.strip(),
            page_type=PageType.ATTRACTION,
            title=title,
            meta_description=description,
            hero_image_url=image_url,
            author=author,
            created_at=now,
            updated_at=now,
        )
    
    def generate_city_metadata(
        self,
        city_id: str,
        city_name: str,
        hero_image_url: Optional[str] = None,
        author: AuthorType = AuthorType.SYSTEM,
    ) -> MetadataEntry:
        """Generate complete metadata entry for a city.
        
        Args:
            city_id: Unique identifier for the city
            city_name: Name of the city
            hero_image_url: URL to the city collage image (uses placeholder if not provided)
            author: Author type (default: system)
            
        Returns:
            MetadataEntry with generated metadata
            
        Raises:
            ValueError: If required parameters are invalid
        """
        if not city_id or not city_id.strip():
            raise ValueError("City ID cannot be empty")
        
        title = self.generate_city_title(city_name)
        description = self.generate_city_description(city_name)
        image_url = hero_image_url or self.constants.PLACEHOLDER_IMAGE_URL
        now = datetime.utcnow()
        
        return MetadataEntry(
            id=city_id.strip(),
            page_name=city_name.strip(),
            page_type=PageType.CITY,
            title=title,
            meta_description=description,
            hero_image_url=image_url,
            author=author,
            created_at=now,
            updated_at=now,
        )
    
    def generate_home_metadata(
        self,
        author: AuthorType = AuthorType.SYSTEM,
    ) -> MetadataEntry:
        """Generate complete metadata entry for the home page.
        
        Args:
            author: Author type (default: system)
            
        Returns:
            MetadataEntry with generated home page metadata
        """
        title = self.generate_home_title()
        description = self.generate_home_description()
        now = datetime.utcnow()
        
        return MetadataEntry(
            id="home",
            page_name="Home",
            page_type=PageType.HOME,
            title=title,
            meta_description=description,
            hero_image_url=self.constants.PLACEHOLDER_IMAGE_URL,
            author=author,
            created_at=now,
            updated_at=now,
        )
