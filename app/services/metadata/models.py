"""Data models for metadata service."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class PageType(str, Enum):
    """Enumeration of page types for metadata."""
    ATTRACTION = "attraction"
    CITY = "city"
    HOME = "home"


class AuthorType(str, Enum):
    """Enumeration of author types for metadata."""
    SYSTEM = "system"
    USER = "user"


@dataclass
class MetadataEntry:
    """Represents a single metadata entry for SEO/AEO optimization.
    
    Attributes:
        id: Unique identifier (attraction_id, city_id, or "home")
        page_name: Name of the attraction, city, or "Home"
        page_type: Type of page (attraction, city, or home)
        title: SEO title (50-60 characters recommended)
        meta_description: Meta description (150-160 characters recommended)
        hero_image_url: URL to hero image or collage
        author: Author type (system or user)
        created_at: Timestamp when entry was created
        updated_at: Timestamp when entry was last updated
    """
    id: str
    page_name: str
    page_type: PageType
    title: str
    meta_description: str
    hero_image_url: str
    author: AuthorType
    created_at: datetime
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def is_valid(self) -> bool:
        """Validate metadata entry structure and required fields.
        
        Returns:
            True if all required fields are present and non-null, False otherwise
        """
        return bool(
            self.id and
            self.id.strip() and
            self.page_name and
            self.page_name.strip() and
            self.page_type and
            self.title and
            self.title.strip() and
            self.meta_description and
            self.meta_description.strip() and
            self.hero_image_url and
            self.hero_image_url.strip() and
            self.author and
            self.created_at is not None and
            self.updated_at is not None
        )
    
    def to_dict(self) -> dict:
        """Convert metadata entry to dictionary for JSON serialization.
        
        Returns:
            Dictionary representation of the metadata entry
        """
        return {
            "id": self.id,
            "page_name": self.page_name,
            "page_type": self.page_type.value,
            "title": self.title,
            "meta_description": self.meta_description,
            "hero_image_url": self.hero_image_url,
            "author": self.author.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "MetadataEntry":
        """Create a MetadataEntry from a dictionary.
        
        Args:
            data: Dictionary containing metadata entry data
            
        Returns:
            MetadataEntry instance
            
        Raises:
            ValueError: If required fields are missing or invalid
        """
        required_fields = {
            "id", "page_name", "page_type", "title", 
            "meta_description", "hero_image_url", "author", "created_at"
        }
        
        missing_fields = required_fields - set(data.keys())
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        try:
            page_type = PageType(data["page_type"])
            author = AuthorType(data["author"])
            
            # Parse ISO format timestamps
            created_at = datetime.fromisoformat(data["created_at"])
            updated_at = datetime.fromisoformat(
                data.get("updated_at", datetime.utcnow().isoformat())
            )
            
            return cls(
                id=data["id"],
                page_name=data["page_name"],
                page_type=page_type,
                title=data["title"],
                meta_description=data["meta_description"],
                hero_image_url=data["hero_image_url"],
                author=author,
                created_at=created_at,
                updated_at=updated_at,
            )
        except (ValueError, KeyError) as e:
            raise ValueError(f"Invalid metadata entry data: {str(e)}")
