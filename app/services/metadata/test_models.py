"""Tests for metadata models."""
import pytest
from datetime import datetime
from app.services.metadata.models import MetadataEntry, PageType, AuthorType


class TestMetadataEntry:
    """Test MetadataEntry dataclass."""
    
    def test_metadata_entry_creation(self):
        """Test creating a valid metadata entry."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            meta_description="From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            hero_image_url="https://example.com/images/eiffel-tower.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        assert entry.id == "123"
        assert entry.page_name == "Eiffel Tower"
        assert entry.page_type == PageType.ATTRACTION
        assert entry.author == AuthorType.SYSTEM
        assert entry.created_at == now
    
    def test_metadata_entry_is_valid(self):
        """Test validation of metadata entry."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            meta_description="From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            hero_image_url="https://example.com/images/eiffel-tower.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        assert entry.is_valid() is True
    
    def test_metadata_entry_invalid_missing_id(self):
        """Test validation fails when id is missing."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="",
            page_name="Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            meta_description="From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            hero_image_url="https://example.com/images/eiffel-tower.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        assert entry.is_valid() is False
    
    def test_metadata_entry_invalid_missing_title(self):
        """Test validation fails when title is missing."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="",
            meta_description="From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            hero_image_url="https://example.com/images/eiffel-tower.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        assert entry.is_valid() is False
    
    def test_metadata_entry_to_dict(self):
        """Test converting metadata entry to dictionary."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            meta_description="From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            hero_image_url="https://example.com/images/eiffel-tower.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        data = entry.to_dict()
        
        assert data["id"] == "123"
        assert data["page_name"] == "Eiffel Tower"
        assert data["page_type"] == "attraction"
        assert data["author"] == "system"
        assert "created_at" in data
        assert "updated_at" in data
    
    def test_metadata_entry_from_dict(self):
        """Test creating metadata entry from dictionary."""
        now = datetime.utcnow()
        data = {
            "id": "123",
            "page_name": "Eiffel Tower",
            "page_type": "attraction",
            "title": "Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            "meta_description": "From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            "hero_image_url": "https://example.com/images/eiffel-tower.jpg",
            "author": "system",
            "created_at": now.isoformat(),
        }
        
        entry = MetadataEntry.from_dict(data)
        
        assert entry.id == "123"
        assert entry.page_name == "Eiffel Tower"
        assert entry.page_type == PageType.ATTRACTION
        assert entry.author == AuthorType.SYSTEM
    
    def test_metadata_entry_from_dict_missing_field(self):
        """Test from_dict raises error when required field is missing."""
        data = {
            "id": "123",
            "page_name": "Eiffel Tower",
            "page_type": "attraction",
            "title": "Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            # Missing meta_description
            "hero_image_url": "https://example.com/images/eiffel-tower.jpg",
            "author": "system",
            "created_at": datetime.utcnow().isoformat(),
        }
        
        with pytest.raises(ValueError, match="Missing required fields"):
            MetadataEntry.from_dict(data)
    
    def test_metadata_entry_from_dict_invalid_page_type(self):
        """Test from_dict raises error when page_type is invalid."""
        data = {
            "id": "123",
            "page_name": "Eiffel Tower",
            "page_type": "invalid_type",
            "title": "Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
            "meta_description": "From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
            "hero_image_url": "https://example.com/images/eiffel-tower.jpg",
            "author": "system",
            "created_at": datetime.utcnow().isoformat(),
        }
        
        with pytest.raises(ValueError, match="Invalid metadata entry data"):
            MetadataEntry.from_dict(data)
    
    def test_page_type_enum(self):
        """Test PageType enum values."""
        assert PageType.ATTRACTION.value == "attraction"
        assert PageType.CITY.value == "city"
        assert PageType.HOME.value == "home"
    
    def test_author_type_enum(self):
        """Test AuthorType enum values."""
        assert AuthorType.SYSTEM.value == "system"
        assert AuthorType.USER.value == "user"
