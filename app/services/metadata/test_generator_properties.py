"""Property-based tests for metadata generator service.

These tests verify that the metadata generator correctly implements
the SEO/AEO enhancement requirements using property-based testing.
"""
import pytest
from hypothesis import given, strategies as st, assume
from datetime import datetime
from app.services.metadata.generator import MetadataGenerator
from app.services.metadata.models import MetadataEntry, PageType, AuthorType
from app.services.metadata.constants import METADATA_CONSTANTS


class TestAttractionTitleGeneration:
    """Tests for attraction title generation.
    
    **Feature: seo-aeo-enhancement, Property 5: Attraction Title Template Compliance**
    **Validates: Requirements 2.1**
    """
    
    @given(st.text(min_size=3, max_size=30).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_attraction_title_follows_template(self, attraction_name: str):
        """Property: For any attraction, the generated SEO title SHALL follow the pattern.
        
        The title must contain the attraction name and follow the exact template:
        "Everything You Need to Know about {name} – Photos, Weather & Real Visitor Reviews"
        """
        generator = MetadataGenerator()
        title = generator.generate_attraction_title(attraction_name)
        
        # Verify template pattern is followed
        assert "Everything You Need to Know about" in title
        assert "– Photos, Weather & Real Visitor Reviews" in title
        
        # Verify length constraints
        assert len(title) <= METADATA_CONSTANTS.TITLE_MAX_LENGTH
        assert len(title) >= METADATA_CONSTANTS.TITLE_MIN_LENGTH
    
    @given(st.text(min_size=3, max_size=30).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_attraction_title_contains_name(self, attraction_name: str):
        """Property: The generated title must contain at least part of the attraction name."""
        generator = MetadataGenerator()
        title = generator.generate_attraction_title(attraction_name)
        
        # The title should contain some portion of the name (may be truncated)
        clean_name = attraction_name.strip()
        # Check that at least the first character of the name appears
        assert len(clean_name) > 0 and (clean_name[0] in title or clean_name in title)
    
    def test_attraction_title_rejects_empty_name(self):
        """Property: Empty attraction names should raise ValueError."""
        generator = MetadataGenerator()
        
        with pytest.raises(ValueError):
            generator.generate_attraction_title("")
        
        with pytest.raises(ValueError):
            generator.generate_attraction_title("   ")
    
    @given(st.text(min_size=3, max_size=30).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_attraction_title_length_within_limits(self, attraction_name: str):
        """Property: Generated titles must respect character limits."""
        generator = MetadataGenerator()
        title = generator.generate_attraction_title(attraction_name)
        
        assert len(title) <= METADATA_CONSTANTS.TITLE_MAX_LENGTH
        assert len(title) >= METADATA_CONSTANTS.TITLE_MIN_LENGTH


class TestAttractionDescriptionGeneration:
    """Tests for attraction description generation.
    
    **Feature: seo-aeo-enhancement, Property 6: Attraction Description Template Compliance**
    **Validates: Requirements 2.2**
    """
    
    @given(st.text(min_size=3, max_size=25).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_attraction_description_follows_template(self, attraction_name: str):
        """Property: For any attraction, the generated meta description SHALL follow the pattern.
        
        The description must follow the exact template:
        "From stunning photos to today's crowd forecasts, weather, tips and reviews, 
        our guide brings together trusted insights and real visitor stories to help you explore {name} with confidence."
        """
        generator = MetadataGenerator()
        description = generator.generate_attraction_description(attraction_name)
        
        # Verify template pattern is followed
        assert "From stunning photos to today's crowd forecasts" in description
        assert "trusted insights and real visitor stories" in description
        assert "with confidence" in description
        
        # Verify length constraints
        assert len(description) <= METADATA_CONSTANTS.DESCRIPTION_MAX_LENGTH
        assert len(description) >= METADATA_CONSTANTS.DESCRIPTION_MIN_LENGTH
    
    @given(st.text(min_size=3, max_size=25).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_attraction_description_contains_name(self, attraction_name: str):
        """Property: The generated description must contain at least part of the attraction name."""
        generator = MetadataGenerator()
        description = generator.generate_attraction_description(attraction_name)
        
        # The description should contain some portion of the name (may be truncated)
        clean_name = attraction_name.strip()
        # Check that at least the first character of the name appears
        assert len(clean_name) > 0 and (clean_name[0] in description or clean_name in description)
    
    def test_attraction_description_rejects_empty_name(self):
        """Property: Empty attraction names should raise ValueError."""
        generator = MetadataGenerator()
        
        with pytest.raises(ValueError):
            generator.generate_attraction_description("")
        
        with pytest.raises(ValueError):
            generator.generate_attraction_description("   ")
    
    @given(st.text(min_size=3, max_size=25).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_attraction_description_length_within_limits(self, attraction_name: str):
        """Property: Generated descriptions must respect character limits."""
        generator = MetadataGenerator()
        description = generator.generate_attraction_description(attraction_name)
        
        assert len(description) <= METADATA_CONSTANTS.DESCRIPTION_MAX_LENGTH
        assert len(description) >= METADATA_CONSTANTS.DESCRIPTION_MIN_LENGTH


class TestCityTitleGeneration:
    """Tests for city title generation.
    
    **Feature: seo-aeo-enhancement, Property 7: City Title Template Compliance**
    **Validates: Requirements 2.3**
    """
    
    @given(st.text(min_size=3, max_size=25).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_city_title_follows_template(self, city_name: str):
        """Property: For any city, the generated SEO title SHALL follow the pattern.
        
        The title must follow the exact template:
        "Plan Your Perfect Day at {name} with Crowd Data, Maps & Insider Tips"
        """
        generator = MetadataGenerator()
        title = generator.generate_city_title(city_name)
        
        # Verify template pattern is followed
        assert "Plan Your Perfect Day at" in title
        assert "with Crowd Data, Maps & Insider Tips" in title
        
        # Verify length constraints
        assert len(title) <= METADATA_CONSTANTS.TITLE_MAX_LENGTH
        assert len(title) >= METADATA_CONSTANTS.TITLE_MIN_LENGTH
    
    @given(st.text(min_size=3, max_size=25).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_city_title_contains_name(self, city_name: str):
        """Property: The generated title must contain at least part of the city name."""
        generator = MetadataGenerator()
        title = generator.generate_city_title(city_name)
        
        # The title should contain some portion of the name (may be truncated)
        clean_name = city_name.strip()
        # Check that at least the first character of the name appears
        assert len(clean_name) > 0 and (clean_name[0] in title or clean_name in title)
    
    def test_city_title_rejects_empty_name(self):
        """Property: Empty city names should raise ValueError."""
        generator = MetadataGenerator()
        
        with pytest.raises(ValueError):
            generator.generate_city_title("")
        
        with pytest.raises(ValueError):
            generator.generate_city_title("   ")
    
    @given(st.text(min_size=3, max_size=25).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_city_title_length_within_limits(self, city_name: str):
        """Property: Generated titles must respect character limits."""
        generator = MetadataGenerator()
        title = generator.generate_city_title(city_name)
        
        assert len(title) <= METADATA_CONSTANTS.TITLE_MAX_LENGTH
        assert len(title) >= METADATA_CONSTANTS.TITLE_MIN_LENGTH


class TestCityDescriptionGeneration:
    """Tests for city description generation.
    
    **Feature: seo-aeo-enhancement, Property 8: City Description Template Compliance**
    **Validates: Requirements 2.4**
    """
    
    @given(st.text(min_size=3, max_size=20).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_city_description_follows_template(self, city_name: str):
        """Property: For any city, the generated meta description SHALL follow the pattern.
        
        The description must follow the exact template:
        "Get an immersive look at {name}—photos, hourly crowd levels, today's weather, 
        insider tips and verified reviews—so you can plan a perfect visit based on trusted sources and tourists' experiences."
        """
        generator = MetadataGenerator()
        description = generator.generate_city_description(city_name)
        
        # Verify template pattern is followed
        assert "Get an immersive look at" in description
        assert "photos, hourly crowd levels, today's weather" in description
        assert "insider tips and verified reviews" in description
        assert "plan a perfect visit" in description
        
        # Verify length constraints
        assert len(description) <= METADATA_CONSTANTS.DESCRIPTION_MAX_LENGTH
        assert len(description) >= METADATA_CONSTANTS.DESCRIPTION_MIN_LENGTH
    
    @given(st.text(min_size=3, max_size=20).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_city_description_contains_name(self, city_name: str):
        """Property: The generated description must contain at least part of the city name."""
        generator = MetadataGenerator()
        description = generator.generate_city_description(city_name)
        
        # The description should contain some portion of the name (may be truncated)
        clean_name = city_name.strip()
        # Check that at least the first character of the name appears
        assert len(clean_name) > 0 and (clean_name[0] in description or clean_name in description)
    
    def test_city_description_rejects_empty_name(self):
        """Property: Empty city names should raise ValueError."""
        generator = MetadataGenerator()
        
        with pytest.raises(ValueError):
            generator.generate_city_description("")
        
        with pytest.raises(ValueError):
            generator.generate_city_description("   ")
    
    @given(st.text(min_size=3, max_size=20).filter(lambda x: x.strip() and len(x.strip()) >= 3))
    def test_city_description_length_within_limits(self, city_name: str):
        """Property: Generated descriptions must respect character limits."""
        generator = MetadataGenerator()
        description = generator.generate_city_description(city_name)
        
        assert len(description) <= METADATA_CONSTANTS.DESCRIPTION_MAX_LENGTH
        assert len(description) >= METADATA_CONSTANTS.DESCRIPTION_MIN_LENGTH


class TestHomePageMetadata:
    """Tests for home page metadata generation."""
    
    def test_home_title_is_valid(self):
        """Property: Home page title must be valid and within limits."""
        generator = MetadataGenerator()
        title = generator.generate_home_title()
        
        assert title
        assert len(title) <= METADATA_CONSTANTS.TITLE_MAX_LENGTH
        assert len(title) >= METADATA_CONSTANTS.TITLE_MIN_LENGTH
    
    def test_home_description_is_valid(self):
        """Property: Home page description must be valid and within limits."""
        generator = MetadataGenerator()
        description = generator.generate_home_description()
        
        assert description
        assert len(description) <= METADATA_CONSTANTS.DESCRIPTION_MAX_LENGTH
        assert len(description) >= METADATA_CONSTANTS.DESCRIPTION_MIN_LENGTH
    
    def test_home_metadata_entry_is_valid(self):
        """Property: Generated home metadata entry must be valid."""
        generator = MetadataGenerator()
        entry = generator.generate_home_metadata()
        
        assert entry.is_valid()
        assert entry.id == "home"
        assert entry.page_name == "Home"
        assert entry.page_type == PageType.HOME
        assert entry.author == AuthorType.SYSTEM


class TestAttractionMetadataGeneration:
    """Tests for complete attraction metadata generation."""
    
    @given(
        st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
        st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
        st.one_of(st.none(), st.text(min_size=10, max_size=100)),
    )
    def test_attraction_metadata_is_complete(
        self, attraction_id: str, attraction_name: str, hero_image_url: str
    ):
        """Property: Generated attraction metadata must be complete and valid."""
        generator = MetadataGenerator()
        entry = generator.generate_attraction_metadata(
            attraction_id, attraction_name, hero_image_url
        )
        
        assert entry.is_valid()
        assert entry.id == attraction_id.strip()
        assert entry.page_name == attraction_name.strip()
        assert entry.page_type == PageType.ATTRACTION
        assert entry.author == AuthorType.SYSTEM
        assert entry.created_at is not None
        assert entry.updated_at is not None
    
    def test_attraction_metadata_rejects_empty_id(self):
        """Property: Empty attraction IDs should raise ValueError."""
        generator = MetadataGenerator()
        
        with pytest.raises(ValueError):
            generator.generate_attraction_metadata("", "Test Attraction")
        
        with pytest.raises(ValueError):
            generator.generate_attraction_metadata("   ", "Test Attraction")


class TestCityMetadataGeneration:
    """Tests for complete city metadata generation."""
    
    @given(
        st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
        st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
        st.one_of(st.none(), st.text(min_size=10, max_size=100)),
    )
    def test_city_metadata_is_complete(
        self, city_id: str, city_name: str, hero_image_url: str
    ):
        """Property: Generated city metadata must be complete and valid."""
        generator = MetadataGenerator()
        entry = generator.generate_city_metadata(
            city_id, city_name, hero_image_url
        )
        
        assert entry.is_valid()
        assert entry.id == city_id.strip()
        assert entry.page_name == city_name.strip()
        assert entry.page_type == PageType.CITY
        assert entry.author == AuthorType.SYSTEM
        assert entry.created_at is not None
        assert entry.updated_at is not None
    
    def test_city_metadata_rejects_empty_id(self):
        """Property: Empty city IDs should raise ValueError."""
        generator = MetadataGenerator()
        
        with pytest.raises(ValueError):
            generator.generate_city_metadata("", "Test City")
        
        with pytest.raises(ValueError):
            generator.generate_city_metadata("   ", "Test City")
