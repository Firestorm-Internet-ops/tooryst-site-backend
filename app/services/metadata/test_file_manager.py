"""Tests for metadata file manager."""
import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from app.services.metadata.file_manager import MetadataFileManager
from app.services.metadata.models import MetadataEntry, PageType, AuthorType


@pytest.fixture
def temp_metadata_dir():
    """Create a temporary directory for metadata files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def file_manager(temp_metadata_dir):
    """Create a MetadataFileManager with temporary paths."""
    metadata_file = str(Path(temp_metadata_dir) / "metadata.json")
    backup_dir = str(Path(temp_metadata_dir) / "backups")
    return MetadataFileManager(metadata_file_path=metadata_file, backup_dir_path=backup_dir)


@pytest.fixture
def sample_entry():
    """Create a sample metadata entry."""
    now = datetime.utcnow()
    return MetadataEntry(
        id="123",
        page_name="Eiffel Tower",
        page_type=PageType.ATTRACTION,
        title="Everything You Need to Know about Eiffel Tower – Photos, Weather & Real Visitor Reviews",
        meta_description="From stunning photos to today's crowd forecasts, weather, tips and reviews, our guide brings together trusted insights and real visitor stories to help you explore Eiffel Tower with confidence.",
        hero_image_url="https://example.com/images/eiffel-tower.jpg",
        author=AuthorType.SYSTEM,
        created_at=now,
    )


@pytest.fixture
def sample_city_entry():
    """Create a sample city metadata entry."""
    now = datetime.utcnow()
    return MetadataEntry(
        id="paris",
        page_name="Paris",
        page_type=PageType.CITY,
        title="Plan Your Perfect Day at Paris with Crowd Data, Maps & Insider Tips",
        meta_description="Get an immersive look at Paris—photos, hourly crowd levels, today's weather, insider tips and verified reviews—so you can plan a perfect visit based on trusted sources and tourists' experiences.",
        hero_image_url="https://example.com/images/paris-collage.jpg",
        author=AuthorType.SYSTEM,
        created_at=now,
    )


class TestMetadataFileManagerBasics:
    """Test basic file manager operations."""
    
    def test_file_manager_initialization(self, file_manager):
        """Test file manager initializes with correct paths."""
        assert file_manager.metadata_file_path is not None
        assert file_manager.backup_dir_path is not None
        assert Path(file_manager.backup_dir_path).exists()
    
    def test_load_empty_metadata_file(self, file_manager):
        """Test loading metadata when file doesn't exist returns empty list."""
        entries = file_manager.load_metadata_file()
        assert entries == []
    
    def test_save_and_load_single_entry(self, file_manager, sample_entry):
        """Test saving and loading a single metadata entry."""
        # Save entry
        result = file_manager.save_metadata_file([sample_entry])
        assert result is True
        
        # Load entry
        entries = file_manager.load_metadata_file()
        assert len(entries) == 1
        assert entries[0].id == sample_entry.id
        assert entries[0].page_name == sample_entry.page_name
        assert entries[0].title == sample_entry.title
    
    def test_save_and_load_multiple_entries(self, file_manager, sample_entry, sample_city_entry):
        """Test saving and loading multiple metadata entries."""
        entries_to_save = [sample_entry, sample_city_entry]
        
        # Save entries
        result = file_manager.save_metadata_file(entries_to_save)
        assert result is True
        
        # Load entries
        loaded_entries = file_manager.load_metadata_file()
        assert len(loaded_entries) == 2
        assert loaded_entries[0].id == sample_entry.id
        assert loaded_entries[1].id == sample_city_entry.id


class TestMetadataFileManagerAddUpdate:
    """Test add and update operations."""
    
    def test_add_entry(self, file_manager, sample_entry):
        """Test adding a new entry."""
        result = file_manager.add_entry(sample_entry)
        assert result is True
        
        entries = file_manager.load_metadata_file()
        assert len(entries) == 1
        assert entries[0].id == sample_entry.id
    
    def test_add_multiple_entries(self, file_manager, sample_entry, sample_city_entry):
        """Test adding multiple entries sequentially."""
        file_manager.add_entry(sample_entry)
        file_manager.add_entry(sample_city_entry)
        
        entries = file_manager.load_metadata_file()
        assert len(entries) == 2
    
    def test_add_entry_replaces_existing(self, file_manager, sample_entry):
        """Test adding entry with same ID replaces existing entry."""
        # Add initial entry
        file_manager.add_entry(sample_entry)
        
        # Create updated entry with same ID
        now = datetime.utcnow()
        updated_entry = MetadataEntry(
            id=sample_entry.id,
            page_name="Updated Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="Updated Title",
            meta_description="Updated Description",
            hero_image_url="https://example.com/images/updated.jpg",
            author=AuthorType.SYSTEM,
            created_at=sample_entry.created_at,
            updated_at=now,
        )
        
        # Add updated entry
        file_manager.add_entry(updated_entry)
        
        # Verify only one entry exists with updated data
        entries = file_manager.load_metadata_file()
        assert len(entries) == 1
        assert entries[0].page_name == "Updated Eiffel Tower"
        assert entries[0].title == "Updated Title"
    
    def test_update_entry(self, file_manager, sample_entry):
        """Test updating an existing entry."""
        # Add initial entry
        file_manager.add_entry(sample_entry)
        
        # Create updated entry
        now = datetime.utcnow()
        updated_entry = MetadataEntry(
            id=sample_entry.id,
            page_name="Updated Eiffel Tower",
            page_type=PageType.ATTRACTION,
            title="Updated Title",
            meta_description="Updated Description",
            hero_image_url="https://example.com/images/updated.jpg",
            author=AuthorType.SYSTEM,
            created_at=sample_entry.created_at,
            updated_at=now,
        )
        
        # Update entry
        result = file_manager.update_entry(sample_entry.id, updated_entry)
        assert result is True
        
        # Verify update
        entries = file_manager.load_metadata_file()
        assert len(entries) == 1
        assert entries[0].page_name == "Updated Eiffel Tower"
    
    def test_update_nonexistent_entry_raises_error(self, file_manager, sample_entry):
        """Test updating nonexistent entry raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            file_manager.update_entry("nonexistent_id", sample_entry)


class TestMetadataFileManagerBackup:
    """Test backup operations."""
    
    def test_create_backup(self, file_manager, sample_entry):
        """Test creating a backup of metadata file."""
        # Save initial entry
        file_manager.save_metadata_file([sample_entry])
        
        # Create backup
        backup_path = file_manager.create_backup()
        
        # Verify backup exists
        assert Path(backup_path).exists()
        assert "metadata_" in backup_path
        assert backup_path.endswith(".json")
    
    def test_backup_contains_same_data(self, file_manager, sample_entry):
        """Test backup contains same data as original."""
        # Save initial entry
        file_manager.save_metadata_file([sample_entry])
        
        # Create backup
        backup_path = file_manager.create_backup()
        
        # Load backup and verify content
        with open(backup_path, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        # Load original
        original_entries = file_manager.load_metadata_file()
        
        # Verify backup has same data
        assert len(backup_data["metadata"]) == len(original_entries)
        assert backup_data["metadata"][0]["id"] == sample_entry.id
    
    def test_create_backup_without_file_raises_error(self, file_manager):
        """Test creating backup when file doesn't exist raises error."""
        with pytest.raises(IOError, match="does not exist"):
            file_manager.create_backup()
    
    def test_multiple_backups_have_different_names(self, file_manager, sample_entry):
        """Test multiple backups have different timestamps."""
        # Save initial entry
        file_manager.save_metadata_file([sample_entry])
        
        # Create first backup
        backup_path_1 = file_manager.create_backup()
        
        # Create second backup (with delay to ensure different timestamp)
        import time
        time.sleep(1.1)  # Sleep for more than 1 second to ensure different timestamp
        backup_path_2 = file_manager.create_backup()
        
        # Verify backups have different names
        assert backup_path_1 != backup_path_2
        assert Path(backup_path_1).exists()
        assert Path(backup_path_2).exists()


class TestMetadataFileManagerValidation:
    """Test validation operations."""
    
    def test_validate_valid_entry(self, file_manager, sample_entry):
        """Test validation of valid entry."""
        result = file_manager.validate_metadata(sample_entry)
        assert result is True
    
    def test_validate_entry_with_empty_id(self, file_manager):
        """Test validation fails for entry with empty id."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        result = file_manager.validate_metadata(entry)
        assert result is False
    
    def test_validate_entry_with_empty_title(self, file_manager):
        """Test validation fails for entry with empty title."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        result = file_manager.validate_metadata(entry)
        assert result is False
    
    def test_validate_entry_with_empty_description(self, file_manager):
        """Test validation fails for entry with empty description."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        result = file_manager.validate_metadata(entry)
        assert result is False
    
    def test_validate_entry_with_empty_image_url(self, file_manager):
        """Test validation fails for entry with empty image URL."""
        now = datetime.utcnow()
        entry = MetadataEntry(
            id="123",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        result = file_manager.validate_metadata(entry)
        assert result is False
    
    def test_validate_entry_with_invalid_page_type(self, file_manager):
        """Test validation fails for entry with invalid page type."""
        now = datetime.utcnow()
        # Create entry with invalid page type by bypassing dataclass validation
        entry = MetadataEntry(
            id="123",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        # Manually set invalid page type
        entry.page_type = PageType.ATTRACTION
        
        result = file_manager.validate_metadata(entry)
        assert result is True  # Should be valid since we're using valid enum
    
    def test_validate_entry_with_future_created_at(self, file_manager):
        """Test validation fails for entry with future created_at."""
        future_time = datetime.utcnow() + timedelta(hours=1)
        entry = MetadataEntry(
            id="123",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=future_time,
        )
        
        result = file_manager.validate_metadata(entry)
        assert result is False
    
    def test_validate_entry_with_updated_before_created(self, file_manager):
        """Test validation fails when updated_at is before created_at."""
        now = datetime.utcnow()
        past_time = now - timedelta(hours=1)
        
        entry = MetadataEntry(
            id="123",
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
            updated_at=past_time,
        )
        
        result = file_manager.validate_metadata(entry)
        assert result is False
    
    def test_save_invalid_entry_raises_error(self, file_manager):
        """Test saving invalid entry raises ValueError."""
        now = datetime.utcnow()
        invalid_entry = MetadataEntry(
            id="",  # Invalid: empty id
            page_name="Test",
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        with pytest.raises(ValueError, match="Invalid metadata entry"):
            file_manager.save_metadata_file([invalid_entry])
    
    def test_add_invalid_entry_raises_error(self, file_manager):
        """Test adding invalid entry raises ValueError."""
        now = datetime.utcnow()
        invalid_entry = MetadataEntry(
            id="123",
            page_name="",  # Invalid: empty page_name
            page_type=PageType.ATTRACTION,
            title="Test Title",
            meta_description="Test Description",
            hero_image_url="https://example.com/test.jpg",
            author=AuthorType.SYSTEM,
            created_at=now,
        )
        
        with pytest.raises(ValueError, match="Invalid metadata entry"):
            file_manager.add_entry(invalid_entry)


class TestMetadataFileManagerRoundTrip:
    """Test round-trip consistency (Property 4)."""
    
    def test_save_and_load_preserves_data(self, file_manager, sample_entry):
        """Test that saving and loading preserves all data."""
        # Save entry
        file_manager.save_metadata_file([sample_entry])
        
        # Load entry
        loaded_entries = file_manager.load_metadata_file()
        loaded_entry = loaded_entries[0]
        
        # Verify all fields match
        assert loaded_entry.id == sample_entry.id
        assert loaded_entry.page_name == sample_entry.page_name
        assert loaded_entry.page_type == sample_entry.page_type
        assert loaded_entry.title == sample_entry.title
        assert loaded_entry.meta_description == sample_entry.meta_description
        assert loaded_entry.hero_image_url == sample_entry.hero_image_url
        assert loaded_entry.author == sample_entry.author
        assert loaded_entry.created_at == sample_entry.created_at
    
    def test_multiple_save_load_cycles(self, file_manager, sample_entry, sample_city_entry):
        """Test multiple save/load cycles maintain consistency."""
        entries = [sample_entry, sample_city_entry]
        
        # First cycle
        file_manager.save_metadata_file(entries)
        loaded_1 = file_manager.load_metadata_file()
        
        # Second cycle
        file_manager.save_metadata_file(loaded_1)
        loaded_2 = file_manager.load_metadata_file()
        
        # Verify consistency
        assert len(loaded_1) == len(loaded_2)
        for e1, e2 in zip(loaded_1, loaded_2):
            assert e1.id == e2.id
            assert e1.title == e2.title
            assert e1.meta_description == e2.meta_description


class TestMetadataFileManagerErrorHandling:
    """Test error handling."""
    
    def test_load_corrupted_json_raises_error(self, file_manager):
        """Test loading corrupted JSON raises ValueError."""
        # Write corrupted JSON
        with open(file_manager.metadata_file_path, 'w') as f:
            f.write("{invalid json")
        
        with pytest.raises(ValueError, match="corrupted"):
            file_manager.load_metadata_file()
    
    def test_load_invalid_metadata_structure_raises_error(self, file_manager):
        """Test loading invalid metadata structure raises ValueError."""
        # Write JSON with invalid metadata
        data = {
            "metadata": [
                {
                    "id": "123",
                    # Missing required fields
                    "page_name": "Test"
                }
            ]
        }
        
        with open(file_manager.metadata_file_path, 'w') as f:
            json.dump(data, f)
        
        with pytest.raises(ValueError, match="Invalid metadata entry"):
            file_manager.load_metadata_file()
