"""Metadata file manager for persistence and backup operations."""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from app.services.metadata.models import MetadataEntry
from app.services.metadata.constants import METADATA_CONSTANTS


class MetadataFileManager:
    """Manages persistence of metadata entries to JSON file with backup support.
    
    This manager handles:
    - Loading metadata from JSON file
    - Saving metadata to JSON file with validation
    - Adding and updating individual entries
    - Creating timestamped backups
    - Validating metadata structure and required fields
    """
    
    def __init__(self, metadata_file_path: Optional[str] = None, backup_dir_path: Optional[str] = None):
        """Initialize the metadata file manager.
        
        Args:
            metadata_file_path: Path to the metadata JSON file. If None, uses default location.
            backup_dir_path: Path to the backup directory. If None, uses default location.
        """
        if metadata_file_path is None:
            # Default to backend/data/metadata.json
            base_dir = Path(__file__).parent.parent.parent.parent  # backend directory
            metadata_file_path = str(base_dir / "data" / METADATA_CONSTANTS.METADATA_FILE_NAME)
        
        if backup_dir_path is None:
            # Default to backend/data/backups
            base_dir = Path(metadata_file_path).parent
            backup_dir_path = str(base_dir / METADATA_CONSTANTS.BACKUP_DIR_NAME)
        
        self.metadata_file_path = metadata_file_path
        self.backup_dir_path = backup_dir_path
        self.constants = METADATA_CONSTANTS
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Ensure that required directories exist."""
        # Create metadata file directory if it doesn't exist
        metadata_dir = Path(self.metadata_file_path).parent
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Create backup directory if it doesn't exist
        backup_dir = Path(self.backup_dir_path)
        backup_dir.mkdir(parents=True, exist_ok=True)
    
    def load_metadata_file(self) -> List[MetadataEntry]:
        """Load metadata entries from JSON file.
        
        Returns:
            List of MetadataEntry objects loaded from file. Returns empty list if file doesn't exist.
            
        Raises:
            ValueError: If JSON file is corrupted or contains invalid metadata
            IOError: If file cannot be read due to permission issues
        """
        metadata_path = Path(self.metadata_file_path)
        
        # If file doesn't exist, return empty list
        if not metadata_path.exists():
            return []
        
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Metadata JSON file is corrupted: {str(e)}")
        except IOError as e:
            raise IOError(f"Cannot read metadata file: {str(e)}")
        
        # Parse metadata entries
        entries = []
        metadata_list = data.get("metadata", [])
        
        for entry_data in metadata_list:
            try:
                entry = MetadataEntry.from_dict(entry_data)
                entries.append(entry)
            except ValueError as e:
                raise ValueError(f"Invalid metadata entry: {str(e)}")
        
        return entries
    
    def save_metadata_file(self, entries: List[MetadataEntry]) -> bool:
        """Save metadata entries to JSON file with validation.
        
        Creates a backup of the existing file before writing the new one.
        
        Args:
            entries: List of MetadataEntry objects to save
            
        Returns:
            True if save was successful
            
        Raises:
            ValueError: If any entry fails validation
            IOError: If file cannot be written due to permission issues
        """
        # Validate all entries before saving
        for entry in entries:
            if not self.validate_metadata(entry):
                raise ValueError(f"Invalid metadata entry: {entry.id}")
        
        # Create backup of existing file if it exists
        if Path(self.metadata_file_path).exists():
            self.create_backup()
        
        # Prepare data structure
        data = {
            "metadata": [entry.to_dict() for entry in entries]
        }
        
        # Write to file
        try:
            with open(self.metadata_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except IOError as e:
            raise IOError(f"Cannot write metadata file: {str(e)}")
        
        return True
    
    def add_entry(self, entry: MetadataEntry) -> bool:
        """Add a new metadata entry to the file.
        
        If an entry with the same ID already exists, it will be replaced.
        
        Args:
            entry: MetadataEntry to add
            
        Returns:
            True if entry was added successfully
            
        Raises:
            ValueError: If entry fails validation
            IOError: If file operations fail
        """
        if not self.validate_metadata(entry):
            raise ValueError(f"Invalid metadata entry: {entry.id}")
        
        # Load existing entries
        entries = self.load_metadata_file()
        
        # Remove entry with same ID if it exists
        entries = [e for e in entries if e.id != entry.id]
        
        # Add new entry
        entries.append(entry)
        
        # Save updated entries
        return self.save_metadata_file(entries)
    
    def update_entry(self, entry_id: str, entry: MetadataEntry) -> bool:
        """Update an existing metadata entry.
        
        Args:
            entry_id: ID of the entry to update
            entry: Updated MetadataEntry object
            
        Returns:
            True if entry was updated successfully
            
        Raises:
            ValueError: If entry fails validation or entry_id not found
            IOError: If file operations fail
        """
        if not self.validate_metadata(entry):
            raise ValueError(f"Invalid metadata entry: {entry.id}")
        
        # Load existing entries
        entries = self.load_metadata_file()
        
        # Find and update entry
        found = False
        for i, e in enumerate(entries):
            if e.id == entry_id:
                entries[i] = entry
                found = True
                break
        
        if not found:
            raise ValueError(f"Metadata entry with ID '{entry_id}' not found")
        
        # Save updated entries
        return self.save_metadata_file(entries)
    
    def create_backup(self) -> str:
        """Create a timestamped backup of the current metadata file.
        
        Returns:
            Path to the created backup file
            
        Raises:
            IOError: If backup cannot be created
        """
        metadata_path = Path(self.metadata_file_path)
        
        if not metadata_path.exists():
            raise IOError("Cannot create backup: metadata file does not exist")
        
        # Generate backup filename with timestamp
        timestamp = datetime.utcnow().strftime(self.constants.BACKUP_TIMESTAMP_FORMAT)
        backup_filename = f"metadata_{timestamp}.json"
        backup_path = Path(self.backup_dir_path) / backup_filename
        
        try:
            # Read existing file
            with open(metadata_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Write backup
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(content)
        except IOError as e:
            raise IOError(f"Cannot create backup: {str(e)}")
        
        return str(backup_path)
    
    def validate_metadata(self, entry: MetadataEntry) -> bool:
        """Validate metadata entry structure and required fields.
        
        Checks that:
        - All required fields are present and non-null
        - Field values are non-empty strings (where applicable)
        - Page type is valid
        - Author type is valid
        - Timestamps are valid datetime objects
        
        Args:
            entry: MetadataEntry to validate
            
        Returns:
            True if entry is valid, False otherwise
        """
        # Check if entry is valid using the model's validation method
        if not entry.is_valid():
            return False
        
        # Additional validation checks
        # Validate page type
        if entry.page_type.value not in self.constants.VALID_PAGE_TYPES:
            return False
        
        # Validate author type
        if entry.author.value not in self.constants.VALID_AUTHOR_TYPES:
            return False
        
        # Validate timestamps are datetime objects
        if not isinstance(entry.created_at, datetime):
            return False
        if not isinstance(entry.updated_at, datetime):
            return False
        
        # Validate created_at is not in the future
        if entry.created_at > datetime.utcnow():
            return False
        
        # Validate updated_at is not before created_at
        if entry.updated_at < entry.created_at:
            return False
        
        return True
