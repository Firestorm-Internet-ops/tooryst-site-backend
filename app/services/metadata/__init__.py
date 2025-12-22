"""Metadata service module for SEO and AEO enhancement."""
from app.services.metadata.models import MetadataEntry
from app.services.metadata.constants import (
    METADATA_CONSTANTS,
    PAGE_TYPES,
    AUTHOR_TYPES,
)
from app.services.metadata.generator import MetadataGenerator
from app.services.metadata.file_manager import MetadataFileManager

__all__ = [
    "MetadataEntry",
    "METADATA_CONSTANTS",
    "PAGE_TYPES",
    "AUTHOR_TYPES",
    "MetadataGenerator",
    "MetadataFileManager",
]
