"""Fetcher interfaces for external data sources (SOLID-friendly).

Each fetcher returns data already shaped to our domain needs; implementations
can be swapped without changing application logic.
"""
from typing import Protocol, Optional, List, Dict, Any


class HeroImagesFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return {"images": [{url, alt, position}]} or None."""


class MapSnapshotFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return map data: latitude, longitude, address, static_map_image_url, maps_link_url, zoom_level."""


class BestTimeFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return best time card/section data (single day + chart_json/reason)."""


class WeatherFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return weather data aligned to weather card fields."""


class SocialVideoFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return primary social video data (platform, title, embed_url, thumbnail_url, source_url, duration_seconds)."""


class ReviewsFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return review aggregate and list for sections."""


class TipsFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return tips split into safety/insider arrays."""


class AboutFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return about fields: short_description, recommended_duration_minutes, highlights."""


class NearbyFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[List[Dict[str, Any]]]:
        """Return list of nearby attractions with fixture-aligned keys."""


class VisitorInfoFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return visitor info: contact_items, opening_hours, accessibility_info, best_season."""


class AudienceProfileFetcher(Protocol):
    async def fetch(self, attraction_id: int, place_id: Optional[str]) -> Optional[List[Dict[str, Any]]]:
        """Return audience profiles: audience_type, description, emoji."""


