"""Utility functions for Google Places API."""
import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def extract_place_id_from_link(link: str) -> Optional[str]:
    """
    Extract place_id from Google Maps link.

    Examples:
    - https://www.google.com/maps/place/?q=place_id:ChIJ... → ChIJ...
    - https://maps.google.com/?q=place_id:ChIJ... → ChIJ...
    - https://www.google.com/maps/place/?q=place_id=ChIJ... → ChIJ...

    Args:
        link: Google Maps URL

    Returns:
        Place ID or None if not found
    """
    if not link or 'place_id' not in link:
        return None

    try:
        # Match place_id with either : or = separator
        match = re.search(r'place_id[:=]([A-Za-z0-9_-]+)', link)
        if match:
            place_id = match.group(1)
            logger.debug(f"Extracted place_id: {place_id} from link: {link}")
            return place_id
        else:
            logger.debug(f"No place_id found in link: {link}")
            return None
    except Exception as e:
        logger.error(f"Error extracting place_id from link {link}: {e}")
        return None
