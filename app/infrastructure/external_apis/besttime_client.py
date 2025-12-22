"""Best Time API client for fetching crowd and timing data."""
import os
import httpx
from typing import Optional, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BestTimeClient:
    """Client for Best Time API (crowd forecasting and venue analysis)."""
    
    BASE_URL = "https://besttime.app/api/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("BESTTIME_API_PRIVATE_KEY")
        if not self.api_key:
            logger.warning("BESTTIME_API_PRIVATE_KEY not set")
    
    async def search_venues(
        self,
        query: str,
        latitude: float,
        longitude: float,
        radius: int = 100,
        num: int = 40,
        fast: bool = True,
        format_: str = "raw",
    ) -> Optional[Dict[str, Any]]:
        """Kick off a BestTime venue search (background job)."""
        if not self.api_key:
            logger.error("Cannot search venues: API key missing")
            return None

        url = f"{self.BASE_URL}/venues/search"
        params = {
            "api_key_private": self.api_key,
            "q": query,
            "lat": latitude,
            "lng": longitude,
            "radius": radius,
            "num": num,
            "fast": "true" if fast else "false",
            "format": format_,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                if data.get("status") != "OK":
                    logger.error(f"BestTime search error: {data.get('status')} - {data.get('message')}")
                    return None
                return data
        except Exception as e:
            logger.error(f"Error performing BestTime venue search: {e}")
            return None

    async def get_search_progress(self, progress_url: str) -> Optional[Dict[str, Any]]:
        """Poll the BestTime search progress endpoint."""
        if not self.api_key:
            logger.error("Cannot poll search progress: API key missing")
            return None

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(progress_url)
                resp.raise_for_status()
                data = resp.json()
                return data
        except Exception as e:
            logger.error(f"Error polling BestTime progress: {e}")
            return None
    
    async def get_venue_forecast(self, venue_id: str) -> Optional[Dict[str, Any]]:
        """Fetch venue forecast by venue_id.
        
        Returns raw API response or None if error.
        """
        if not self.api_key:
            logger.error("Cannot fetch venue forecast: API key missing")
            return None
        
        url = f"{self.BASE_URL}/forecasts"
        params = {
            "api_key_private": self.api_key,
            "venue_id": venue_id
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") != "OK":
                    logger.error(f"Best Time API error: {data.get('status')} - {data.get('message')}")
                    return None
                
                return data
        except Exception as e:
            logger.error(f"Error fetching venue forecast: {e}")
            return None
    
    async def get_forecast(
        self,
        venue_name: str,
        venue_address: str
    ) -> Optional[Dict[str, Any]]:
        """Get forecast for venue by name and address using POST to /forecasts endpoint.
        
        This is the main method for fetching crowd forecast data.
        Returns raw API response or None if error.
        """
        if not self.api_key:
            logger.error("Cannot query venue: API key missing")
            return None
        
        url = f"{self.BASE_URL}/forecasts"
        
        params = {
            "api_key_private": self.api_key,
            "venue_name": venue_name,
            "venue_address": venue_address
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                logger.info(f"Requesting forecast for: {venue_name} at {venue_address}")
                # Use POST as per BestTime API docs
                response = await client.post(url, params=params)
                response.raise_for_status()
                result = response.json()
                
                if result.get("status") != "OK":
                    logger.error(f"Best Time API error: {result.get('status')} - {result.get('message')}")
                    return None
                
                logger.info(f"Successfully fetched forecast. Venue ID: {result.get('venue_info', {}).get('venue_id')}")
                return result
        except httpx.HTTPStatusError as e:
            # Only log first 500 chars to avoid giant HTML logs
            text = e.response.text[:500]
            logger.error(f"HTTP error creating forecast: {e.response.status_code} - {text}")
            return None
        except Exception as e:
            logger.error(f"Error creating forecast: {e}")
            return None

    async def new_forecast(
        self,
        venue_name: str,
        venue_address: str
    ) -> Optional[Dict[str, Any]]:
        """Create new forecast for venue by name and address.
        
        Returns raw API response or None if error.
        """
        if not self.api_key:
            logger.error("Cannot query venue: API key missing")
            return None
        
        # Correct URL: /forecasts (not /forecasts/new)
        url = f"{self.BASE_URL}/forecasts"
        
        params = {
            "api_key_private": self.api_key,
            "venue_name": venue_name,
            "venue_address": venue_address
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                logger.info(f"Requesting forecast for: {venue_name} at {venue_address}")
                # Use POST as per BestTime API docs
                response = await client.post(url, params=params)
                response.raise_for_status()
                result = response.json()
                
                if result.get("status") != "OK":
                    logger.error(f"Best Time API error: {result.get('status')} - {result.get('message')}")
                    return None
                
                logger.info(f"Successfully fetched forecast. Venue ID: {result.get('venue_info', {}).get('venue_id')}")
                return result
        except httpx.HTTPStatusError as e:
            # Only log first 500 chars to avoid giant HTML logs
            text = e.response.text[:500]
            logger.error(f"HTTP error creating forecast: {e.response.status_code} - {text}")
            return None
        except Exception as e:
            logger.error(f"Error creating forecast: {e}")
            return None
