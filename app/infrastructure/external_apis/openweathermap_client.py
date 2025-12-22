"""OpenWeatherMap API client for weather data."""
import os
import httpx
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class OpenWeatherMapClient:
    """Client for OpenWeatherMap API (Current Weather & Forecast)."""
    
    BASE_URL = "https://api.openweathermap.org/data/2.5"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENWEATHERMAP_API_KEY")
        if not self.api_key:
            logger.warning("OPENWEATHERMAP_API_KEY not set")
    
    async def get_current_weather(
        self,
        latitude: float,
        longitude: float
    ) -> Optional[Dict[str, Any]]:
        """Fetch current weather data.
        
        Args:
            latitude: Latitude of the location
            longitude: Longitude of the location
        
        Returns:
            Raw API response or None if error
        """
        if not self.api_key:
            logger.error("Cannot fetch weather: API key missing")
            return None
        
        url = f"{self.BASE_URL}/weather"
        params = {
            "lat": latitude,
            "lon": longitude,
            "appid": self.api_key,
            "units": "metric"  # Celsius
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Error fetching current weather: {e}")
            return None
    
    async def get_forecast(
        self,
        latitude: float,
        longitude: float,
        days: int = 5
    ) -> Optional[Dict[str, Any]]:
        """Fetch weather forecast (5 days / 3-hour intervals).
        
        Args:
            latitude: Latitude of the location
            longitude: Longitude of the location
            days: Number of days (max 5 for free tier)
        
        Returns:
            Raw API response or None if error
        """
        if not self.api_key:
            logger.error("Cannot fetch forecast: API key missing")
            return None
        
        url = f"{self.BASE_URL}/forecast"
        params = {
            "lat": latitude,
            "lon": longitude,
            "appid": self.api_key,
            "units": "metric",  # Celsius
            "cnt": days * 8  # 8 intervals per day (3-hour intervals)
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Error fetching weather forecast: {e}")
            return None
