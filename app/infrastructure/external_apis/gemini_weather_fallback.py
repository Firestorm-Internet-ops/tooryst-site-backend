"""Gemini-based fallback for Weather when OpenWeatherMap API fails."""
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import pytz
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class GeminiWeatherFallback:
    """Generate weather data using Gemini AI when OpenWeatherMap API is unavailable."""
    
    def __init__(self, client: Optional[GeminiClient] = None):
        self.client = client or GeminiClient()
    
    async def generate_weather_data(
        self,
        attraction_name: str,
        city_name: str,
        country: str,
        timezone_str: str = "UTC",
        days: int = 5
    ) -> Optional[Dict[str, Any]]:
        """Generate weather forecast using Gemini AI.
        
        Args:
            attraction_name: Name of the attraction
            city_name: City name
            country: Country name
            timezone_str: Timezone string
            days: Number of forecast days
        
        Returns:
            Dict with card and forecast_days data
        """
        try:
            tz = pytz.timezone(timezone_str)
        except:
            tz = pytz.UTC
        
        now = datetime.now(tz)
        today_date = now.strftime("%Y-%m-%d")
        
        prompt = f"""You are a weather expert. Generate a realistic {days}-day weather forecast for {city_name}, {country}.

Today's date: {today_date}

Generate a JSON response with the following structure:

{{
  "today": {{
    "temperature_c": <average temp in Celsius>,
    "feels_like_c": <feels like temp>,
    "min_temperature_c": <min temp>,
    "max_temperature_c": <max temp>,
    "summary": "<weather condition like 'Partly Cloudy', 'Sunny', 'Rainy'>",
    "precipitation_mm": <rainfall in mm>,
    "wind_speed_kph": <wind speed in kph>,
    "humidity_percent": <humidity percentage>
  }},
  "forecast": [
    {{
      "date": "{(now + timedelta(days=i)).strftime('%Y-%m-%d')}",
      "temperature_c": <temp>,
      "feels_like_c": <feels like>,
      "min_temperature_c": <min>,
      "max_temperature_c": <max>,
      "summary": "<condition>",
      "precipitation_mm": <rainfall>,
      "wind_speed_kph": <wind>,
      "humidity_percent": <humidity>
    }}
    ... for {days} days
  ]
}}

Guidelines:
- Use realistic temperatures for {city_name} in December
- Weather conditions: Sunny, Partly Cloudy, Cloudy, Rainy, Stormy, etc.
- Be realistic based on the location and season

Return ONLY the JSON, no other text."""

        result = await self.client.generate_json(prompt)
        
        if not result:
            logger.error("Failed to generate weather data with Gemini")
            return None
        
        # Extract today's weather
        today_weather = result.get("today", {})
        
        # Build forecast days
        forecast_days = []
        forecast_list = result.get("forecast", [])
        
        for idx, day_forecast in enumerate(forecast_list[:days]):
            day_date = (now + timedelta(days=idx)).strftime("%Y-%m-%d")
            
            forecast_days.append({
                "date": day_date,
                "card": {
                    "date_local": day_date,
                    "temperature_c": day_forecast.get("temperature_c", 15),
                    "feels_like_c": day_forecast.get("feels_like_c", 14),
                    "min_temperature_c": day_forecast.get("min_temperature_c", 10),
                    "max_temperature_c": day_forecast.get("max_temperature_c", 20),
                    "summary": day_forecast.get("summary", "Partly Cloudy"),
                    "precipitation_mm": day_forecast.get("precipitation_mm", 0),
                    "wind_speed_kph": day_forecast.get("wind_speed_kph", 10),
                    "humidity_percent": day_forecast.get("humidity_percent", 60),
                    "icon_url": "https://openweathermap.org/img/wn/02d@2x.png"  # Default icon
                }
            })
        
        return {
            "card": {
                "date_local": today_date,
                "temperature_c": today_weather.get("temperature_c", 15),
                "feels_like_c": today_weather.get("feels_like_c", 14),
                "min_temperature_c": today_weather.get("min_temperature_c", 10),
                "max_temperature_c": today_weather.get("max_temperature_c", 20),
                "summary": today_weather.get("summary", "Partly Cloudy"),
                "precipitation_mm": today_weather.get("precipitation_mm", 0),
                "wind_speed_kph": today_weather.get("wind_speed_kph", 10),
                "humidity_percent": today_weather.get("humidity_percent", 60),
                "icon_url": "https://openweathermap.org/img/wn/02d@2x.png"
            },
            "forecast_days": forecast_days,
            "source": "gemini_fallback"
        }
