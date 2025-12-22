"""Weather Fetcher implementation using OpenWeatherMap API."""
import os
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime, timedelta
import pytz
from .openweathermap_client import OpenWeatherMapClient
from .gemini_weather_fallback import GeminiWeatherFallback

logger = logging.getLogger(__name__)


class WeatherFetcherImpl:
    """Fetches weather data from OpenWeatherMap API with Gemini fallback."""
    
    def __init__(
        self,
        client: Optional[OpenWeatherMapClient] = None,
        fallback: Optional[GeminiWeatherFallback] = None
    ):
        self.client = client or OpenWeatherMapClient()
        self.fallback = fallback or GeminiWeatherFallback()
    
    def _kelvin_to_celsius(self, kelvin: float) -> int:
        """Convert Kelvin to Celsius."""
        return round(kelvin - 273.15)
    
    def _map_weather_condition(self, weather_main: str, weather_description: str) -> str:
        """Map OpenWeatherMap condition to readable format."""
        condition_map = {
            "Clear": "Clear Sky",
            "Clouds": "Cloudy",
            "Rain": "Rainy",
            "Drizzle": "Light Rain",
            "Thunderstorm": "Thunderstorm",
            "Snow": "Snowy",
            "Mist": "Misty",
            "Fog": "Foggy",
            "Haze": "Hazy"
        }
        
        # Use description for more detail if available
        if weather_description:
            return weather_description.title()
        
        return condition_map.get(weather_main, weather_main)
    
    def _get_weather_icon_url(self, icon_code: str) -> str:
        """Get weather icon URL from OpenWeatherMap."""
        return f"https://openweathermap.org/img/wn/{icon_code}@2x.png"
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str],
        latitude: float = 48.8584,
        longitude: float = 2.2945,
        timezone_str: str = "Europe/Paris",
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None,
        country: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch weather data for an attraction.
        
        Returns:
        - card: Today's weather for the card view
        - forecast_days: Multiple days of forecast for DB storage
        - source: "openweathermap_api" or "gemini_fallback"
        """
        try:
            tz = pytz.timezone(timezone_str)
        except:
            tz = pytz.UTC
        
        now = datetime.now(tz)
        
        # Fetch current weather
        current_weather = await self.client.get_current_weather(latitude, longitude)
        if not current_weather:
            logger.warning(f"Failed to fetch current weather for attraction {attraction_id}")
            return None
        
        # Fetch forecast
        forecast_data = await self.client.get_forecast(latitude, longitude, days=5)
        if not forecast_data:
            logger.warning(f"Failed to fetch weather forecast for attraction {attraction_id}")
            forecast_data = {"list": []}
        
        # Process current weather for card
        main = current_weather.get("main", {})
        weather_list = current_weather.get("weather", [])
        weather_info = weather_list[0] if weather_list else {}
        wind = current_weather.get("wind", {})
        rain = current_weather.get("rain", {})
        
        temperature_c = round(main.get("temp", 0))
        feels_like_c = round(main.get("feels_like", 0))
        min_temperature_c = round(main.get("temp_min", 0))
        max_temperature_c = round(main.get("temp_max", 0))
        humidity_percent = main.get("humidity", 0)
        
        condition = self._map_weather_condition(
            weather_info.get("main", ""),
            weather_info.get("description", "")
        )
        
        icon_code = weather_info.get("icon", "01d")
        icon_url = self._get_weather_icon_url(icon_code)
        
        # Precipitation (rain in last 1h or 3h)
        precipitation_mm = rain.get("1h", rain.get("3h", 0))
        
        # Wind speed (convert m/s to kph)
        wind_speed_ms = wind.get("speed", 0)
        wind_speed_kph = round(wind_speed_ms * 3.6)
        
        # Process forecast data - group by day
        forecast_by_day: Dict[str, List[Dict]] = {}
        
        for forecast_item in forecast_data.get("list", []):
            dt = forecast_item.get("dt")
            if not dt:
                continue
            
            forecast_time = datetime.fromtimestamp(dt, tz=tz)
            date_key = forecast_time.strftime("%Y-%m-%d")
            
            if date_key not in forecast_by_day:
                forecast_by_day[date_key] = []
            
            forecast_by_day[date_key].append(forecast_item)
        
        # Build forecast days
        forecast_days = []
        
        for date_str in sorted(forecast_by_day.keys()):
            day_forecasts = forecast_by_day[date_str]
            
            # Calculate daily aggregates
            temps = [f.get("main", {}).get("temp", 0) for f in day_forecasts]
            feels_like_temps = [f.get("main", {}).get("feels_like", 0) for f in day_forecasts]
            min_temps = [f.get("main", {}).get("temp_min", 0) for f in day_forecasts]
            max_temps = [f.get("main", {}).get("temp_max", 0) for f in day_forecasts]
            humidities = [f.get("main", {}).get("humidity", 0) for f in day_forecasts]
            
            # Get most common weather condition for the day
            conditions = []
            for f in day_forecasts:
                weather = f.get("weather", [])
                if weather:
                    conditions.append(weather[0].get("main", ""))
            
            most_common_condition = max(set(conditions), key=conditions.count) if conditions else "Clear"
            
            # Get icon from midday forecast (around 12:00)
            midday_forecast = day_forecasts[len(day_forecasts) // 2]
            midday_weather = midday_forecast.get("weather", [])
            midday_icon = midday_weather[0].get("icon", "01d") if midday_weather else "01d"
            
            # Calculate precipitation
            total_precipitation = 0
            for f in day_forecasts:
                rain_data = f.get("rain", {})
                total_precipitation += rain_data.get("3h", 0)
            
            # Calculate wind speed
            wind_speeds = [f.get("wind", {}).get("speed", 0) for f in day_forecasts]
            avg_wind_speed_ms = sum(wind_speeds) / len(wind_speeds) if wind_speeds else 0
            avg_wind_speed_kph = round(avg_wind_speed_ms * 3.6)
            
            forecast_days.append({
                "date": date_str,
                "card": {
                    "date_local": date_str,
                    "temperature_c": round(sum(temps) / len(temps)) if temps else 0,
                    "feels_like_c": round(sum(feels_like_temps) / len(feels_like_temps)) if feels_like_temps else 0,
                    "min_temperature_c": round(min(min_temps)) if min_temps else 0,
                    "max_temperature_c": round(max(max_temps)) if max_temps else 0,
                    "summary": self._map_weather_condition(most_common_condition, ""),
                    "precipitation_mm": round(total_precipitation, 1),
                    "wind_speed_kph": avg_wind_speed_kph,
                    "humidity_percent": round(sum(humidities) / len(humidities)) if humidities else 0,
                    "icon_url": self._get_weather_icon_url(midday_icon)
                }
            })
        
        return {
            "card": {
                "date_local": now.strftime("%Y-%m-%d"),
                "temperature_c": temperature_c,
                "feels_like_c": feels_like_c,
                "min_temperature_c": min_temperature_c,
                "max_temperature_c": max_temperature_c,
                "summary": condition,
                "precipitation_mm": round(precipitation_mm, 1),
                "wind_speed_kph": wind_speed_kph,
                "humidity_percent": humidity_percent,
                "icon_url": icon_url
            },
            "forecast_days": forecast_days,  # For DB storage (5 days)
            "source": "openweathermap_api"
        }
        
        return None
