"""Gemini-based fallback for Best Time data when BestTime API fails."""
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import pytz
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class GeminiBestTimeFallback:
    """Generate best time data using Gemini AI when BestTime API is unavailable."""
    
    def __init__(self, client: Optional[GeminiClient] = None):
        self.client = client or GeminiClient()
    
    async def generate_best_time_data(
        self,
        venue_name: str,
        venue_address: str,
        timezone_str: str = "UTC"
    ) -> Optional[Dict[str, Any]]:
        """Generate best time data using Gemini AI.
        
        Args:
            venue_name: Name of the attraction
            venue_address: Address of the attraction
            timezone_str: Timezone string (e.g., "Europe/Paris")
        
        Returns:
            Dict with card, section, and all_days data matching BestTime structure
        """
        try:
            tz = pytz.timezone(timezone_str)
        except:
            tz = pytz.UTC
        
        now = datetime.now(tz)
        today_date = now.strftime("%Y-%m-%d")
        
        # First, get general attraction info (opening/closing times)
        prompt = f"""You are a travel expert. Generate typical opening and closing times for this attraction:

Attraction: {venue_name}
Address: {venue_address}

Generate a JSON response with:
{{
  "opening_time": "HH:MM" (typical opening time, e.g., "09:00"),
  "closing_time": "HH:MM" (typical closing time, e.g., "18:00")
}}

Return ONLY the JSON."""

        result = await self.client.generate_json(prompt)
        if not result:
            logger.error("Failed to generate opening/closing times with Gemini")
            return None
        
        opening_time = result.get("opening_time", "09:00")
        closing_time = result.get("closing_time", "18:00")
        opening_hour = int(opening_time.split(":")[0])
        closing_hour = int(closing_time.split(":")[0])
        
        current_hour = now.hour
        is_open_now = opening_hour <= current_hour < closing_hour
        
        # Generate 7 days of data with day-specific best times and reasons
        all_days = []
        today_data = None
        
        for day_offset in range(7):
            day_date = now.date() + timedelta(days=day_offset)
            day_datetime = now + timedelta(days=day_offset)
            day_name = day_datetime.strftime("%A")
            is_weekend = day_datetime.weekday() >= 5  # Saturday=5, Sunday=6
            
            # Generate day-specific best time and reason
            day_prompt = f"""You are a travel expert. Generate the best time to visit this attraction on a {day_name} and explain why.

Attraction: {venue_name}
Address: {venue_address}
Day: {day_name}
Day Type: {"weekend" if is_weekend else "weekday"}
Operating Hours: {opening_time} - {closing_time}

Generate a JSON response with:
{{
  "best_time_start": "HH:MM" (best time to arrive, e.g., "09:00"),
  "best_time_end": "HH:MM" (best time window end, e.g., "11:00"),
  "reason": "1-2 sentences explaining why this is the best time on {day_name}"
}}

Consider that {day_name} is a {"weekend" if is_weekend else "weekday"} and adjust accordingly.
Return ONLY the JSON."""

            day_result = await self.client.generate_json(day_prompt)
            if not day_result:
                logger.warning(f"Failed to generate day-specific data for {day_name}")
                best_time_start = "09:00"
                best_time_end = "11:00"
                reason_text = f"Visit {venue_name} during quieter hours for the best experience."
            else:
                best_time_start = day_result.get("best_time_start", "09:00")
                best_time_end = day_result.get("best_time_end", "11:00")
                reason_text = day_result.get("reason", f"Visit {venue_name} during quieter hours for the best experience.")
            
            best_time_window = f"{best_time_start} - {best_time_end}"
            
            # Generate unique hourly data for this specific day
            hourly_for_day = await self.generate_hourly_crowd_levels(
                venue_name=venue_name,
                venue_address=venue_address,
                opening_hour=opening_hour,
                closing_hour=closing_hour,
                day_name=day_name,
                is_weekend=is_weekend
            )
            
            # Calculate average crowd level for the day
            day_crowd_level = 50  # default (moderate)
            if hourly_for_day:
                avg_crowd = sum(h.get("value", 50) for h in hourly_for_day) / len(hourly_for_day)
                day_crowd_level = round(avg_crowd)
            else:
                hourly_for_day = []
            
            # Only today can be "now"
            is_open_now_day = False
            if day_offset == 0:
                is_open_now_day = is_open_now
            
            day_data = {
                "date": day_date.strftime("%Y-%m-%d"),
                "day_name": day_name,
                "card": {
                    "is_open_today": True,
                    "is_open_now": is_open_now_day,
                    "today_local_date": day_date.strftime("%Y-%m-%d"),
                    "today_opening_time": opening_time,
                    "today_closing_time": closing_time,
                    "crowd_level_today": day_crowd_level,
                    "best_time_today": best_time_window
                },
                "section": {
                    "best_time_today": best_time_window,
                    "reason_text": reason_text,
                    "hourly_crowd_levels": hourly_for_day
                }
            }
            
            all_days.append(day_data)
            
            # Store today's data for the top-level card/section
            if day_offset == 0:
                today_data = day_data
        
        if not today_data:
            today_data = all_days[0] if all_days else None
        
        return {
            "card": today_data["card"] if today_data else {},
            "section": today_data["section"] if today_data else {},
            "all_days": all_days,
            "source": "gemini_fallback"  # Mark as fallback data
        }

    async def generate_hourly_crowd_levels(
        self,
        venue_name: str,
        venue_address: str,
        opening_hour: Optional[int] = None,
        closing_hour: Optional[int] = None,
        day_name: str = "Monday",
        is_weekend: bool = False
    ) -> Optional[list]:
        """Generate hourly crowd levels for a specific day using Gemini AI.
        
        Used as fallback when BestTime API doesn't provide hour_analysis data.
        
        Args:
            venue_name: Name of the attraction
            venue_address: Address of the attraction
            opening_hour: Opening hour (0-23), defaults to 9
            closing_hour: Closing hour (0-23), defaults to 18
            day_name: Day of week (e.g., "Monday", "Saturday")
            is_weekend: Whether this is a weekend day
        
        Returns:
            List of hourly crowd levels with format [{"hour": "HH:MM", "value": 0-100}, ...]
        """
        if opening_hour is None:
            opening_hour = 9
        if closing_hour is None:
            closing_hour = 18
        
        day_type = "weekend" if is_weekend else "weekday"
        
        prompt = f"""You are a travel expert. Generate realistic hourly crowd level data for this attraction on a typical {day_type} {day_name}:

Attraction: {venue_name}
Address: {venue_address}
Operating Hours: {opening_hour:02d}:00 - {closing_hour:02d}:00
Day Type: {day_type}

Generate a JSON array with hourly crowd levels for each hour the attraction is open:

[
  {{"hour": "09:00", "value": 35}},
  {{"hour": "10:00", "value": 42}},
  ... (continue for each hour until closing)
]

Guidelines:
- Include only hours between {opening_hour:02d}:00 and {closing_hour:02d}:00
- Crowd values: 0-20=Very Quiet, 21-40=Quiet, 41-60=Moderate, 61-80=Busy, 81-100=Extremely Busy
- Use realistic numeric values (e.g., 35, 72, 88) not just multiples of 20
- Typical pattern: quieter at opening, peak around midday, quieter before closing
- {"Weekend days are typically 20-40% busier than weekdays" if is_weekend else "Weekday patterns are more predictable"}
- Consider the specific attraction type and location
- Return ONLY the JSON array, no other text

Return ONLY valid JSON array."""

        result = await self.client.generate_json(prompt)
        if not result:
            logger.error(f"Failed to generate hourly crowd levels for {venue_name}")
            return None
        
        # Validate the result is a list
        if not isinstance(result, list):
            logger.error(f"Gemini returned non-list result for hourly data: {type(result)}")
            return None
        
        # Validate each entry has hour and value
        validated_hourly = []
        for entry in result:
            if isinstance(entry, dict) and "hour" in entry and "value" in entry:
                try:
                    # Validate hour format
                    datetime.strptime(entry["hour"], "%H:%M")
                    # Validate value is 0-100
                    value = int(entry["value"])
                    if 0 <= value <= 100:
                        validated_hourly.append({
                            "hour": entry["hour"],
                            "value": value
                        })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid hourly entry: {e}, skipping")
                    continue
        
        if not validated_hourly:
            logger.warning(f"No valid hourly data generated for {venue_name}")
            return None
        
        return validated_hourly

    async def generate_special_days_data(
        self,
        venue_name: str,
        venue_address: str,
        timezone_str: str = "UTC"
    ) -> Optional[Dict[str, Any]]:
        """Generate special days data using Gemini AI for upcoming year.

        Special days include holidays, events, festivals, or other exceptional days
        that might affect crowd levels and operating hours.

        Args:
            venue_name: Name of the attraction
            venue_address: Address of the attraction
            timezone_str: Timezone string (e.g., "Europe/Paris")

        Returns:
            Dict with special_days list containing special day data
        """
        try:
            tz = pytz.timezone(timezone_str)
        except:
            tz = pytz.UTC

        now = datetime.now(tz)
        current_year = now.year
        next_year = current_year + 1

        prompt = f"""You are a travel expert with deep knowledge of tourist attractions worldwide. Generate special days data for this attraction for the upcoming year:

Attraction: {venue_name}
Address: {venue_address}
Current Year: {current_year}
Next Year: {next_year}

IMPORTANT: Your response must be specific to "{venue_name}" - consider its unique characteristics, location, and type of attraction. Focus on days that would significantly affect visitor patterns, crowd levels, or operating hours.

Generate a JSON response with the following structure:

{{
  "special_days": [
    {{
      "date": "YYYY-MM-DD" (specific date of the special day),
      "day": "Day Name" (e.g., "Monday", "Tuesday"),
      "is_open_today": true/false (whether the attraction is open on this special day),
      "today_opening_time": "HH:MM" or null (opening time in 24h format, null if closed),
      "today_closing_time": "HH:MM" or null (closing time in 24h format, null if closed),
      "crowd_level_today": 1-5 (crowd level: 1=Very Light, 2=Light, 3=Moderate, 4=Busy, 5=Peak),
      "best_time_today": "HH:MM - HH:MM" (best 2-hour window to visit),
      "reason_text": "Detailed explanation (2-3 sentences) why this is the best time, considering the special day circumstances",
      "hourly_crowd_levels": [
        {{"hour": "09:00", "value": 45}},
        {{"hour": "10:00", "value": 50}},
        ... (hourly data as objects with hour and value for the operating hours)
      ]
    }},
    ... (generate all relevant special days for the upcoming year)
  ]
}}

Guidelines:
- Include major holidays, local festivals, events, or seasonal peaks that affect this specific attraction
- For each special day, consider how the event/holiday impacts crowds and hours
- Crowd levels: 1=Very Light (<20%), 2=Light (20-40%), 3=Moderate (40-60%), 4=Busy (60-80%), 5=Peak (80-100%)
- Best time should be during relatively quieter periods on busy special days
- Opening/closing times in 24-hour format (e.g., "09:00", "18:00")
- Hourly crowd levels as objects like {{"hour": "11:00", "value": 85}} with hour in HH:MM format and value as 0-100 integer
- Be realistic and specific to {venue_name} and its location
- Include dates from {current_year}-{now.month:02d}-{now.day:02d} to {next_year}-12-31
- If the attraction is typically closed on certain holidays, mark as closed
- Provide detailed reasons that explain crowd patterns on that specific day

Return ONLY the JSON, no other text."""

        result = await self.client.generate_json(prompt)
        if not result:
            logger.error("Failed to generate special days data with Gemini")
            return None

        special_days = result.get("special_days", [])
        if not special_days:
            logger.warning("No special days generated by Gemini")
            return {"special_days": []}

        # Validate and clean the data
        validated_special_days = []
        for day in special_days:
            try:
                # Validate date format
                datetime.strptime(day["date"], "%Y-%m-%d")
                validated_special_days.append(day)
            except (ValueError, KeyError) as e:
                logger.warning(f"Invalid special day data: {e}, skipping")
                continue

        return {
            "special_days": validated_special_days,
            "source": "gemini_special_days"
        }
