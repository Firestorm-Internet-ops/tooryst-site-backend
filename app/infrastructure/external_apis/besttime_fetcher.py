"""Best Time Fetcher implementation using Best Time API."""
import asyncio
import os
from typing import Optional, Dict, Any, List, Tuple
import logging
from datetime import datetime, timedelta
import math
import re
import unicodedata
import pytz

from app.constants import EARTH_RADIUS_KM
from app.config import settings
from .besttime_client import BestTimeClient
from .gemini_besttime_fallback import GeminiBestTimeFallback
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class BestTimeFetcherImpl:
    """Fetches best time data from Best Time API with Gemini fallback."""
    
    def __init__(
        self,
        client: Optional[BestTimeClient] = None,
        gemini_fallback: Optional[GeminiBestTimeFallback] = None,
        gemini_client: Optional[GeminiClient] = None
    ):
        self.client = client or BestTimeClient()
        self.gemini_fallback = gemini_fallback or GeminiBestTimeFallback()
        self.gemini_client = gemini_client or GeminiClient()
        self.name_match_cache = {}
    
    def _format_time(self, hour: int) -> str:
        """Format hour (0-23) to HH:MM format."""
        return f"{hour:02d}:00"
    
    async def _try_besttime_with_fallback(
        self,
        attraction: Any,
        city_name: Optional[str],
        venue_name: str
    ) -> Optional[Dict[str, Any]]:
        """Try BestTime API with fallback chain for venue not found errors.
        
        Fallback chain:
        1. resolved_name + address
        2. resolved_name + city_name
        3. attraction_name + address
        4. attraction_name + city_name
        """
        attempts = []
        
        # Build list of attempts
        if attraction.resolved_name and attraction.address:
            attempts.append((attraction.resolved_name, attraction.address, "resolved_name + address"))
        
        if attraction.resolved_name and city_name:
            attempts.append((attraction.resolved_name, f"{venue_name}, {city_name}", "resolved_name + city_name"))
        
        if attraction.address:
            attempts.append((attraction.name, attraction.address, "name + address"))
        
        if city_name:
            attempts.append((attraction.name, f"{venue_name}, {city_name}", "name + city_name"))
        
        # Try each attempt
        for search_name, search_address, attempt_desc in attempts:
            logger.info(f"Calling BestTime API with venue_name='{search_name}', venue_address='{search_address}'")
            logger.info(f"  Attempt: {attempt_desc}")
            
            result = await self.client.get_forecast(
                venue_name=search_name,
                venue_address=search_address
            )
            
            # Check if we got a successful response
            if result and result.get("status") == "OK":
                logger.info(f"✓ BestTime API succeeded with {attempt_desc}")
                return result
            
            # Check if it's a "venue not found" error
            if result and result.get("status") == "Error":
                message = result.get("message", "")
                if "Could not find venue" in message or "not does not have enough volume" in message:
                    logger.warning(f"  ⚠ Venue not found with {attempt_desc}, trying next fallback...")
                    continue
                else:
                    # Different error, don't retry
                    logger.error(f"  ✗ BestTime API error: {message}")
                    return result
            
            # No result, try next
            logger.warning(f"  ⚠ No result with {attempt_desc}, trying next fallback...")
        
        # All attempts failed
        logger.warning(f"All BestTime API attempts failed for {venue_name}")
        return None
    
    def _map_intensity_to_crowd_level(self, intensity_nr: int) -> int:
        """Map hour_analysis intensity_nr (-2 to 2) to crowd level (0-5).
        
        intensity_nr scale:
        -2 = Low
        -1 = Below average
        0 = Average
        1 = Above average
        2 = High
        999 = Closed
        """
        if intensity_nr == settings.BEST_TIME_INTENSITY_CLOSED:
            return 0  # Closed
        # Map -2 to 2 scale to 0 to 5 scale
        # -2 -> 0, -1 -> 1, 0 -> 2, 1 -> 3, 2 -> 4
        # Add buffer for very high (5)
        return max(settings.BEST_TIME_CROWD_LEVEL_MIN, min(settings.BEST_TIME_CROWD_LEVEL_MAX, intensity_nr + 2))
    
    def _get_open_hours_from_analysis(self, hour_analysis: List[Dict]) -> Tuple[Optional[int], Optional[int]]:
        """Extract opening and closing hours from hour_analysis.
        
        Returns (opening_hour, closing_hour) or (None, None) if closed all day.
        """
        open_hours = [h["hour"] for h in hour_analysis if h.get("intensity_nr") != 999]
        if not open_hours:
            return None, None
        return min(open_hours), max(open_hours) + 1  # +1 because closing is exclusive
    
    def _find_best_time_window(
        self, 
        quiet_hours: List[int], 
        hour_analysis: List[Dict],
        opening_hour: Optional[int],
        closing_hour: Optional[int]
    ) -> str:
        """Find the best time window (lowest crowd period)."""
        window_hours = settings.BEST_TIME_WINDOW_HOURS
        default_closing = settings.BEST_TIME_CLOSING_HOUR_DEFAULT
        
        if quiet_hours:
            # Use first quiet hour as start
            start_hour = quiet_hours[0]
            end_hour = min(start_hour + window_hours, closing_hour if closing_hour else default_closing)
            return f"{self._format_time(start_hour)} - {self._format_time(end_hour)}"
        
        # Fallback: find lowest intensity from hour_analysis (only open hours)
        open_hour_data = [h for h in hour_analysis if h.get("intensity_nr") != settings.BEST_TIME_INTENSITY_CLOSED]
        if open_hour_data:
            min_hour_data = min(open_hour_data, key=lambda x: x.get("intensity_nr", 100))
            start_hour = min_hour_data["hour"]
            end_hour = min(start_hour + window_hours, closing_hour if closing_hour else default_closing)
            return f"{self._format_time(start_hour)} - {self._format_time(end_hour)}"
        
        return "09:00 - 11:00"  # default
    
    def _generate_reason_text(self, quiet_hours: List[int], busy_hours: List[int]) -> str:
        """Generate reason text for best time recommendation."""
        morning_threshold = settings.BEST_TIME_MORNING_THRESHOLD_HOUR
        if quiet_hours:
            time_desc = "morning" if quiet_hours[0] < morning_threshold else "evening"
            return f"Fewer crowds during {time_desc} hours make for a more pleasant visit"
        if busy_hours:
            return "Visit outside peak hours for a better experience"
        return "Visit during off-peak hours for the best experience"
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str] = None,
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch best time data for an attraction using BestTime forecast API.
        
        Returns:
        - card: Today's data for the card view
        - section: Today's data for the section view
        - regular_days: 7 days of data for DB storage
        - special_days: Special event data
        """
        # Get attraction details from database
        from app.infrastructure.persistence.db import SessionLocal
        from app.infrastructure.persistence import models
        
        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                logger.error(f"Attraction {attraction_id} not found")
                return None

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            venue_name = attraction_name or attraction.name
            
            # Determine venue_name for BestTime API with fallback chain:
            # 1. resolved_name + address
            # 2. name + address
            # 3. name + city_name
            if attraction.resolved_name and attraction.address:
                besttime_venue_name = attraction.resolved_name
                venue_address = attraction.address
                logger.info(f"Fetching best time data for: {venue_name}")
                logger.info(f"  Using resolved_name + address: {besttime_venue_name}, {venue_address}")
            elif attraction.address:
                besttime_venue_name = attraction.name
                venue_address = attraction.address
                logger.info(f"Fetching best time data for: {venue_name}")
                logger.info(f"  Using name + address: {besttime_venue_name}, {venue_address}")
            else:
                besttime_venue_name = attraction.name
                venue_address = f"{venue_name}, {city_name}" if city_name else venue_name
                logger.info(f"Fetching best time data for: {venue_name}")
                logger.info(f"  Using name + city_name: {besttime_venue_name}, {venue_address}")

        finally:
            session.close()

        city_tz = city.timezone if city else None

        # Try BestTime API with fallback chain for venue not found errors
        # 1. resolved_name + address
        # 2. resolved_name + city_name
        # 3. attraction_name + address
        # 4. attraction_name + city_name
        forecast_result = await self._try_besttime_with_fallback(
            attraction=attraction,
            city_name=city_name,
            venue_name=venue_name
        )

        if not forecast_result or forecast_result.get("status") != "OK":
            logger.warning(f"BestTime forecast API returned no data for {venue_name}, using Gemini fallback")
            return await self._fallback_gemini(venue_name, venue_address, city_tz)

        analysis = forecast_result.get("analysis", [])
        if not analysis:
            logger.warning("BestTime forecast returned empty analysis; using Gemini fallback")
            return await self._fallback_gemini(besttime_venue_name, venue_address, city_tz)

        # Process forecast into regular_days (day-of-week based, no specific dates)
        # First pass: collect all day data
        regular_days_raw = []
        for day in analysis:
            day_info = day.get("day_info", {})
            hour_analysis = day.get("hour_analysis", [])
            busy_hours = day.get("busy_hours", [])
            quiet_hours = day.get("quiet_hours", [])

            # Get the day_int from forecast (0=Monday through 6=Sunday)
            forecast_day_int = day_info.get("day_int")
            if forecast_day_int is None:
                logger.warning(f"Skipping forecast day - missing day_int: {day_info}")
                continue

            # Extract opening and closing hours from hour_analysis
            opens, closes = self._get_open_hours_from_analysis(hour_analysis)
            
            day_name = day_info.get("day_text", "")
            
            # Build hourly data with Gemini fallback if needed
            hourly_data = await self._get_hourly_data_with_fallback(
                hour_analysis=hour_analysis,
                venue_name=besttime_venue_name,
                venue_address=venue_address,
                day_name=day_name,
                forecast_day_int=forecast_day_int,
                opens=opens,
                closes=closes
            )
            
            # Find best time window
            best_window = self._find_best_time_window(quiet_hours, hour_analysis, opens, closes)
            
            day_crowd_level_num = round(sum(h["value"] for h in hourly_data) / len(hourly_data)) if hourly_data else 0

            regular_days_raw.append({
                'forecast_day_int': forecast_day_int,
                'day_name': day_name,
                'best_window': best_window,
                'day_info': day_info,
                'hour_analysis': hour_analysis,
                'busy_hours': busy_hours,
                'quiet_hours': quiet_hours,
                'opens': opens,
                'closes': closes,
                'hourly_data': hourly_data,
                'day_crowd_level_num': day_crowd_level_num
            })

        # Batch generate reason texts for all days (reduces API calls from 7 to 1)
        reason_texts = []
        if regular_days_raw:
            reason_texts = await self._batch_reason_texts_with_gemini(venue_name, regular_days_raw)

        # Second pass: build final regular_days structure
        regular_days: List[Dict[str, Any]] = []
        today_data = None

        for i, day_data in enumerate(regular_days_raw):
            day_data_full = {
                "day_int": day_data['forecast_day_int'],
                "day_name": day_data['day_name'],
                "card": {
                    "is_open_today": day_data['opens'] is not None,
                    "is_open_now": False,  # no reliable local time from API
                    "today_opening_time": self._format_time(day_data['opens']) if day_data['opens'] is not None else None,
                    "today_closing_time": self._format_time(day_data['closes']) if day_data['closes'] is not None else None,
                    "crowd_level_today": day_data['day_crowd_level_num'],  # Store raw 0-100 value
                    "best_time_today": day_data['best_window']
                },
                "section": {
                    "best_time_today": day_data['best_window'],
                    "reason_text": reason_texts[i] if i < len(reason_texts) else "Based on crowd patterns for this day",
                    "hourly_crowd_levels": day_data['hourly_data']
                },
                "data_source": "besttime"
            }

            regular_days.append(day_data_full)

            # Check if this is today's day (forecast_day_int == current weekday)
            # Use city timezone if available, fallback to UTC
            if city and city.timezone:
                try:
                    venue_tz = pytz.timezone(city.timezone)
                    now_venue = datetime.now(venue_tz)
                except Exception:
                    now_venue = datetime.utcnow()
            else:
                now_venue = datetime.utcnow()
            today_day_int = now_venue.weekday()
            if day_data['forecast_day_int'] == today_day_int:
                today_data = day_data_full

        # Sort regular_days by day_int (0=Monday to 6=Sunday)
        regular_days.sort(key=lambda x: x["day_int"])

        if not today_data and regular_days:
            # Fallback to first day if today not found
            today_data = regular_days[0]

        if not today_data:
            logger.error("No forecast data available")
            return None

        # TODO: Re-enable special days generation
        # # Generate special days data using Gemini
        # timezone_str = city.timezone if city and city.timezone else "UTC"
        # special_days_result = await self.gemini_fallback.generate_special_days_data(
        #     venue_name=venue_name,
        #     venue_address=venue_address,
        #     timezone_str=timezone_str
        # )
        # special_days = special_days_result.get("special_days", []) if special_days_result else []
        special_days = []

        return {
            "card": today_data["card"],
            "section": today_data["section"],
            "regular_days": regular_days,
            "special_days": special_days,
            "data_source": "besttime"
        }

    async def _poll_progress(self, progress_url: str) -> Optional[Dict[str, Any]]:
        """Poll BestTime progress endpoint until finished or timeout."""
        max_attempts = 8
        delay_seconds = 2
        for attempt in range(max_attempts):
            data = await self.client.get_search_progress(progress_url)
            if not data:
                await asyncio.sleep(delay_seconds)
                continue
            if data.get("job_finished"):
                return data
            await asyncio.sleep(delay_seconds)
        return None

    async def _fallback_gemini(self, venue_name: str, venue_address: str, city_timezone: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Use Gemini fallback when BestTime data is unavailable."""
        logger.info(f"Using Gemini fallback for: {venue_name}")
        logger.info(f"  Address: {venue_address}")
        
        # Generate regular weekly data
        timezone_str = city_timezone if city_timezone else "UTC"
        gemini_result = await self.gemini_fallback.generate_best_time_data(
            venue_name=venue_name,
            venue_address=venue_address,
            timezone_str=timezone_str
        )
        if not gemini_result:
            logger.error(f"Both BestTime API and Gemini fallback failed for {venue_name}")
            return None

        # Convert all_days to regular_days format (day-of-week based)
        all_days = gemini_result.get("all_days", [])
        regular_days = []
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        for i, day_data in enumerate(all_days[:7]):  # Take first 7 days
            regular_day = {
                "day_int": i,
                "day_name": day_names[i],
                "card": day_data["card"],
                "section": day_data["section"],
                "data_source": "gemini_fallback"
            }
            # Remove date-specific fields from card
            regular_day["card"].pop("today_local_date", None)
            regular_days.append(regular_day)

        # Generate special days
        timezone_str = city_timezone if city_timezone else "UTC"
        special_days_result = await self.gemini_fallback.generate_special_days_data(
            venue_name=venue_name,
            venue_address=venue_address,
            timezone_str=timezone_str
        )
        special_days = special_days_result.get("special_days", []) if special_days_result else []

        # Update the result structure
        gemini_result["regular_days"] = regular_days
        gemini_result["special_days"] = special_days
        gemini_result.pop("all_days", None)  # Remove old all_days
        gemini_result["data_source"] = "gemini_fallback"

        logger.info(f"Successfully generated best time data for {venue_name} using Gemini fallback")
        return gemini_result

    def _extract_open_close(self, day_info: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
        """Extract opens/closes from BestTime day_info venue_open_close_v2."""
        venue_open_close = day_info.get("venue_open_close_v2", {})
        hours_24h = venue_open_close.get("24h", [])
        if hours_24h:
            opens = hours_24h[0].get("opens")
            closes = hours_24h[0].get("closes")
            return opens, closes
        return None, None

    async def _get_hourly_data_with_fallback(
        self,
        hour_analysis: List[Dict[str, Any]],
        venue_name: str,
        venue_address: str,
        day_name: str,
        forecast_day_int: int,
        opens: Optional[int],
        closes: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get hourly data from hour_analysis, with Gemini fallback if empty.
        
        Args:
            hour_analysis: BestTime hour_analysis data
            venue_name: Name of attraction
            venue_address: Address of attraction (from database)
            day_name: Day name (e.g., "Monday")
            forecast_day_int: Day int (0=Monday, 6=Sunday)
            opens: Opening hour
            closes: Closing hour
        
        Returns:
            List of hourly crowd levels
        """
        # Try to build from hour_analysis first
        hourly_data = self._build_hourly_from_hour_analysis(hour_analysis)
        
        # If we got data, return it
        if hourly_data:
            return hourly_data
        
        # Fallback to Gemini if hour_analysis is empty
        logger.warning(f"No hour_analysis data for {day_name}, using Gemini fallback for hourly data")
        logger.info(f"  Generating hourly data for {venue_name} ({venue_address}) on {day_name}")
        
        is_weekend = forecast_day_int >= 5  # Saturday=5, Sunday=6
        
        # Use default hours if not provided
        if opens is None:
            opens = 9
        if closes is None:
            closes = 18
        
        try:
            hourly_data = await self.gemini_fallback.generate_hourly_crowd_levels(
                venue_name=venue_name,
                venue_address=venue_address,
                opening_hour=opens,
                closing_hour=closes,
                day_name=day_name,
                is_weekend=is_weekend
            )
            
            if hourly_data:
                logger.info(f"Generated {len(hourly_data)} hourly data points via Gemini for {day_name}")
                return hourly_data
        except Exception as e:
            logger.error(f"Gemini hourly fallback failed: {e}")
        
        # Final fallback: generate synthetic data
        logger.warning(f"Generating synthetic hourly data for {day_name}")
        return self._generate_synthetic_hourly_data(opens, closes, is_weekend)

    def _generate_synthetic_hourly_data(
        self,
        opens: int,
        closes: int,
        is_weekend: bool
    ) -> List[Dict[str, Any]]:
        """Generate synthetic hourly crowd levels as last resort.
        
        Uses a typical pattern: quiet at opening, peak midday, quiet before closing.
        """
        hourly = []
        
        # Typical crowd pattern
        for hour in range(opens, closes):
            # Calculate relative position in the day (0.0 to 1.0)
            day_length = closes - opens
            relative_pos = (hour - opens) / day_length if day_length > 0 else 0.5
            
            # Bell curve: quiet at edges, peak in middle
            # Using a simple quadratic: 100 * (1 - (2*x - 1)^2) where x is relative_pos
            base_value = 100 * (1 - (2 * relative_pos - 1) ** 2)
            
            # Add weekend boost (20-30% busier)
            if is_weekend:
                base_value *= 1.25
            
            # Add some variation
            value = max(20, min(90, int(base_value)))
            
            hourly.append({
                "hour": self._format_time(hour),
                "value": value
            })
        
        return hourly

    def _build_hourly_from_hour_analysis(
        self,
        hour_analysis: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Convert BestTime hour_analysis to hourly crowd levels (0-100 scale)."""
        hourly = []
        if not hour_analysis:
            return hourly

        for hour_data in hour_analysis:
            hour = hour_data.get("hour")
            intensity_nr = hour_data.get("intensity_nr")
            
            if hour is None or intensity_nr is None:
                continue
            
            # Skip closed hours
            if intensity_nr == settings.BEST_TIME_INTENSITY_CLOSED:
                continue
            
            # Map intensity (-2 to 2) to 0-100 scale
            # -2 = 0, -1 = 25, 0 = 50, 1 = 75, 2 = 100
            value = max(0, min(100, (intensity_nr + 2) * 20))
            
            hourly.append({
                "hour": self._format_time(hour),
                "value": value
            })
        return hourly

    def _build_hourly_from_day_raw(
        self,
        day_raw: List[int],
        opens: Optional[int],
        closes: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Convert BestTime day_raw (24 slots starting 06:00) to hourly raw values."""
        hourly = []
        if not day_raw:
            return hourly

        for idx, value in enumerate(day_raw):
            hour = (6 + idx) % 24  # BestTime day_raw starts at 06:00
            if opens is not None and closes is not None:
                if not (opens <= hour < closes):
                    continue
            hourly.append({
                "hour": self._format_time(hour),
                "value": value  # keep raw 0-100 for bar graph
            })
        return hourly

    def _best_window_from_hourly(self, hourly: List[Dict[str, Any]]) -> str:
        """Find 2-hour window with lowest average raw value."""
        if len(hourly) < 2:
            if hourly:
                return hourly[0]["hour"]
            return ""
        best_start_idx = 0
        best_avg = float("inf")
        for i in range(len(hourly) - 1):
            window = hourly[i:i+2]
            avg = sum(h["value"] for h in window) / len(window)
            if avg < best_avg:
                best_avg = avg
                best_start_idx = i
        start_hour = hourly[best_start_idx]["hour"]
        # parse hour
        h_int = int(start_hour.split(":")[0])
        end_hour = self._format_time((h_int + 2) % 24)
        return f"{start_hour} - {end_hour}"

    def _haversine_km(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance between two points on Earth in km."""
        R = EARTH_RADIUS_KM
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def _normalize(self, s: str) -> str:
        """Lowercase, strip accents, remove punctuation, collapse spaces."""
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        s = s.lower()
        s = re.sub(r"[^a-z0-9\\s]", " ", s)
        s = " ".join(s.split())
        return s

    async def _gemini_name_match(self, target: str, candidate: str) -> bool:
        """Use Gemini to determine if two venue names refer to the same place."""
        cache_key = f"{target}|{candidate}"
        if cache_key in self.name_match_cache:
            return self.name_match_cache[cache_key]

        prompt = (
            f"Determine if these two venue names refer to the same place. "
            f"Names: '{target}' and '{candidate}'. "
            f"Answer with only 'yes' or 'no'."
        )

        try:
            response = await self.gemini_client.generate_text(prompt)
            if response:
                response_lower = response.strip().lower()
                is_match = response_lower.startswith('yes')
                self.name_match_cache[cache_key] = is_match
                return is_match
        except Exception as e:
            logger.error(f"Gemini name matching failed: {e}")

        # Fallback to False on error
        self.name_match_cache[cache_key] = False
        return False

    async def _name_match_ok(self, target: str, candidate: str, threshold: float = 0.9) -> bool:
        """Check name similarity after normalization, with Gemini fallback."""
        import difflib
        nt = self._normalize(target)
        nc = self._normalize(candidate)
        if not nt or not nc:
            return False
        ratio = difflib.SequenceMatcher(None, nt, nc).ratio()

        # If fuzzy match is high enough, accept immediately
        if ratio >= threshold:
            return True

        # Otherwise, use Gemini for smarter matching
        logger.info(f"Fuzzy match {ratio:.2f} < {threshold}, using Gemini for '{target}' vs '{candidate}'")
        return await self._gemini_name_match(target, candidate)

    def _crowd_label(self, value_0_100: int) -> str:
        """Map 0-100 raw value to labeled bucket."""
        bucket = max(0, min(5, round(value_0_100 / 20)))
        labels = {
            0: "0 closed/empty",
            1: "1 very light",
            2: "2 light",
            3: "3 moderate",
            4: "4 busy",
            5: "5 peak"
        }
        return labels.get(bucket, f"{bucket}")

    async def _batch_reason_texts_with_gemini(
        self,
        venue_name: str,
        days_data: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate reason texts for multiple days in a single Gemini API call."""
        if not days_data:
            return []

        # Build a comprehensive prompt for all days
        prompt_parts = [
            f"Generate specific reason texts for visiting {venue_name} at different times on different days.",
            "For each day, provide 1-2 sentences explaining why the recommended time window is best for avoiding crowds.",
            "",
            "Days and their recommended visit times:",
        ]

        for i, day_data in enumerate(days_data, 1):
            day_name = day_data['day_name']
            best_window = day_data['best_window']
            crowd_level = day_data['day_crowd_level_num']
            opens = day_data.get('opens')
            closes = day_data.get('closes')
            quiet_hours = day_data.get('quiet_hours', [])

            if opens is not None and closes is not None:
                hours_str = f"{opens:02d}:00-{closes:02d}:00"
            else:
                hours_str = "Closed"

            quiet_str = f", quiet hours: {quiet_hours}" if quiet_hours else ""

            prompt_parts.append(f"{i}. {day_name} - Best time: {best_window} (Open {hours_str}, crowd level {crowd_level}/100{quiet_str})")

        prompt_parts.extend([
            "",
            "IMPORTANT: Return ONLY the reason texts, one per line, in the same order as the days listed above.",
            "Each reason should be 1-2 sentences explaining why that time window is best.",
            "Do NOT include day names, times, or any other information - ONLY the reason text.",
            "Example format:",
            "Arrive early to beat the crowds and enjoy the exhibits at your own pace.",
            "Mid-morning offers a good balance between fewer crowds and full facility availability."
        ])

        prompt = "\n".join(prompt_parts)

        try:
            # Single API call for all days
            response = await self.gemini_fallback.client.generate_text(prompt)
            if response:
                # Parse the response - each line is a reason for the corresponding day
                lines = response.strip().split('\n')
                reasons = []

                for line in lines:
                    line = line.strip()
                    if line:
                        # Clean up and limit length
                        reason_text = line[:240]
                        reasons.append(reason_text)

                # Ensure we have a reason for each day
                while len(reasons) < len(days_data):
                    reasons.append("Based on crowd patterns for this day")

                return reasons[:len(days_data)]

        except Exception as e:
            logger.error(f"Batch Gemini reason generation failed: {e}")

        # Fallback: generate individual reasons
        reasons = []
        for day_data in days_data:
            quiet_hours = day_data.get('quiet_hours', [])
            busy_hours = day_data.get('busy_hours', [])
            reason_text = self._generate_reason_text(quiet_hours, busy_hours)
            reasons.append(reason_text)

        return reasons

    async def _reason_text_with_gemini(
        self,
        venue_name: str,
        window: str,
        day_info: Dict[str, Any],
        day_raw: List[int]
    ) -> str:
        """Generate a two-line reason via Gemini; fallback to deterministic text."""
        prompt = (
            f"You are a travel assistant. Given crowd data for {venue_name}, "
            f"write two short sentences (<=2 lines total) explaining why "
            f"'{window}' is the best time to visit, referencing that it is quieter "
            f"than busier hours. Be concise and specific."
            f"\nDay info: {day_info}\nDay raw: {day_raw}"
        )
        try:
            text = await self.gemini_fallback.client.generate_text(prompt)
            if text:
                # Keep only first two sentences/lines
                lines = text.strip().splitlines()
                if len(lines) > 2:
                    lines = lines[:2]
                return " ".join(line.strip() for line in lines if line.strip())[:240]
        except Exception as e:
            logger.error(f"Gemini reason generation failed: {e}")

        return (
            f"{window} is the quietest window. Crowds are lower than surrounding hours, "
            f"so you get a smoother visit."
        )
