"""Helpers to hydrate attraction page/sections DTOs from the database."""
from datetime import datetime
from typing import List, Optional
import asyncio
import logging

from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.application.dto.attraction_dto import (
    AttractionPageDTO,
    AttractionCardsDTO,
    HeroImageDTO,
    BestTimeCardDTO,
    WeatherCardDTO,
    SocialVideoCardDTO,
    MapCardDTO,
    ReviewCardDTO,
    TipDTO,
    TipsCardDTO,
    AboutCardDTO,
    NearbyAttractionCardDTO,
)
from app.infrastructure.external_apis.weather_fetcher import WeatherFetcherImpl
from app.application.dto.section_dto import (
    AttractionSectionsDTO,
    SectionDTO,
    BestTimeSectionContentDTO,
    BestTimeTabDTO,
    ReviewsSectionContentDTO,
    ReviewItemDTO,
    WidgetSectionContentDTO,
    MapSectionContentDTO,
    VisitorInfoSectionContentDTO,
    VisitorInfoItemDTO,
    OpeningHoursDTO,
    TipsSectionContentDTO,
    TipItemDTO,
    SocialVideosSectionContentDTO,
    SocialVideoItemDTO,
    NearbyAttractionsSectionContentDTO,
    NearbyAttractionItemDTO,
    AudienceProfileSectionContentDTO,
    AudienceProfileItemDTO,
)
from app.infrastructure.external_apis.nearby_attractions_fetcher import NearbyAttractionsFetcherImpl
from app.infrastructure.persistence.storage_functions import store_nearby_attractions
from app.config import settings


class AttractionDataService:
    """Service to assemble DTOs from persistence models."""

    def __init__(self):
        self.session_factory = SessionLocal
        self.weather_fetcher = WeatherFetcherImpl()
        self.logger = logging.getLogger(__name__)

    def _session(self):
        return self.session_factory()
    
    async def _fetch_and_store_weather_data(self, attraction: models.Attraction, session):
        """Fetch weather data using WeatherFetcher and store it in the database if missing."""
        try:
            # Check if we already have weather data for this attraction
            today_date = datetime.now().date()
            weather_exists = (
                session.query(models.WeatherForecast)
                .filter(
                    models.WeatherForecast.attraction_id == attraction.id,
                    models.WeatherForecast.date_local >= today_date
                )
                .first()
            )
            
            if weather_exists:
                self.logger.info(f"Weather data already exists for attraction {attraction.id}")
                return
            
            self.logger.info(f"No weather data found for attraction {attraction.id}, fetching from API...")
            
            # Fetch weather data using the weather fetcher
            timezone_str = attraction.timezone if hasattr(attraction, 'timezone') else "UTC"
            weather_data = await self.weather_fetcher.fetch(
                attraction_id=attraction.id,
                place_id=None,
                latitude=attraction.latitude if attraction.latitude else 0.0,
                longitude=attraction.longitude if attraction.longitude else 0.0,
                timezone_str=timezone_str,
                attraction_name=attraction.name,
                city_name=city_name,
                country=country
            )
            
            if not weather_data:
                self.logger.warning(f"Failed to fetch weather data for attraction {attraction.id}")
                return
            
            self.logger.info(f"Successfully fetched weather data for attraction {attraction.id}")
            
            # Store the current weather card data
            card_data = weather_data.get("card")
            if card_data:
                weather_forecast = models.WeatherForecast(
                    attraction_id=attraction.id,
                    date_local=card_data.get("date_local"),
                    temperature_c=card_data.get("temperature_c"),
                    feels_like_c=card_data.get("feels_like_c"),
                    min_temperature_c=card_data.get("min_temperature_c"),
                    max_temperature_c=card_data.get("max_temperature_c"),
                    condition=card_data.get("summary"),
                    precipitation_mm=card_data.get("precipitation_mm"),
                    wind_speed_kph=card_data.get("wind_speed_kph"),
                    humidity_percent=card_data.get("humidity_percent"),
                    icon_url=card_data.get("icon_url"),
                    source=weather_data.get("source", "openweathermap_api"),
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                session.add(weather_forecast)
            
            # Store forecast days if available
            forecast_days = weather_data.get("forecast_days", [])
            for forecast_day in forecast_days:
                day_card = forecast_day.get("card")
                if day_card:
                    forecast_forecast = models.WeatherForecast(
                        attraction_id=attraction.id,
                        date_local=day_card.get("date_local"),
                        temperature_c=day_card.get("temperature_c"),
                        feels_like_c=day_card.get("feels_like_c"),
                        min_temperature_c=day_card.get("min_temperature_c"),
                        max_temperature_c=day_card.get("max_temperature_c"),
                        condition=day_card.get("summary"),
                        precipitation_mm=day_card.get("precipitation_mm"),
                        wind_speed_kph=day_card.get("wind_speed_kph"),
                        humidity_percent=day_card.get("humidity_percent"),
                        icon_url=day_card.get("icon_url"),
                        source=weather_data.get("source", "openweathermap_api"),
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    session.add(forecast_forecast)
            
            session.commit()
            self.logger.info(f"Successfully stored weather data for attraction {attraction.id}")
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error fetching and storing weather data for attraction {attraction.id}: {e}")

    # -------- Page cards --------
    async def build_page_cards(self, attraction, city_name: Optional[str] = None, country: Optional[str] = None, timezone: Optional[str] = None) -> AttractionCardsDTO:
        """Hydrate AttractionCardsDTO from various tables."""
        try:
            session_ctx = self._session()
        except Exception:
            # Fallback to empty cards if DB not available
            return AttractionCardsDTO()

        try:
            with session_ctx as session:
                # Hero images (positions 0-9 only, proxy rejects higher)
                hero_rows = (
                    session.query(models.HeroImage)
                    .filter(
                        models.HeroImage.attraction_id == attraction.id,
                        models.HeroImage.position <= 9
                    )
                    .order_by(models.HeroImage.position.asc(), models.HeroImage.id.asc())
                    .all()
                )
                # Use proxy URL for on-demand fetching and GCS caching
                # Proxy endpoint: GET /api/v1/image/{attraction_id}/{position}
                # - Checks if image exists in GCS → redirect to CDN
                # - If not → fetches from Google Places → uploads to GCS → redirect
                hero_images = (
                    {"images": [
                        HeroImageDTO(
                            url=f"{settings.API_BASE_URL}/api/v1/image/{attraction.id}/{h.position}",
                            alt=h.alt_text,
                            position=h.position,
                            gcs_url_hero=h.gcs_url_hero,
                            gcs_url_card=h.gcs_url_card
                        ) for h in hero_rows
                    ]}
                    if hero_rows
                    else None
                )

                # Best time (card) - get today's data based on timezone
                from datetime import datetime, timezone as tz
                import pytz
                
                today_day_int = None
                if timezone:
                    try:
                        city_tz = pytz.timezone(timezone)
                        today_dt = datetime.now(city_tz)
                        today_day_int = today_dt.weekday()  # 0=Monday, 6=Sunday
                    except Exception:
                        today_day_int = datetime.now().weekday()
                else:
                    today_day_int = datetime.now().weekday()
                
                best_time_row = (
                    session.query(models.BestTimeData)
                    .filter(
                        models.BestTimeData.attraction_id == attraction.id,
                        models.BestTimeData.day_int == today_day_int,
                        models.BestTimeData.day_type == "regular"
                    )
                    .first()
                )
                best_time = None
                if best_time_row:
                    best_time = BestTimeCardDTO(
                        is_open_today=bool(best_time_row.is_open_today),
                        today_local_date=str(best_time_row.local_date) if best_time_row.local_date else "",
                        today_opening_hours_local=None,
                        today_opening_time=str(best_time_row.best_time_start_local)
                        if best_time_row.best_time_start_local
                        else None,
                        today_closing_time=str(best_time_row.best_time_end_local)
                        if best_time_row.best_time_end_local
                        else None,
                        crowd_level_today=best_time_row.crowd_level_today,
                        crowd_level_label_today=best_time_row.crowd_level_label_today,
                        best_time_today={
                            "start_local_time": str(best_time_row.best_time_start_local),
                            "end_local_time": str(best_time_row.best_time_end_local),
                        }
                        if best_time_row.best_time_start_local and best_time_row.best_time_end_local
                        else None,
                        best_time_text=best_time_row.reason_text,
                        summary_text=best_time_row.reason_text,
                    )

                # Weather: get all available data from today onwards based on timezone
                today_date = None
                if timezone:
                    try:
                        city_tz = pytz.timezone(timezone)
                        today_date = datetime.now(city_tz).date()
                    except Exception:
                        today_date = datetime.now().date()
                else:
                    today_date = datetime.now().date()

                # Get all weather data from today onwards (today through last available date in DB)
                weather_rows = (
                    session.query(models.WeatherForecast)
                    .filter(
                        models.WeatherForecast.attraction_id == attraction.id,
                        models.WeatherForecast.date_local >= today_date
                    )
                    .order_by(models.WeatherForecast.date_local.asc())
                    .all()
                )

                # If no weather data found from today onwards, try to fetch and store it
                if not weather_rows:
                    self.logger.info(f"No weather data found for attraction {attraction.id}, attempting to fetch...")
                    
                    # Try to fetch and store weather data
                    try:
                        await self._fetch_and_store_weather_data(attraction, session)
                        
                        # After fetching, try to get weather data again
                        weather_rows = (
                            session.query(models.WeatherForecast)
                            .filter(
                                models.WeatherForecast.attraction_id == attraction.id,
                                models.WeatherForecast.date_local >= today_date
                            )
                            .order_by(models.WeatherForecast.date_local.asc())
                            .all()
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to fetch weather data for attraction {attraction.id}: {e}")
                        
                        # If fetching fails, get the most recent available data
                        weather_rows = (
                            session.query(models.WeatherForecast)
                            .filter(models.WeatherForecast.attraction_id == attraction.id)
                            .order_by(models.WeatherForecast.date_local.desc())
                            .limit(7)
                            .all()
                        )
                        weather_rows = list(reversed(weather_rows))  # Sort ascending
                
                # Build weather card with first day's data (for backward compatibility)
                weather = None
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"Found {len(weather_rows)} weather rows for attraction {attraction.id}")
                
                if weather_rows:
                    weather_row = weather_rows[0]
                    logger.info(f"Using weather data for date: {weather_row.date_local}")
                    weather = WeatherCardDTO(
                        date_local=str(weather_row.date_local) if weather_row.date_local else "",
                        temperature_c=float(weather_row.temperature_c) if weather_row.temperature_c is not None else None,
                        feels_like_c=float(weather_row.feels_like_c) if weather_row.feels_like_c is not None else None,
                        min_temperature_c=float(weather_row.min_temperature_c) if weather_row.min_temperature_c is not None else None,
                        max_temperature_c=float(weather_row.max_temperature_c) if weather_row.max_temperature_c is not None else None,
                        condition=weather_row.condition if weather_row else None,
                        precipitation_mm=float(weather_row.precipitation_mm) if weather_row.precipitation_mm is not None else None,
                        wind_speed_kph=float(weather_row.wind_speed_kph) if weather_row.wind_speed_kph is not None else None,
                        humidity_percent=weather_row.humidity_percent if weather_row else None,
                        icon_url=weather_row.icon_url if weather_row else None,
                    )
                    logger.info(f"Weather DTO created: {weather}")
                else:
                    logger.warning(f"No weather data found for attraction {attraction.id}")
                    weather = WeatherCardDTO(
                        date_local="",
                        temperature_c=None,
                        feels_like_c=None,
                        min_temperature_c=None,
                        max_temperature_c=None,
                        condition=None,
                        precipitation_mm=None,
                        wind_speed_kph=None,
                        humidity_percent=None,
                        icon_url=None,
                    )

                # Social video from metadata
                metadata_row = (
                    session.query(models.AttractionMetadata)
                    .filter(models.AttractionMetadata.attraction_id == attraction.id)
                    .first()
                )
                social_video = None
                if metadata_row and metadata_row.social_video_embed_url:
                    social_video = SocialVideoCardDTO(
                        platform="youtube",
                        title=attraction.name,
                        embed_url=metadata_row.social_video_embed_url,
                        thumbnail_url=None,
                        source_url=metadata_row.social_video_embed_url,
                    )

                # Map card from map_snapshot
                map_row = (
                    session.query(models.MapSnapshot)
                    .filter(models.MapSnapshot.attraction_id == attraction.id)
                    .first()
                )
                map_card = None
                if map_row:
                    map_card = MapCardDTO(
                        latitude=float(map_row.latitude) if map_row.latitude is not None else 0.0,
                        longitude=float(map_row.longitude) if map_row.longitude is not None else 0.0,
                        static_map_image_url=map_row.static_map_url,
                        maps_link_url=map_row.directions_url,
                        address=map_row.address,
                    )

                # Review card (aggregate)
                review_card = ReviewCardDTO(
                    overall_rating=float(attraction.rating) if attraction.rating is not None else None,
                    rating_scale_max=5,
                    review_count=attraction.review_count,
                    summary_gemini=getattr(attraction, "summary_gemini", None),
                )

                # Tips card - get first safety tip and first two insider tips
                safety_tips = (
                    session.query(models.Tip)
                    .filter(models.Tip.attraction_id == attraction.id, models.Tip.tip_type == "SAFETY")
                    .order_by(models.Tip.id.asc())
                    .limit(1)
                    .all()
                )
                insider_tips = (
                    session.query(models.Tip)
                    .filter(models.Tip.attraction_id == attraction.id, models.Tip.tip_type == "INSIDER")
                    .order_by(models.Tip.id.asc())
                    .limit(2)
                    .all()
                )

                safety = [
                    TipDTO(id=tip.id, text=tip.text, source=tip.source)
                    for tip in safety_tips
                ]
                insider = [
                    TipDTO(id=tip.id, text=tip.text, source=tip.source)
                    for tip in insider_tips
                ]
                tips_card = TipsCardDTO(safety=safety, insider=insider) if (safety_tips or insider_tips) else None

                # About card from metadata extensions
                about_card = AboutCardDTO(
                    short_description=metadata_row.short_description if metadata_row else None,
                    recommended_duration_minutes=metadata_row.recommended_duration_minutes if metadata_row else None,
                    highlights=metadata_row.highlights if metadata_row else None,
                )

                # Nearby attraction card - pick first
                nearby_row = (
                    session.query(models.NearbyAttraction)
                    .filter(models.NearbyAttraction.attraction_id == attraction.id)
                    .order_by(models.NearbyAttraction.id.asc())
                    .first()
                )
                nearby_card = None
                if nearby_row:
                    nearby_card = NearbyAttractionCardDTO(
                        id=nearby_row.id,
                        slug=nearby_row.slug or "",
                        name=nearby_row.name,
                        distance_km=float(nearby_row.distance_km) if nearby_row.distance_km is not None else None,
                        walking_time_minutes=nearby_row.walking_time_minutes,
                        hero_image_url=nearby_row.image_url,
                    )

                return AttractionCardsDTO(
                    hero_images=hero_images,
                    best_time=best_time,
                    weather=weather,
                    social_video=social_video,
                    map=map_card,
                    review=review_card,
                    tips=tips_card,
                    about=about_card,
                    nearby_attraction=nearby_card,
                )
        except Exception:
            # If anything fails (e.g., tables not present), return empty cards
            return AttractionCardsDTO()

    # -------- Enrichment --------
    async def _enrich_google_places_images(
        self,
        nearby_items: List[NearbyAttractionItemDTO],
        attraction_id: int
    ) -> List[NearbyAttractionItemDTO]:
        """
        Enrich Google Places attractions with fresh images from Google Places API.

        Identifies attractions by external link URL and fetches fresh photo URLs.
        Caches images to GCS to avoid repeated API calls.

        Args:
            nearby_items: List of nearby attraction DTOs
            attraction_id: ID of the parent attraction (for GCS path)

        Returns:
            Enriched list with updated image_urls for Google Places attractions
        """
        from app.utils.google_places_utils import extract_place_id_from_link
        from app.infrastructure.external_apis.google_places_client import GooglePlacesClient
        from app.infrastructure.external_apis.gcs_client import gcs_client, image_processor
        import httpx

        places_client = GooglePlacesClient()

        for item in nearby_items:
            # Skip if already has GCS URL
            if item.gcs_url:
                item.image_url = item.gcs_url
                continue

            link = item.link

            # Check if it's a Google Places attraction (external link)
            is_google_place = link and isinstance(link, str) and "google.com/maps" in link

            if is_google_place and not item.image_url:
                # Extract place_id from link
                place_id = extract_place_id_from_link(link)

                if place_id:
                    try:
                        # Fetch fresh photo URL
                        fresh_image_url = await places_client.get_place_photo_url(
                            place_id=place_id,
                            max_width=800
                        )

                        if fresh_image_url:
                            # Download and cache to GCS
                            try:
                                async with httpx.AsyncClient() as client:
                                    resp = await client.get(fresh_image_url, timeout=30)
                                    if resp.status_code == 200:
                                        # Convert to WebP
                                        webp_bytes, _, _ = image_processor.process_image(
                                            resp.content, 800
                                        )

                                        # Upload to GCS
                                        nearby_key = item.id if item.id else hash(item.name)
                                        
                                        gcs_url = gcs_client.upload_nearby_attraction_image(
                                            attraction_id=attraction_id,
                                            nearby_attraction_id=nearby_key,
                                            image_bytes=webp_bytes
                                        )

                                        if gcs_url:
                                            item.gcs_url = gcs_url
                                            item.image_url = gcs_url

                                            # Update DB with GCS URL
                                            if item.id:
                                                self._update_nearby_gcs_url(item.id, gcs_url)
                                            
                                            self.logger.info(f"Cached nearby image for {item.name}: {gcs_url}")
                                        else:
                                            item.image_url = fresh_image_url
                                    else:
                                        item.image_url = fresh_image_url
                            except Exception as e:
                                self.logger.warning(f"Failed to cache nearby image: {e}")
                                item.image_url = fresh_image_url
                        else:
                            self.logger.debug(f"No fresh image available for {item.name}")
                    except Exception as e:
                        self.logger.error(f"Failed to enrich image for {item.name}: {e}")
                        # Keep existing image_url on error

        return nearby_items

    def _update_nearby_gcs_url(self, nearby_id: int, gcs_url: str):
        """Update nearby attraction with GCS URL."""
        if not nearby_id:
            return
        try:
            with self.session_factory() as session:
                session.query(models.NearbyAttraction).filter(
                    models.NearbyAttraction.id == nearby_id
                ).update({"gcs_url": gcs_url})
                session.commit()
        except Exception as e:
            self.logger.error(f"Failed to update nearby GCS URL: {e}")

    # -------- Sections --------
    async def build_sections(self, attraction, city_name: str, country: Optional[str], timezone: Optional[str] = None) -> List[SectionDTO]:
        try:
            session_ctx = self._session()
        except Exception:
            return []

        try:
            with session_ctx as session:
                sections: List[SectionDTO] = []

            # Calculate today's day_int based on timezone
            from datetime import datetime
            import pytz
            
            today_day_int = None
            if timezone:
                try:
                    city_tz = pytz.timezone(timezone)
                    today_dt = datetime.now(city_tz)
                    today_day_int = today_dt.weekday()  # 0=Monday, 6=Sunday
                except Exception:
                    today_day_int = datetime.now().weekday()
            else:
                today_day_int = datetime.now().weekday()

            # Best time section - get today's data by day_int
            best_time_row = (
                session.query(models.BestTimeData)
                .filter(
                    models.BestTimeData.attraction_id == attraction.id,
                    models.BestTimeData.day_int == today_day_int,
                    models.BestTimeData.day_type == "regular"
                )
                .first()
            )
            if best_time_row:
                tab = BestTimeTabDTO(
                    label="Today",
                    date=str(best_time_row.local_date),
                    chart_json=best_time_row.chart_json,
                    summary=best_time_row.reason_text,
                )
                sections.append(
                    SectionDTO(
                        section_type="best_time",
                            title="Best Time to Visit",
                            subtitle="Plan your visit for the best experience",
                            layout="tabs",
                        is_visible=True,
                        order=1,
                        content=BestTimeSectionContentDTO(tabs=[tab], default_tab="Today"),
                    )
                )

            # Reviews section (top 5)
            review_rows = (
                session.query(models.Review)
                .filter(models.Review.attraction_id == attraction.id)
                .order_by(models.Review.id.asc())
                .limit(5)
                .all()
            )
            if review_rows:
                items = [
                    ReviewItemDTO(
                        author_name=r.author_name,
                        author_url=r.author_url,
                        author_photo_url=r.author_photo_url,
                        rating=int(r.rating) if r.rating is not None else 0,
                        text=r.text,
                        time=r.time.isoformat() if isinstance(r.time, datetime) else None,
                        source=r.source or "Google",
                    )
                    for r in review_rows
                ]
                sections.append(
                    SectionDTO(
                        section_type="reviews",
                            title="Reviews",
                            subtitle="What visitors are saying",
                            layout="grid",
                        is_visible=True,
                        order=2,
                        content=ReviewsSectionContentDTO(
                            overall_rating=float(attraction.rating) if attraction.rating is not None else None,
                            rating_scale_max=5,
                            total_reviews=attraction.review_count,
                            summary="Visitors love the iconic views",
                            items=items,
                        ),
                    )
                )

            # Widgets section
            widget_row = (
                session.query(models.WidgetConfig)
                .filter(models.WidgetConfig.attraction_id == attraction.id)
                .first()
            )
            if widget_row and (widget_row.widget_primary or widget_row.widget_secondary):
                sections.append(
                    SectionDTO(
                        section_type="widgets",
                            title="Widgets",
                            subtitle=None,
                            layout="default",
                        is_visible=True,
                        order=3,
                        content=WidgetSectionContentDTO(
                            html=None,
                            custom_config={
                                "widget_primary": widget_row.widget_primary,
                                "widget_secondary": widget_row.widget_secondary,
                            },
                        ),
                    )
                )

            # Map section
            map_row = (
                session.query(models.MapSnapshot)
                .filter(models.MapSnapshot.attraction_id == attraction.id)
                .first()
            )
            if map_row:
                sections.append(
                    SectionDTO(
                        section_type="map",
                            title="Location & Directions",
                            subtitle="Find your way to the landmark",
                            layout="full_width",
                        is_visible=True,
                        order=4,
                        content=MapSectionContentDTO(
                            latitude=float(map_row.latitude) if map_row.latitude is not None else 0.0,
                            longitude=float(map_row.longitude) if map_row.longitude is not None else 0.0,
                            address=map_row.address,
                            directions_url=map_row.directions_url,
                            zoom_level=map_row.zoom_level,
                        ),
                    )
                )

            # Visitor info section
            metadata_row = (
                session.query(models.AttractionMetadata)
                .filter(models.AttractionMetadata.attraction_id == attraction.id)
                .first()
            )
            if metadata_row:
                contact_items = []
                contact_info = metadata_row.contact_info or {}
                
                # Handle new contact_info structure: {phone: {value, url}, email: {value, url}, website: {value, url}}
                if isinstance(contact_info, dict):
                    for key in ['phone', 'email', 'website']:
                        if key in contact_info and contact_info[key]:
                            item_data = contact_info[key]
                            if isinstance(item_data, dict):
                                contact_items.append(VisitorInfoItemDTO(
                                    label=key.title(),
                                    value=item_data.get('value', ''),
                                    url=item_data.get('url')
                                ))

                opening_hours = []
                hours_data = metadata_row.opening_hours or []
                
                # Handle opening_hours as list of objects: [{day, open_time, close_time, is_closed}, ...]
                if isinstance(hours_data, list):
                    for day_info in hours_data:
                        if isinstance(day_info, dict):
                            opening_hours.append(
                                OpeningHoursDTO(
                                    day=day_info.get('day', ''),
                                    open_time=day_info.get('open_time'),
                                    close_time=day_info.get('close_time'),
                                    is_closed=day_info.get('is_closed', False)
                                )
                            )

                sections.append(
                    SectionDTO(
                        section_type="visitor_info",
                        title="Visitor Information",
                        subtitle=None,
                        layout="two_column",
                        is_visible=True,
                        order=5,
                        content=VisitorInfoSectionContentDTO(
                            contact_items=contact_items,
                            opening_hours=opening_hours,
                            accessibility_info=metadata_row.accessibility_info,
                            best_season=metadata_row.best_season,
                        ),
                    )
                )

                # Tips section - get all tips
                all_tip_rows = (
                    session.query(models.Tip)
                    .filter(models.Tip.attraction_id == attraction.id)
                    .order_by(models.Tip.id.asc())
                    .all()
                )
                if all_tip_rows:
                    tip_items_safety = [
                        TipItemDTO(id=t.id, text=t.text, source=t.source) for t in all_tip_rows if t.tip_type == "SAFETY"
                    ]
                    tip_items_insider = [
                        TipItemDTO(id=t.id, text=t.text, source=t.source) for t in all_tip_rows if t.tip_type == "INSIDER"
                    ]
                    sections.append(
                        SectionDTO(
                            section_type="tips",
                            title="Tips & Insights",
                            subtitle="Practical tips for a smoother visit",
                            layout="two_column",
                            is_visible=True,
                            order=6,
                            content=TipsSectionContentDTO(safety=tip_items_safety, insider=tip_items_insider),
                        )
                    )

                # Social videos section
                video_rows = (
                    session.query(models.SocialVideo)
                    .filter(models.SocialVideo.attraction_id == attraction.id)
                    .order_by(models.SocialVideo.position.asc())
                    .all()
                )
                if video_rows:
                    video_items = [
                        SocialVideoItemDTO(
                            id=v.id,
                            platform=v.platform,
                            title=v.title,
                            embed_url=v.embed_url,
                            thumbnail_url=v.thumbnail_url,
                            duration_seconds=v.duration_seconds
                        )
                        for v in video_rows
                    ]
                    sections.append(
                        SectionDTO(
                            section_type="social_videos",
                            title="Videos",
                            subtitle=None,
                            layout="grid",
                            is_visible=True,
                            order=7,
                            content=SocialVideosSectionContentDTO(items=video_items),
                        )
                    )

            # Nearby attractions section - HYBRID APPROACH
            # 1. Query DB first (fast path)
            # 2. If fewer than target count, call fetcher to get more from Google Places
            # 3. Persist results to DB for future requests (cache-on-read)
            target_count = settings.NEARBY_ATTRACTIONS_COUNT  # 10
            logger = logging.getLogger(__name__)

            nearby_rows = (
                session.query(models.NearbyAttraction)
                .filter(models.NearbyAttraction.attraction_id == attraction.id)
                .order_by(models.NearbyAttraction.id.asc())
                .limit(target_count)
                .all()
            )

            db_count = len(nearby_rows)
            logger.info(f"Found {db_count} nearby attractions in DB for {attraction.name}")

            nearby_items = []

            # If insufficient results AND we have coordinates, call fetcher
            if db_count < target_count and attraction.latitude and attraction.longitude:
                logger.info(f"DB has {db_count}/{target_count}, calling fetcher for more...")

                try:
                    fetcher = NearbyAttractionsFetcherImpl()
                    fetcher_result = await fetcher.fetch(
                        attraction_id=attraction.id,
                        attraction_name=attraction.name,
                        city_name=city_name,
                        latitude=float(attraction.latitude),
                        longitude=float(attraction.longitude),
                        place_id=attraction.place_id,
                        force_google=False
                    )

                    if fetcher_result:
                        section_items = fetcher_result.get('section', {}).get('items', [])
                        nearby_list = fetcher_result.get('nearby', [])

                        # Convert fetcher items to DTOs
                        for item in section_items[:target_count]:
                            nearby_items.append(
                                NearbyAttractionItemDTO(
                                    id=item.get('id') or 0,
                                    slug=item.get('slug'),
                                    name=item.get('name'),
                                    distance_text=item.get('distance_text'),
                                    distance_km=item.get('distance_km'),
                                    rating=item.get('rating'),
                                    user_ratings_total=item.get('user_ratings_total'),
                                    review_count=item.get('review_count'),
                                    image_url=item.get('image_url'),
                                    link=item.get('link'),
                                    vicinity=item.get('vicinity'),
                                    audience_type=item.get('audience_type'),
                                    audience_text=item.get('audience_text'),
                                )
                            )

                        # Persist to database for future requests (cache-on-read)
                        if nearby_list and len(nearby_list) > db_count:
                            try:
                                store_nearby_attractions(attraction.id, nearby_list)
                                logger.info(f"Persisted {len(nearby_list)} nearby attractions to DB")
                            except Exception as e:
                                logger.error(f"Failed to persist nearby attractions: {e}")

                except Exception as e:
                    logger.error(f"Fetcher failed, falling back to DB results: {e}")

            # If fetcher wasn't called or failed, use existing DB results
            if not nearby_items and nearby_rows:
                for n in nearby_rows:
                    # Start with nearby attraction data
                    image_url = n.gcs_url or n.image_url
                    rating = float(n.rating) if n.rating is not None else None
                    review_count = n.review_count

                    # Try to fetch missing data from attractions table
                    nearby_attr = None

                    # First try by nearby_attraction_id
                    if n.nearby_attraction_id:
                        nearby_attr = (
                            session.query(models.Attraction)
                            .filter(models.Attraction.id == n.nearby_attraction_id)
                            .first()
                        )

                    # Fallback: try by slug if nearby_attraction_id is null
                    if not nearby_attr and n.slug:
                        nearby_attr = (
                            session.query(models.Attraction)
                            .filter(models.Attraction.slug == n.slug)
                            .first()
                        )
                        if nearby_attr:
                            logger.info(f"Found nearby attraction by slug: {n.slug} (id: {nearby_attr.id})")

                    if nearby_attr:
                        # Fill in missing image from hero_images table
                        if image_url is None:
                            hero_image = (
                                session.query(models.HeroImage)
                                .filter(models.HeroImage.attraction_id == nearby_attr.id)
                                .order_by(models.HeroImage.position.asc())
                                .first()
                            )
                            if hero_image:
                                # Use GCS URL if available, fall back to original URL
                                image_url = hero_image.gcs_url_hero or hero_image.gcs_url_card
                                logger.info(f"Fetched hero image for {n.name}: {image_url}")
                            else:
                                logger.warning(f"No hero image found for {n.name} (attraction_id: {nearby_attr.id})")

                        # Fill in missing rating
                        if rating is None and nearby_attr.rating is not None:
                            rating = float(nearby_attr.rating)

                        # Fill in missing review count
                        if review_count is None and nearby_attr.review_count is not None:
                            review_count = nearby_attr.review_count

                    nearby_items.append(
                        NearbyAttractionItemDTO(
                            id=n.id,
                            slug=n.slug,
                            name=n.name,
                            distance_text=n.distance_text,
                            distance_km=float(n.distance_km) if n.distance_km is not None else None,
                            rating=rating,
                            user_ratings_total=n.user_ratings_total,
                            review_count=review_count,
                            image_url=image_url,
                            link=n.link,
                            vicinity=n.vicinity,
                            audience_type=n.audience_type,
                            audience_text=n.audience_text,
                        )
                    )

            # Enrich and add section
            if nearby_items:
                nearby_items = await self._enrich_google_places_images(nearby_items, attraction.id)

                sections.append(
                    SectionDTO(
                        section_type="nearby_attractions",
                        title="Nearby Attractions",
                        subtitle=None,
                        layout="grid",
                        is_visible=True,
                        order=8,
                        content=NearbyAttractionsSectionContentDTO(items=nearby_items),
                    )
                )

            # Audience profile - derive from nearby audience_type/text if present
            # Audience profiles: prefer explicit table, fallback to nearby
            audience_rows = (
                session.query(models.AudienceProfile)
                .filter(models.AudienceProfile.attraction_id == attraction.id)
                .order_by(models.AudienceProfile.id.asc())
                .all()
            )
            if audience_rows:
                audience_items = [
                    AudienceProfileItemDTO(
                        audience_type=a.audience_type,
                        description=a.description or "",
                        emoji=a.emoji,
                    )
                    for a in audience_rows
                ]
            else:
                audience_items = [
                    AudienceProfileItemDTO(
                        audience_type=n.audience_type,
                        description=n.audience_text or "",
                        emoji=None,
                    )
                    for n in nearby_rows
                    if n.audience_type
                ]
            if audience_items:
                sections.append(
                    SectionDTO(
                        section_type="audience_profiles",
                        title="Who This Is For",
                        subtitle=None,
                        layout="cards",
                        is_visible=True,
                        order=9,
                        content=AudienceProfileSectionContentDTO(items=audience_items),
                    )
                )

                return sections
        except Exception:
            return []

    async def build_page_dto(self, attraction, city_name: str, country: Optional[str]) -> AttractionPageDTO:
        """Assemble full page DTO."""
        
        # Determine timezone
        timezone = None
        if hasattr(attraction, 'city') and attraction.city:
            timezone = attraction.city.timezone
        elif hasattr(attraction, 'timezone'): # Future proof for entity enrichment
            timezone = attraction.timezone
            
        cards = await self.build_page_cards(attraction, city_name=city_name, country=country, timezone=timezone)

        # Get nearby attractions with enriched data
        session = self._session()
        nearby_attractions = []
        best_time_data = None
        social_videos_data = None
        visitor_info_data = None
        audience_profiles_data = None

        try:
            with session as s:
                import logging
                logger = logging.getLogger(__name__)

                # Get best_time data (all days for BestTimesSection)
                best_time_rows = (
                    s.query(models.BestTimeData)
                    .filter(models.BestTimeData.attraction_id == attraction.id)
                    .order_by(models.BestTimeData.day_int.asc())
                    .all()
                )

                if best_time_rows:
                    regular_days = []
                    special_days = []

                    for row in best_time_rows:
                        day_data = {
                            "day_name": row.day_name,
                            "day_int": row.day_int,
                            "local_date": str(row.date_local) if row.date_local else None,
                            "is_open": row.is_open_today,
                            "opening_time": str(row.today_opening_time) if row.today_opening_time else None,
                            "closing_time": str(row.today_closing_time) if row.today_closing_time else None,
                            "crowd_level": row.crowd_level_today,
                            "best_time_text": row.reason_text,
                            "hourly_crowd_levels": row.hourly_crowd_levels,
                        }

                        if row.day_type == "regular":
                            regular_days.append(day_data)
                        else:
                            special_days.append(day_data)

                    best_time_data = {
                        "regular_days": regular_days,
                        "special_days": special_days,
                    }

                # Get social videos
                social_video_rows = (
                    s.query(models.SocialVideo)
                    .filter(models.SocialVideo.attraction_id == attraction.id)
                    .order_by(models.SocialVideo.position.asc())
                    .all()
                )

                if social_video_rows:
                    social_videos_data = [
                        {
                            "id": v.id,
                            "platform": v.platform,
                            "title": v.title,
                            "embed_url": v.embed_url,
                            "thumbnail_url": v.thumbnail_url,
                            "duration_seconds": v.duration_seconds,
                        }
                        for v in social_video_rows
                    ]

                # Get visitor info from metadata
                metadata_row = (
                    s.query(models.AttractionMetadata)
                    .filter(models.AttractionMetadata.attraction_id == attraction.id)
                    .first()
                )

                if metadata_row:
                    contact_info = metadata_row.contact_info or {}
                    opening_hours = metadata_row.opening_hours or []

                    visitor_info_data = {
                        "contact_info": contact_info,
                        "opening_hours": opening_hours,
                        "accessibility_info": metadata_row.accessibility_info,
                        "best_season": metadata_row.best_season,
                    }

                # Get audience profiles
                audience_rows = (
                    s.query(models.AudienceProfile)
                    .filter(models.AudienceProfile.attraction_id == attraction.id)
                    .order_by(models.AudienceProfile.id.asc())
                    .all()
                )

                if audience_rows:
                    audience_profiles_data = [
                        {
                            "audience_type": a.audience_type,
                            "description": a.description or "",
                            "emoji": a.emoji,
                        }
                        for a in audience_rows
                    ]

                # Get nearby attractions with enriched data
                nearby_rows = (
                    s.query(models.NearbyAttraction)
                    .filter(models.NearbyAttraction.attraction_id == attraction.id)
                    .order_by(models.NearbyAttraction.id.asc())
                    .all()
                )

                for n in nearby_rows:
                    image_url = n.image_url
                    rating = float(n.rating) if n.rating is not None else None
                    review_count = n.review_count

                    logger.info(f"Processing nearby attraction: {n.name} (slug: {n.slug}, nearby_id: {n.nearby_attraction_id}, image: {image_url})")

                    # Try to fetch missing data from attractions table
                    nearby_attr = None

                    # First try by nearby_attraction_id
                    if n.nearby_attraction_id:
                        nearby_attr = (
                            s.query(models.Attraction)
                            .filter(models.Attraction.id == n.nearby_attraction_id)
                            .first()
                        )
                        if nearby_attr:
                            logger.info(f"Found nearby attraction by ID: {nearby_attr.slug}")

                    # Fallback: try by slug if nearby_attraction_id is null
                    if not nearby_attr and n.slug:
                        nearby_attr = (
                            s.query(models.Attraction)
                            .filter(models.Attraction.slug == n.slug)
                            .first()
                        )
                        if nearby_attr:
                            logger.info(f"Found nearby attraction by slug: {n.slug} (id: {nearby_attr.id})")

                    if nearby_attr:
                        # Fill in missing image from hero_images table
                        if image_url is None:
                            hero_image = (
                                s.query(models.HeroImage)
                                .filter(models.HeroImage.attraction_id == nearby_attr.id)
                                .order_by(models.HeroImage.position.asc())
                                .first()
                            )
                            if hero_image:
                                # Use GCS URL if available, fall back to original URL
                                image_url = hero_image.gcs_url_hero or hero_image.gcs_url_card
                                logger.info(f"Fetched hero image for {n.name}: {image_url}")
                            else:
                                logger.warning(f"No hero image found for {n.name} (attraction_id: {nearby_attr.id})")

                        # Fill in missing rating
                        if rating is None and nearby_attr.rating is not None:
                            rating = float(nearby_attr.rating)

                        # Fill in missing review count
                        if review_count is None and nearby_attr.review_count is not None:
                            review_count = nearby_attr.review_count
                    else:
                        logger.warning(f"Could not find attraction for nearby: {n.name} (slug: {n.slug}, nearby_id: {n.nearby_attraction_id})")

                    nearby_attractions.append({
                        "name": n.name,
                        "slug": n.slug,
                        "link": n.link,
                        "distance_km": float(n.distance_km) if n.distance_km is not None else None,
                        "distance_text": n.distance_text,
                        "walking_time_minutes": n.walking_time_minutes,
                        "image_url": image_url,
                        "rating": rating,
                        "review_count": review_count,
                        "vicinity": n.vicinity,
                    })
        except Exception as e:
            logger = __import__('logging').getLogger(__name__)
            logger.error(f"Error building page DTO for {attraction.slug}: {e}")

        return AttractionPageDTO(
            attraction_id=attraction.id,
            slug=attraction.slug,
            name=attraction.name,
            city=city_name,
            country=country,
            timezone=timezone,
            cards=cards,
            best_time=best_time_data,
            nearby_attractions=nearby_attractions if nearby_attractions else None,
            social_videos=social_videos_data,
            visitor_info=visitor_info_data,
            audience_profiles=audience_profiles_data,
        )

    async def build_sections_dto(self, attraction, city_name: str, country: Optional[str]) -> AttractionSectionsDTO:
        """Assemble sections DTO."""
        # Determine timezone
        timezone = None
        if hasattr(attraction, 'city') and attraction.city:
            timezone = attraction.city.timezone
            
        sections = await self.build_sections(attraction, city_name, country, timezone=timezone)
        return AttractionSectionsDTO(
            attraction_id=attraction.id,
            slug=attraction.slug,
            name=attraction.name,
            city=city_name,
            country=country,
            sections=sections,
        )


