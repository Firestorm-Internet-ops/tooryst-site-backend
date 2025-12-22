"""SQLAlchemy models matching the create_schema.sql tables."""
from sqlalchemy import (
    Column,
    BigInteger,
    String,
    Boolean,
    DECIMAL,
    Integer,
    Text,
    Date,
    Time,
    DateTime,
    JSON,
    Enum,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from app.infrastructure.persistence.db import Base


class AudienceProfile(Base):
    __tablename__ = "audience_profiles"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    audience_type = Column(String(64), nullable=False)
    description = Column(Text)
    emoji = Column(String(16))
    created_at = Column(DateTime)

    attraction = relationship("Attraction")
class City(Base):
    __tablename__ = "cities"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    slug = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    country = Column(String(255))
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))
    timezone = Column(String(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    attractions = relationship("Attraction", back_populates="city")


class Attraction(Base):
    __tablename__ = "attractions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    city_id = Column(BigInteger, ForeignKey("cities.id"), nullable=False, index=True)
    slug = Column(String(255), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    resolved_name = Column(String(255))
    place_id = Column(String(255))
    rating = Column(DECIMAL(3, 2))
    review_count = Column(Integer)
    summary_gemini = Column(Text)
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))
    address = Column(String(512))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    city = relationship("City", back_populates="attractions")
    hero_images = relationship("HeroImage", back_populates="attraction")
    best_time_entries = relationship("BestTimeData", back_populates="attraction")
    reviews = relationship("Review", back_populates="attraction")
    tips = relationship("Tip", back_populates="attraction")
    map_snapshot = relationship("MapSnapshot", back_populates="attraction", uselist=False)
    nearby_attractions = relationship(
        "NearbyAttraction", 
        back_populates="attraction",
        primaryjoin="and_(Attraction.id == NearbyAttraction.attraction_id)"
    )
    widget_config = relationship("WidgetConfig", back_populates="attraction", uselist=False)
    metadata_entry = relationship("AttractionMetadata", back_populates="attraction", uselist=False)


class HeroImage(Base):
    __tablename__ = "hero_images"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    url = Column(String(1024), nullable=False)
    alt_text = Column(String(512))
    position = Column(Integer, default=0)
    created_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="hero_images")


class BestTimeData(Base):
    __tablename__ = "best_time_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    day_type = Column(Enum("regular", "special"), nullable=False, default="regular")
    date_local = Column(Date)
    day_int = Column(Integer)
    day_name = Column(String(16), nullable=False)
    # Card data
    is_open_today = Column(Boolean, nullable=False, default=False)
    today_opening_time = Column(Time)
    today_closing_time = Column(Time)
    crowd_level_today = Column(Integer)
    best_time_today = Column(String(64))
    # Section data
    reason_text = Column(String(1024))
    hourly_crowd_levels = Column(JSON)
    # Metadata
    data_source = Column(String(32), default='besttime')
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="best_time_entries")


class Review(Base):
    __tablename__ = "reviews"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    author_name = Column(String(255), nullable=False)
    author_url = Column(String(512))
    author_photo_url = Column(String(512))
    rating = Column(Integer, nullable=False)
    text = Column(Text)
    time = Column(DateTime)
    source = Column(String(64), default="Google")
    created_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="reviews")


class Tip(Base):
    __tablename__ = "tips"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    tip_type = Column(Enum("SAFETY", "INSIDER"), nullable=False)
    text = Column(Text, nullable=False)
    source = Column(String(128))
    scope = Column(Enum("attraction", "city"), nullable=False, default="attraction")
    position = Column(Integer, default=1)  # 0 for prominent, 1 for detailed
    created_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="tips")


class MapSnapshot(Base):
    __tablename__ = "map_snapshot"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False)
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))
    address = Column(String(512))
    directions_url = Column(String(1024))
    static_map_url = Column(String(1024))
    zoom_level = Column(Integer)
    created_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="map_snapshot")


class NearbyAttraction(Base):
    __tablename__ = "nearby_attractions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    nearby_attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(255))
    place_id = Column(String(255))
    rating = Column(DECIMAL(3, 2))
    user_ratings_total = Column(Integer)
    review_count = Column(Integer)
    image_url = Column(String(1024))
    link = Column(String(1024))
    vicinity = Column(String(255))
    distance_text = Column(String(64))
    distance_km = Column(DECIMAL(6, 3))
    walking_time_minutes = Column(Integer)
    audience_type = Column(String(64))
    audience_text = Column(String(255))
    created_at = Column(DateTime)

    attraction = relationship("Attraction", foreign_keys=[attraction_id], back_populates="nearby_attractions")
    nearby_attraction = relationship("Attraction", foreign_keys=[nearby_attraction_id])


class WidgetConfig(Base):
    __tablename__ = "widget_config"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, unique=True)
    widget_primary = Column(Text)
    widget_secondary = Column(Text)
    created_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="widget_config")


class AttractionMetadata(Base):
    __tablename__ = "attraction_metadata"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, unique=True)
    contact_info = Column(JSON)
    accessibility_info = Column(Text)
    best_season = Column(Text)
    opening_hours = Column(JSON)
    short_description = Column(Text)
    recommended_duration_minutes = Column(Integer)
    highlights = Column(JSON)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    attraction = relationship("Attraction", back_populates="metadata_entry")


# Weather forecast table
class WeatherForecast(Base):
    __tablename__ = "weather_forecast"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    date_local = Column(Date, nullable=False)
    # Card data (all weather fields)
    temperature_c = Column(Integer)
    feels_like_c = Column(Integer)
    min_temperature_c = Column(Integer)
    max_temperature_c = Column(Integer)
    summary = Column(String(255))
    precipitation_mm = Column(DECIMAL(6, 1))
    wind_speed_kph = Column(Integer)
    humidity_percent = Column(Integer)
    icon_url = Column(String(1024))
    # Metadata
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    attraction = relationship("Attraction")




class SocialVideo(Base):
    __tablename__ = "social_videos"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attraction_id = Column(BigInteger, ForeignKey("attractions.id"), nullable=False, index=True)
    video_id = Column(String(255), nullable=False)
    platform = Column(String(32), nullable=False, default="youtube")
    title = Column(String(512))
    embed_url = Column(String(1024))
    thumbnail_url = Column(String(1024))
    watch_url = Column(String(1024))
    duration_seconds = Column(Integer)
    view_count = Column(BigInteger)
    channel_title = Column(String(255))
    position = Column(Integer, default=0)
    created_at = Column(DateTime)

    attraction = relationship("Attraction")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    started_at = Column(DateTime)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(32), nullable=False, default="running")
    attractions_processed = Column(Integer, default=0)
    attractions_succeeded = Column(Integer, default=0)
    attractions_failed = Column(Integer, default=0)
    attractions_completed = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    pipeline_metadata = Column("metadata", JSON, nullable=True)
    updated_at = Column(DateTime)


class ContactSubmission(Base):
    __tablename__ = "contact_submissions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    subject = Column(String(512))
    message = Column(Text, nullable=False)
    status = Column(Enum("new", "read", "responded"), nullable=False, default="new")
    created_at = Column(DateTime)
