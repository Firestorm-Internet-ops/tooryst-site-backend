"""Storage functions for pipeline data persistence.

This module contains all database storage functions used by the pipeline tasks.
Each function handles inserting/updating data from external API fetchers.

Previously located at: scripts/db_helper.py (moved for clean architecture)
"""
import os
import pymysql
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging
from dotenv import load_dotenv

from app.config import settings
from app.core.notifications import notification_manager, AlertType, AlertSeverity

load_dotenv()
logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection."""
    config = {
        'host': os.getenv('DATABASE_HOST', 'localhost'),
        'port': int(os.getenv('DATABASE_PORT', 3306)),
        'user': os.getenv('DATABASE_USER', 'root'),
        'password': os.getenv('DATABASE_PASSWORD', ''),
        'database': os.getenv('DATABASE_NAME', 'storyboard'),
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    try:
        return pymysql.connect(**config)
    except pymysql.Error as e:
        logger.error(f"Database connection error: {e}")

        # Send notification for database connection error
        notification_manager.send_alert(
            alert_type=AlertType.DATABASE_ERROR,
            severity=AlertSeverity.CRITICAL,
            title="Database Connection Failed",
            message=f"Failed to connect to database.\n\nError: {str(e)}",
            metadata={
                "host": config['host'],
                "port": config['port'],
                "database": config['database'],
                "error_type": type(e).__name__
            }
        )

        raise


def get_all_attractions() -> List[Dict[str, Any]]:
    """Get all attractions from database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    a.id, a.slug, a.name, a.place_id,
                    a.latitude, a.longitude,
                    c.name as city_name, c.country, c.timezone
                FROM attractions a
                JOIN cities c ON a.city_id = c.id
                ORDER BY a.id
            """)
            attractions = cursor.fetchall()
            logger.info(f"✓ Found {len(attractions)} attractions")
            return attractions
    except pymysql.Error as e:
        logger.error(f"Failed to get attractions: {e}")

        # Send notification for database query error
        notification_manager.send_alert(
            alert_type=AlertType.DATABASE_ERROR,
            severity=AlertSeverity.ERROR,
            title="Database Query Failed",
            message=f"Failed to query attractions from database.\n\nError: {str(e)}",
            metadata={
                "operation": "get_all_attractions",
                "error_type": type(e).__name__
            }
        )

        return []
    except Exception as e:
        logger.error(f"Unexpected error getting attractions: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_or_create_test_attraction() -> Optional[int]:
    """Get or create test attraction (Eiffel Tower) and return its ID."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # First, ensure Paris city exists
            cursor.execute("""
                INSERT INTO cities (slug, name, country, latitude, longitude)
                VALUES ('paris', 'Paris', 'France', 48.8566, 2.3522)
                ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)
            """)
            cursor.execute("SELECT LAST_INSERT_ID() as id")
            city_id = cursor.fetchone()['id']

            # Then, ensure Eiffel Tower attraction exists
            cursor.execute("""
                INSERT INTO attractions (
                    city_id, slug, name, place_id,
                    latitude, longitude
                )
                VALUES (
                    %s, 'eiffel-tower', 'Eiffel Tower',
                    'ChIJLU7jZClu5kcR4PcOOO6p3I0',
                    48.8584, 2.2945
                )
                ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)
            """, (city_id,))

            cursor.execute("SELECT LAST_INSERT_ID() as id")
            attraction_id = cursor.fetchone()['id']

            conn.commit()
            logger.info(f"✓ Test attraction ID: {attraction_id}")
            return attraction_id
    except Exception as e:
        logger.error(f"Failed to get/create test attraction: {e}")
        return None
    finally:
        conn.close()


def store_hero_images(attraction_id: int, images: List[Dict[str, Any]]) -> bool:
    """Store hero images in database with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Delete existing images (with lock held)
            cursor.execute(
                "DELETE FROM hero_images WHERE attraction_id = %s",
                (attraction_id,)
            )

            # Insert new images (with lock held)
            for image in images:
                cursor.execute("""
                    INSERT INTO hero_images (
                        attraction_id, url, alt_text, position
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    attraction_id,
                    image.get('url'),
                    image.get('alt'),
                    image.get('position')
                ))

            conn.commit()  # Releases lock
            logger.info(f"✓ Stored {len(images)} hero images")
            return True
    except Exception as e:
        logger.error(f"Failed to store hero images: {e}")
        return False
    finally:
        conn.close()


def store_best_time_data(attraction_id: int, days_data: List[Dict[str, Any]]) -> bool:
    """Store best time data with support for regular and special days."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            for day in days_data:
                card = day.get('card', {})
                section = day.get('section', {})

                # Convert hourly_crowd_levels to JSON string
                import json
                hourly_json = json.dumps(section.get('hourly_crowd_levels', []))

                cursor.execute("""
                    INSERT INTO best_time_data (
                        attraction_id, day_type, date_local, day_int, day_name,
                        is_open_today, today_opening_time, today_closing_time,
                        crowd_level_today, best_time_today,
                        reason_text, hourly_crowd_levels, data_source
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        day_name = VALUES(day_name),
                        is_open_today = VALUES(is_open_today),
                        today_opening_time = VALUES(today_opening_time),
                        today_closing_time = VALUES(today_closing_time),
                        crowd_level_today = VALUES(crowd_level_today),
                        best_time_today = VALUES(best_time_today),
                        reason_text = VALUES(reason_text),
                        hourly_crowd_levels = VALUES(hourly_crowd_levels),
                        data_source = VALUES(data_source),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    attraction_id,
                    day.get('day_type', 'regular'),  # Default to regular if not specified
                    day.get('date_local'),  # NULL for regular days
                    day.get('day_int'),     # NULL for special days
                    day.get('day_name'),
                    card.get('is_open_today'),
                    card.get('today_opening_time'),
                    card.get('today_closing_time'),
                    card.get('crowd_level_today'),
                    card.get('best_time_today'),
                    section.get('reason_text'),
                    hourly_json,
                    day.get('data_source', 'besttime')
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(days_data)} days of best time data")
            return True
    except Exception as e:
        logger.error(f"Failed to store best time data: {e}")
        return False
    finally:
        conn.close()


def store_weather_forecast(attraction_id: int, forecast_days: List[Dict[str, Any]]) -> bool:
    """Store weather forecast data with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            for day in forecast_days:
                card = day.get('card', {})

                cursor.execute("""
                    INSERT INTO weather_forecast (
                        attraction_id, date_local,
                        temperature_c, feels_like_c,
                        min_temperature_c, max_temperature_c,
                        summary, precipitation_mm,
                        wind_speed_kph, humidity_percent,
                        icon_url
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        temperature_c = VALUES(temperature_c),
                        feels_like_c = VALUES(feels_like_c),
                        min_temperature_c = VALUES(min_temperature_c),
                        max_temperature_c = VALUES(max_temperature_c),
                        summary = VALUES(summary),
                        precipitation_mm = VALUES(precipitation_mm),
                        wind_speed_kph = VALUES(wind_speed_kph),
                        humidity_percent = VALUES(humidity_percent),
                        icon_url = VALUES(icon_url),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    attraction_id,
                    day.get('date'),
                    card.get('temperature_c'),
                    card.get('feels_like_c'),
                    card.get('min_temperature_c'),
                    card.get('max_temperature_c'),
                    card.get('summary'),
                    card.get('precipitation_mm'),
                    card.get('wind_speed_kph'),
                    card.get('humidity_percent'),
                    card.get('icon_url')
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(forecast_days)} days of weather forecast")
            return True
    except Exception as e:
        logger.error(f"Failed to store weather forecast: {e}")
        return False
    finally:
        conn.close()


def store_map_snapshot(attraction_id: int, card: Dict[str, Any], section: Dict[str, Any]) -> bool:
    """Store map snapshot data with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            cursor.execute("""
                INSERT INTO map_snapshot (
                    attraction_id, latitude, longitude,
                    static_map_url, directions_url,
                    address, zoom_level
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    latitude = VALUES(latitude),
                    longitude = VALUES(longitude),
                    static_map_url = VALUES(static_map_url),
                    directions_url = VALUES(directions_url),
                    address = VALUES(address),
                    zoom_level = VALUES(zoom_level)
            """, (
                attraction_id,
                card.get('latitude'),
                card.get('longitude'),
                card.get('static_map_image_url'),
                section.get('directions_url'),
                card.get('address'),
                section.get('zoom_level')
            ))

            conn.commit()
            logger.info(f"✓ Stored map snapshot")
            return True
    except Exception as e:
        logger.error(f"Failed to store map snapshot: {e}")
        return False
    finally:
        conn.close()


def store_reviews(attraction_id: int, card: Dict[str, Any], reviews: List[Dict[str, Any]]) -> bool:
    """Store reviews data with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Update attraction table with overall rating data (with lock held)
            cursor.execute("""
                UPDATE attractions
                SET rating = %s,
                    review_count = %s,
                    summary_gemini = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                card.get('overall_rating'),
                card.get('total_reviews'),
                card.get('summary'),
                attraction_id
            ))

            # Delete existing reviews for this attraction
            cursor.execute(
                "DELETE FROM reviews WHERE attraction_id = %s",
                (attraction_id,)
            )

            # Insert new reviews
            for review in reviews:
                # Convert datetime to string if needed (pymysql expects string or None)
                review_time = review.get('time')
                if review_time and isinstance(review_time, datetime):
                    review_time = review_time.strftime('%Y-%m-%d %H:%M:%S')
                elif review_time and not isinstance(review_time, str):
                    # If it's not a datetime or string, convert to string or None
                    review_time = str(review_time) if review_time else None

                cursor.execute("""
                    INSERT INTO reviews (
                        attraction_id, author_name, author_url,
                        author_photo_url, rating, text,
                        time, source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    attraction_id,
                    review.get('author_name'),
                    review.get('author_url'),
                    review.get('author_photo_url'),
                    review.get('rating'),
                    review.get('text'),
                    review_time,
                    review.get('source', 'Google')
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(reviews)} reviews and updated attraction rating")
            return True
    except Exception as e:
        logger.error(f"Failed to store reviews: {e}")
        return False
    finally:
        conn.close()


def store_tips(attraction_id: int, tips: List[Dict[str, Any]]) -> bool:
    """Store tips data with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Delete existing tips for this attraction (with lock held)
            cursor.execute(
                "DELETE FROM tips WHERE attraction_id = %s",
                (attraction_id,)
            )

            # Insert new tips
            for tip in tips:
                cursor.execute("""
                    INSERT INTO tips (
                        attraction_id, tip_type, text,
                        source, scope, position
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    attraction_id,
                    tip.get('tip_type'),
                    tip.get('text'),
                    tip.get('source', 'Gemini'),
                    tip.get('scope', 'attraction'),
                    tip.get('position', 1)
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(tips)} tips")
            return True
    except Exception as e:
        logger.error(f"Failed to store tips: {e}")
        return False
    finally:
        conn.close()


def store_metadata(attraction_id: int, metadata: Dict[str, Any]) -> bool:
    """Store attraction metadata with row-level locking for concurrent safety."""
    try:
        import json
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Convert JSON fields to strings
            contact_info_json = json.dumps(metadata.get('contact_info', []))
            opening_hours_json = json.dumps(metadata.get('opening_hours', []))
            highlights_json = json.dumps(metadata.get('highlights', []))

            cursor.execute("""
                INSERT INTO attraction_metadata (
                    attraction_id, contact_info, accessibility_info,
                    best_season, opening_hours, short_description,
                    recommended_duration_minutes, highlights
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    contact_info = VALUES(contact_info),
                    accessibility_info = VALUES(accessibility_info),
                    best_season = VALUES(best_season),
                    opening_hours = VALUES(opening_hours),
                    short_description = VALUES(short_description),
                    recommended_duration_minutes = VALUES(recommended_duration_minutes),
                    highlights = VALUES(highlights),
                    updated_at = CURRENT_TIMESTAMP
            """, (
                attraction_id,
                contact_info_json,
                metadata.get('accessibility_info'),
                metadata.get('best_season'),
                opening_hours_json,
                metadata.get('short_description'),
                metadata.get('recommended_duration_minutes'),
                highlights_json
            ))

            conn.commit()  # Releases lock
            logger.info(f"✓ Stored metadata")
            return True
    except Exception as e:
        logger.error(f"Failed to store metadata: {e}")
        return False
    finally:
        conn.close()


def store_audience_profiles(attraction_id: int, profiles: List[Dict[str, Any]]) -> bool:
    """Store audience profiles with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Delete existing profiles for this attraction (with lock held)
            cursor.execute(
                "DELETE FROM audience_profiles WHERE attraction_id = %s",
                (attraction_id,)
            )

            # Insert new profiles
            for profile in profiles:
                cursor.execute("""
                    INSERT INTO audience_profiles (
                        attraction_id, audience_type, description, emoji
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    attraction_id,
                    profile.get('audience_type'),
                    profile.get('description'),
                    profile.get('emoji')
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(profiles)} audience profiles")
            return True
    except Exception as e:
        logger.error(f"Failed to store audience profiles: {e}")
        return False
    finally:
        conn.close()


def store_social_videos(attraction_id: int, videos: List[Dict[str, Any]]) -> bool:
    """Store social videos (YouTube Shorts) with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Delete existing videos for this attraction (with lock held)
            cursor.execute(
                "DELETE FROM social_videos WHERE attraction_id = %s",
                (attraction_id,)
            )

            # Insert new videos
            for idx, video in enumerate(videos):
                cursor.execute("""
                    INSERT INTO social_videos (
                        attraction_id, video_id, platform, title,
                        embed_url, thumbnail_url, watch_url,
                        duration_seconds, view_count, channel_title, position
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    attraction_id,
                    video.get('video_id'),
                    video.get('platform', 'youtube'),
                    video.get('title'),
                    video.get('embed_url'),
                    video.get('thumbnail_url'),
                    video.get('watch_url'),
                    video.get('duration_seconds'),
                    video.get('view_count'),
                    video.get('channel_title'),
                    idx
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(videos)} social videos")
            return True
    except Exception as e:
        logger.error(f"Failed to store social videos: {e}")
        return False
    finally:
        conn.close()


def store_nearby_attractions(attraction_id: int, nearby_list: List[Dict[str, Any]]) -> bool:
    """Store nearby attractions with row-level locking for concurrent safety."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Acquire row lock on attractions table
            cursor.execute(
                "SELECT id FROM attractions WHERE id = %s FOR UPDATE",
                (attraction_id,)
            )

            # Delete existing nearby attractions for this attraction (with lock held)
            cursor.execute(
                "DELETE FROM nearby_attractions WHERE attraction_id = %s",
                (attraction_id,)
            )

            # Insert new nearby attractions (skip if both image and link are null)
            for nearby in nearby_list:
                image_url = nearby.get('image_url')
                link = nearby.get('link')

                # Skip if both image and link are null
                if image_url is None and link is None:
                    continue

                cursor.execute("""
                    INSERT INTO nearby_attractions (
                        attraction_id, nearby_attraction_id, name, slug, place_id, rating,
                        user_ratings_total, review_count, image_url, link,
                        vicinity, distance_text, distance_km, walking_time_minutes,
                        audience_type, audience_text
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    attraction_id,
                    nearby.get('nearby_attraction_id'),
                    nearby.get('name'),
                    nearby.get('slug'),
                    nearby.get('place_id'),
                    nearby.get('rating'),
                    nearby.get('user_ratings_total'),
                    nearby.get('review_count'),
                    image_url,
                    link,
                    nearby.get('vicinity'),
                    nearby.get('distance_text'),
                    min(float(nearby.get('distance_km') or 0), settings.DISTANCE_MAX_KM) if nearby.get('distance_km') is not None else None,
                    nearby.get('walking_time_minutes'),
                    nearby.get('audience_type'),
                    nearby.get('audience_text')
                ))

            conn.commit()
            logger.info(f"✓ Stored {len(nearby_list)} nearby attractions")
            return True
    except Exception as e:
        logger.error(f"Failed to store nearby attractions: {e}")
        return False
    finally:
        conn.close()


def get_attractions_needing_videos(max_videos: int = 5) -> List[Dict[str, Any]]:
    """Get attractions that need more YouTube videos."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    a.id, a.slug, a.name, a.place_id,
                    a.latitude, a.longitude,
                    a.youtube_video_count,
                    a.youtube_last_fetched,
                    c.name as city_name, c.country
                FROM attractions a
                JOIN cities c ON a.city_id = c.id
                WHERE a.youtube_video_count < %s
                ORDER BY a.youtube_video_count ASC, a.id ASC
            """, (max_videos,))
            attractions = cursor.fetchall()
            logger.info(f"✓ Found {len(attractions)} attractions needing videos")
            return attractions
    except Exception as e:
        logger.error(f"Failed to get attractions needing videos: {e}")
        return []
    finally:
        conn.close()


def increment_youtube_count(attraction_id: int) -> bool:
    """Increment YouTube video count for an attraction."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE attractions
                SET youtube_video_count = youtube_video_count + 1,
                    youtube_last_fetched = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (attraction_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to increment YouTube count: {e}")
        return False
    finally:
        conn.close()


def mark_youtube_complete(attraction_id: int, max_videos: int = 5) -> bool:
    """Mark attraction as having maximum YouTube videos."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE attractions
                SET youtube_video_count = %s,
                    youtube_last_fetched = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (max_videos, attraction_id))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to mark YouTube complete: {e}")
        return False
    finally:
        conn.close()


def get_youtube_progress_stats() -> Dict[str, int]:
    """Get statistics on YouTube video fetching progress."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    youtube_video_count,
                    COUNT(*) as count
                FROM attractions
                GROUP BY youtube_video_count
                ORDER BY youtube_video_count
            """)
            results = cursor.fetchall()

            stats = {f"videos_{r['youtube_video_count']}": r['count'] for r in results}
            stats['total'] = sum(stats.values())

            return stats
    except Exception as e:
        logger.error(f"Failed to get YouTube progress stats: {e}")
        return {}
    finally:
        conn.close()


def store_single_social_video(attraction_id: int, video: Dict[str, Any], position: int) -> bool:
    """Store a single social video without deleting existing ones."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO social_videos (
                    attraction_id, video_id, platform, title,
                    embed_url, thumbnail_url, watch_url,
                    duration_seconds, view_count, channel_title, position
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                attraction_id,
                video.get('video_id'),
                video.get('platform', 'youtube'),
                video.get('title'),
                video.get('embed_url'),
                video.get('thumbnail_url'),
                video.get('watch_url'),
                video.get('duration_seconds'),
                video.get('view_count'),
                video.get('channel_title'),
                position
            ))

            conn.commit()
            logger.info(f"✓ Stored video at position {position}")
            return True
    except Exception as e:
        logger.error(f"Failed to store single video: {e}")
        return False
    finally:
        conn.close()
