"""Celery tasks for running the full data pipeline."""
import asyncio
import logging
import sys
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime

# Add scripts directory to path
project_root = Path(__file__).parent.parent.parent
scripts_path = str(project_root / "scripts")
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

from sqlalchemy import text

from app.celery_app import celery_app
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.core.notifications import notification_manager, AlertType, AlertSeverity

# Import storage functions at module level
from app.infrastructure.persistence.storage_functions import (
    store_hero_images,
    store_map_snapshot,
    store_reviews,
    store_tips,
    store_audience_profiles,
    store_social_videos,
    store_nearby_attractions
)

def setup_pipeline_logging(run_id: str = None) -> logging.Logger:
    """Setup individual logging for each pipeline run."""
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Generate unique log file name
    if run_id:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"pipeline_run_{run_id}_{timestamp}.log"
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"pipeline_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger(f'pipeline_{run_id or "default"}')
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # File handler only - no console output
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Prevent propagation - logs ONLY go to pipeline file, not Celery worker
    logger.propagate = False

    logger.info(f"Pipeline logging initialized. Log file: {log_file}")

    # Log to Celery worker just once to show where to find detailed logs
    import logging as std_logging
    celery_logger = std_logging.getLogger(__name__)
    celery_logger.info(f"📋 Pipeline logs: {log_file}")

    return logger


def get_all_attractions() -> List[Dict[str, Any]]:
    """Get all attractions from database."""
    session = SessionLocal()
    try:
        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .all()
        )
        
        result = []
        for attraction, city in attractions:
            result.append({
                'id': attraction.id,
                'slug': attraction.slug,
                'name': attraction.name,
                'place_id': attraction.place_id,
                'city_name': city.name,
                'country': city.country,
                'timezone': city.timezone,
                'latitude': float(attraction.latitude) if attraction.latitude else None,
                'longitude': float(attraction.longitude) if attraction.longitude else None
            })
        
        return result
        
    finally:
        session.close()


@celery_app.task(name="app.tasks.pipeline_tasks.run_pipeline_for_attractions")
def run_pipeline_for_attractions(attraction_slugs: List[str] = None):
    """Run the full data pipeline for specific attractions.
    
    Args:
        attraction_slugs: List of attraction slugs to process. If None, processes all.
    
    This task:
    1. Gets specified attractions from database
    2. Fetches all data types for each attraction
    3. Stores in database
    """
    # Create pipeline run record first to get ID for logging
    pipeline_run_id = None
    session = SessionLocal()
    try:
        from datetime import datetime
        import json

        # Insert pipeline run record
        session.execute(text("""
            INSERT INTO pipeline_runs (started_at, status, metadata)
            VALUES (:started_at, 'running', :metadata)
        """), {
            'started_at': datetime.utcnow(),
            'metadata': json.dumps({'attraction_slugs': attraction_slugs}) if attraction_slugs else '{}'
        })
        session.commit()
        # mysqlclient returns a tuple, so use scalar() to get the id
        pipeline_run_id = session.execute(text("SELECT LAST_INSERT_ID()")).scalar()
    except Exception as e:
        print(f"Failed to create pipeline run record: {e}")
    finally:
        session.close()
    
    # Setup individual logging for this run
    logger = setup_pipeline_logging(str(pipeline_run_id) if pipeline_run_id else None)
    
    logger.info(f"Starting pipeline for {len(attraction_slugs) if attraction_slugs else 'all'} attractions")
    if pipeline_run_id:
        logger.info(f"Pipeline run ID: {pipeline_run_id}")
    else:
        logger.warning("No pipeline run ID generated")
    
    try:
        from app.infrastructure.external_apis.hero_images_fetcher import GooglePlacesHeroImagesFetcher
        from app.infrastructure.external_apis.weather_fetcher import WeatherFetcherImpl
        from app.infrastructure.external_apis.map_fetcher import MapFetcherImpl
        from app.infrastructure.external_apis.besttime_fetcher import BestTimeFetcherImpl
        from app.infrastructure.external_apis.metadata_fetcher import MetadataFetcherImpl
        from app.infrastructure.external_apis.reviews_fetcher import ReviewsFetcherImpl
        from app.infrastructure.external_apis.tips_fetcher import TipsFetcherImpl
        from app.infrastructure.external_apis.audience_fetcher import AudienceFetcherImpl
        from app.infrastructure.external_apis.social_videos_fetcher import SocialVideosFetcherImpl
        from app.infrastructure.external_apis.nearby_attractions_fetcher import NearbyAttractionsFetcherImpl
        
        # Import storage functions for refresh tasks
        from app.tasks.refresh_tasks import store_best_time_data, store_weather_data, store_metadata
        # Note: db_helper storage functions are imported at module level
        
        # Get attractions
        session = SessionLocal()
        try:
            query = (
                session.query(models.Attraction, models.City)
                .join(models.City, models.Attraction.city_id == models.City.id)
            )
            
            if attraction_slugs:
                query = query.filter(models.Attraction.slug.in_(attraction_slugs))
            
            attraction_data = query.all()
            
            attractions = []
            for attraction, city in attraction_data:
                attractions.append({
                    'id': attraction.id,
                    'slug': attraction.slug,
                    'name': attraction.name,
                    'place_id': attraction.place_id,
                    'city_name': city.name,
                    'country': city.country,
                    'timezone': city.timezone,
                    'latitude': float(attraction.latitude) if attraction.latitude else None,
                    'longitude': float(attraction.longitude) if attraction.longitude else None
                })
        finally:
            session.close()
        
        logger.info(f"Processing {len(attractions)} attractions")
        
        if not attractions:
            return {"status": "success", "processed": 0}
        
        # Create fetchers
        fetchers = {
            'hero_images': GooglePlacesHeroImagesFetcher(),
            'weather': WeatherFetcherImpl(),
            'map': MapFetcherImpl(),
            'best_time': BestTimeFetcherImpl(),
            'metadata': MetadataFetcherImpl(),
            'reviews': ReviewsFetcherImpl(),
            'tips': TipsFetcherImpl(),
            'audience': AudienceFetcherImpl(),
            'social_videos': SocialVideosFetcherImpl(),
            'nearby': NearbyAttractionsFetcherImpl()
        }
        
        success_count = 0
        error_count = 0
        
        for idx, attraction in enumerate(attractions, 1):
            try:
                logger.info("="*80)
                logger.info(f"[{idx}/{len(attractions)}] PROCESSING: {attraction['name']}")
                logger.info(f"City: {attraction['city_name']}, Country: {attraction['country']}")
                logger.info("="*80)

                # Initialize event loop for async operations
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                # Process each data type
                # 1. Metadata
                logger.info("ℹ️  Fetching visitor info...")
                try:
                    result = loop.run_until_complete(
                        fetchers['metadata'].fetch(
                            attraction_id=attraction['id'],
                            place_id=attraction['place_id'],
                            attraction_name=attraction['name'],
                            city_name=attraction['city_name']
                        )
                    )
                    if result and result.get('metadata'):
                        store_metadata(attraction['id'], result['metadata'])
                        logger.info("  ✓ Stored visitor info")
                    else:
                        logger.warning("  ⚠ No visitor info found")
                except Exception as e:
                    logger.error(f"  ✗ Visitor info error: {e}")

                # 2. Hero Images
                logger.info("📸 Fetching hero images...")
                try:
                    result = loop.run_until_complete(
                        fetchers['hero_images'].fetch(
                            attraction_id=attraction['id'],
                            place_id=attraction['place_id'],
                            attraction_name=attraction['name'],
                            city_name=attraction['city_name']
                        )
                    )
                    if result and result.get('images'):
                        store_hero_images(attraction['id'], result['images'])
                        logger.info(f"  ✓ Stored {len(result['images'])} hero images")
                    else:
                        logger.warning("  ⚠ No hero images found")
                except Exception as e:
                    logger.error(f"  ✗ Hero images error: {e}")

                # Close event loop
                loop.close()
                success_count += 1
                logger.info("="*80)
                logger.info(f"✓ COMPLETED: {attraction['name']}")
                logger.info("="*80)

                # ========== REST OF DATA FETCHING COMMENTED OUT ==========
                """
                # 3. Best Time
                logger.info("⏰ Fetching best time data...")
                try:
                    result = loop.run_until_complete(
                        fetchers['best_time'].fetch(
                            attraction_id=attraction['id'],
                            place_id=attraction['place_id']
                        )
                    )
                    if result and result.get('all_days'):
                        store_best_time_data(attraction['id'], result['all_days'])
                        logger.info(f"  ✓ Stored {len(result['all_days'])} days of crowd data")
                    else:
                        logger.warning("  ⚠ No best time data found")
                except Exception as e:
                    logger.error(f"  ✗ Best time error: {e}")

                # 4. Weather
                logger.info("🌤️  Fetching weather forecast...")
                if attraction['latitude'] and attraction['longitude']:
                    try:
                        result = loop.run_until_complete(
                            fetchers['weather'].fetch(
                                attraction_id=attraction['id'],
                                place_id=attraction['place_id'],
                                latitude=attraction['latitude'],
                                longitude=attraction['longitude'],
                                timezone_str="UTC",
                                attraction_name=attraction['name'],
                                city_name=attraction['city_name'],
                                country=attraction['country']
                            )
                        )
                        if result and result.get('forecast_days'):
                            store_weather_data(attraction['id'], result['forecast_days'])
                            logger.info(f"  ✓ Stored {len(result['forecast_days'])} days of weather")
                        else:
                            logger.warning("  ⚠ No weather data found")
                    except Exception as e:
                        logger.error(f"  ✗ Weather error: {e}")
                else:
                    logger.warning("  ⚠ Skipped (no coordinates)")

                # 5. Tips
                logger.info("💡 Fetching tips...")
                try:
                    result = loop.run_until_complete(
                        fetchers['tips'].fetch(
                            attraction_id=attraction['id'],
                            place_id=attraction['place_id'],
                            attraction_name=attraction['name']
                        )
                    )
                    if result and result.get('tips'):
                        store_tips(attraction['id'], result['tips'])
                        logger.info(f"  ✓ Stored {len(result['tips'])} tips")
                    else:
                        logger.warning("  ⚠ No tips found")
                except Exception as e:
                    logger.error(f"  ✗ Tips error: {e}")

                # 6. Map
                logger.info("🗺️  Fetching map data...")
                if attraction['latitude'] and attraction['longitude']:
                    try:
                        result = loop.run_until_complete(
                            fetchers['map'].fetch(
                                attraction_id=attraction['id'],
                                place_id=attraction['place_id'],
                                latitude=attraction['latitude'],
                                longitude=attraction['longitude'],
                                address=None,
                                attraction_name=attraction['name'],
                                city_name=attraction['city_name'],
                                zoom_level=15
                            )
                        )
                        if result and result.get('card') and result.get('section'):
                            store_map_snapshot(attraction['id'], result['card'], result['section'])
                            logger.info("  ✓ Stored map snapshot")
                        else:
                            logger.warning("  ⚠ No map data found")
                    except Exception as e:
                        logger.error(f"  ✗ Map error: {e}")
                else:
                    logger.warning("  ⚠ Skipped (no coordinates)")

                # 7. Reviews
                logger.info("⭐ Fetching reviews...")
                try:
                    result = loop.run_until_complete(
                        fetchers['reviews'].fetch(
                            attraction_id=attraction['id'],
                            place_id=attraction['place_id'],
                            attraction_name=attraction['name'],
                            city_name=attraction['city_name']
                        )
                    )
                    if result and result.get('card') and result.get('reviews'):
                        store_reviews(attraction['id'], result['card'], result['reviews'])
                        logger.info(f"  ✓ Stored {len(result['reviews'])} reviews")
                    else:
                        logger.warning("  ⚠ No reviews found")
                except Exception as e:
                    logger.error(f"  ✗ Reviews error: {e}")

                # 8. Social Videos
                logger.info("📹 Fetching social videos...")
                # Check if YouTube quota is exceeded
                if fetchers['social_videos'].is_quota_exceeded():
                    logger.warning("  ⏭️  Skipping - YouTube quota exceeded")
                    logger.info("  📝 Adding to retry queue for tomorrow")
                    # Add to retry queue
                    try:
                        retry_session = SessionLocal()
                        retry_sql = (
                            "INSERT INTO youtube_retry_queue (attraction_id, status, error_message) "
                            "VALUES (:attraction_id, 'pending', 'YouTube quota exceeded during pipeline run') "
                            "ON DUPLICATE KEY UPDATE "
                            "status = 'pending', "
                            "error_message = 'YouTube quota exceeded during pipeline run', "
                            "updated_at = CURRENT_TIMESTAMP"
                        )
                        retry_session.execute(text(retry_sql), {'attraction_id': attraction['id']})
                        retry_session.commit()
                        retry_session.close()
                    except Exception as queue_error:
                        logger.error(f"  ✗ Failed to add to retry queue: {queue_error}")
                else:
                    try:
                        result = loop.run_until_complete(
                            fetchers['social_videos'].fetch(
                                attraction_id=attraction['id'],
                                attraction_name=attraction['name'],
                                city_name=attraction['city_name'],
                                country=attraction['country']
                            )
                        )
                        if result and result.get('videos'):
                            store_social_videos(attraction['id'], result['videos'])
                            logger.info(f"  ✓ Stored {len(result['videos'])} videos")
                        else:
                            logger.warning("  ⚠ No videos found")
                    except Exception as e:
                        logger.error(f"  ✗ Social videos error: {e}")

                # 9. Nearby Attractions
                logger.info("📍 Fetching nearby attractions...")
                if attraction['latitude'] and attraction['longitude']:
                    try:
                        result = loop.run_until_complete(
                            fetchers['nearby'].fetch(
                                attraction_id=attraction['id'],
                                attraction_name=attraction['name'],
                                city_name=attraction['city_name'],
                                latitude=attraction['latitude'],
                                longitude=attraction['longitude'],
                                place_id=attraction['place_id']
                            )
                        )
                        if result and result.get('nearby'):
                            store_nearby_attractions(attraction['id'], result['nearby'])
                            logger.info(f"  ✓ Stored {len(result['nearby'])} nearby attractions")
                        else:
                            logger.warning("  ⚠ No nearby attractions found")
                    except Exception as e:
                        logger.error(f"  ✗ Nearby attractions error: {e}")
                else:
                    logger.warning("  ⚠ Skipped (no coordinates)")

                # 10. Audience Profiles
                logger.info("👥 Fetching audience profiles...")
                try:
                    result = loop.run_until_complete(
                        fetchers['audience'].fetch(
                            attraction_id=attraction['id'],
                            attraction_name=attraction['name'],
                            city_name=attraction['city_name']
                        )
                    )
              x      if result and result.get('profiles'):
                        store_audience_profiles(attraction['id'], result['profiles'])
                        logger.info(f"  ✓ Stored {len(result['profiles'])} audience profiles")
                    else:
                        logger.warning("  ⚠ No audience profiles found")
                except Exception as e:
                    logger.error(f"  ✗ Audience profiles error: {e}")
                
                loop.close()
                success_count += 1
                logger.info("="*80)
                logger.info(f"✓ COMPLETED: {attraction['name']}")
                logger.info("="*80)
                """
                # ========== END OF COMMENTED DATA FETCHING ==========

            except Exception as e:
                error_count += 1
                logger.error("="*80)
                logger.error(f"✗ FAILED: {attraction['name']}")
                logger.error(f"Error: {e}")
                logger.error("="*80)
                import traceback
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                
                # Send notification for attraction processing failure
                notification_manager.send_alert(
                    alert_type=AlertType.PIPELINE_FAILED,
                    severity=AlertSeverity.ERROR,
                    title=f"Pipeline Failed for {attraction['name']}",
                    message=f"Failed to process attraction: {attraction['name']}\n\nError: {str(e)}\n\nStack trace:\n{stack_trace}",
                    metadata={
                        "attraction_name": attraction['name'],
                        "attraction_id": attraction['id'],
                        "city": attraction['city_name'],
                        "country": attraction['country'],
                        "error_type": type(e).__name__
                    }
                )
        
        logger.info("")
        logger.info("="*80)
        logger.info("PIPELINE EXECUTION COMPLETE")
        logger.info("="*80)
        logger.info(f"Total Processed: {len(attractions)}")
        logger.info(f"✓ Successful: {success_count}")
        logger.info(f"✗ Failed: {error_count}")
        logger.info("="*80)
        
        # Update pipeline run record
        if pipeline_run_id:
            session = SessionLocal()
            try:
                from datetime import datetime
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET completed_at = :completed_at,
                        status = 'completed',
                        attractions_processed = :processed,
                        attractions_succeeded = :succeeded,
                        attractions_failed = :failed
                    WHERE id = :run_id
                """), {
                    'completed_at': datetime.utcnow(),
                    'processed': len(attractions),
                    'succeeded': success_count,
                    'failed': error_count,
                    'run_id': pipeline_run_id
                })
                session.commit()
                logger.info(f"Updated pipeline run record: {pipeline_run_id}")
            except Exception as e:
                logger.error(f"Failed to update pipeline run record: {e}")
            finally:
                session.close()
        
        return {
            "status": "success",
            "processed": len(attractions),
            "success": success_count,
            "errors": error_count,
            "pipeline_run_id": pipeline_run_id
        }
        
    except Exception as e:
        logger.error("="*80)
        logger.error("PIPELINE TASK FAILED")
        logger.error(f"Error: {e}")
        logger.error("="*80)
        import traceback
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        
        # Update pipeline run record as failed
        if pipeline_run_id:
            session = SessionLocal()
            try:
                from datetime import datetime
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET completed_at = :completed_at,
                        status = 'failed',
                        error_message = :error
                    WHERE id = :run_id
                """), {
                    'completed_at': datetime.utcnow(),
                    'error': str(e),
                    'run_id': pipeline_run_id
                })
                session.commit()
            except Exception as update_error:
                logger.error(f"Failed to update pipeline run record: {update_error}")
            finally:
                session.close()
        
        # Send notification for complete pipeline failure
        notification_manager.send_alert(
            alert_type=AlertType.PIPELINE_FAILED,
            severity=AlertSeverity.CRITICAL,
            title="Complete Pipeline Execution Failed",
            message=f"The entire pipeline task failed to execute.\n\nError: {str(e)}\n\nStack trace:\n{stack_trace}",
            metadata={
                "error_type": type(e).__name__,
                "attraction_slugs": str(attraction_slugs) if attraction_slugs else "all",
                "pipeline_run_id": pipeline_run_id
            }
        )
        
        return {"status": "error", "error": str(e), "pipeline_run_id": pipeline_run_id}


@celery_app.task(name="app.tasks.pipeline_tasks.run_full_pipeline")
def run_full_pipeline():
    """Run the full data pipeline for all attractions.
    
    This task:
    1. Gets all attractions from database
    2. Fetches all data types for each attraction
    3. Stores in database
    
    Runs monthly to keep all data fresh.
    """
    logger.info("Starting full pipeline task for all attractions")
    # Delegate to run_pipeline_for_attractions with no filter
    return run_pipeline_for_attractions(attraction_slugs=None)
