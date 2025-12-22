"""Parallel pipeline tasks with staged processing - REFACTORED VERSION.

This version eliminates ~900 lines of duplication by using a configuration-driven approach.
"""
import os
import asyncio
import logging
from typing import List, Optional, Callable, Any, Dict
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from sqlalchemy import text

from app.celery_app import celery_app
from app.core.stage_manager import stage_manager
from app.core.retry_manager import retry_manager
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models

# Import fetchers
from app.infrastructure.external_apis.metadata_fetcher import MetadataFetcherImpl
from app.infrastructure.external_apis.hero_images_fetcher import GooglePlacesHeroImagesFetcher
from app.infrastructure.external_apis.besttime_fetcher import BestTimeFetcherImpl
from app.infrastructure.external_apis.weather_fetcher import WeatherFetcherImpl
from app.infrastructure.external_apis.tips_fetcher import TipsFetcherImpl
from app.infrastructure.external_apis.map_fetcher import MapFetcherImpl
from app.infrastructure.external_apis.reviews_fetcher import ReviewsFetcherImpl
from app.infrastructure.external_apis.social_videos_fetcher import SocialVideosFetcherImpl
from app.infrastructure.external_apis.nearby_attractions_fetcher import NearbyAttractionsFetcherImpl
from app.infrastructure.external_apis.audience_fetcher import AudienceFetcherImpl

# Import storage functions
from app.infrastructure.persistence.storage_functions import (
    store_metadata, store_hero_images, store_best_time_data,
    store_weather_forecast, store_tips, store_map_snapshot,
    store_reviews, store_social_videos, store_nearby_attractions,
    store_audience_profiles
)

logger = logging.getLogger(__name__)


def setup_pipeline_logging(pipeline_run_id: int) -> logging.Logger:
    """Setup individual logging for each pipeline run.

    If log file already exists, reuse it. Otherwise, create new one.
    This ensures all stage tasks write to the same log file.
    """
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    # Create logger
    logger_inst = logging.getLogger(f'pipeline_{pipeline_run_id}')

    # If logger already has handlers, it's already set up - just return it
    if logger_inst.handlers:
        return logger_inst

    logger_inst.setLevel(logging.INFO)

    # Check if log file already exists for this pipeline run
    existing_files = list(log_dir.glob(f"pipeline_run_{pipeline_run_id}_*.log"))

    if existing_files:
        # Use existing log file (append mode)
        log_file = existing_files[0]
    else:
        # Generate unique log file name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"pipeline_run_{pipeline_run_id}_{timestamp}.log"

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler in append mode
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger_inst.addHandler(file_handler)

    # Prevent propagation - logs ONLY go to pipeline file, not Celery worker
    logger_inst.propagate = False

    # Log to Celery worker just once to show where to find detailed logs
    celery_logger = logging.getLogger(__name__)
    celery_logger.info(f"📋 Pipeline run {pipeline_run_id} logs: {log_file}")

    return logger_inst


@dataclass
class StageConfig:
    """Configuration for a pipeline stage.

    This replaces ~100 lines of duplicated code per stage with a simple config object.
    """
    stage_number: int
    stage_name: str
    fetcher_class: type
    storage_function: Callable
    result_key: str  # Key in fetcher result to check (e.g., 'metadata', 'images', 'forecast_days')
    data_type: str  # For retry queue (e.g., 'metadata', 'hero_images', 'weather')
    next_stage_name: Optional[str]  # None for final stage
    next_stage_task: Optional[Callable] = None  # Celery task for next stage
    continue_on_no_data: bool = True  # Whether to continue pipeline if no data found
    is_final_stage: bool = False  # Whether this is the last stage
    coordinate_defaults: Optional[Dict[str, float]] = None  # Default lat/lng if missing (Paris)

    def build_fetch_params(self, attraction: models.Attraction, city: Optional[models.City]) -> Dict[str, Any]:
        """Build parameters for fetcher.fetch() call.

        Each fetcher has slightly different required parameters. This method
        handles the variations without duplicating code.
        """
        params = {
            'attraction_id': attraction.id,
            'attraction_name': attraction.name,
        }

        # Add place_id if fetcher needs it
        if hasattr(self.fetcher_class, '__init__'):
            params['place_id'] = attraction.place_id

        # Add city info if available
        if city:
            params['city_name'] = city.name

            # Some fetchers need country
            if self.stage_name in ['social_videos', 'weather', 'metadata']:
                params['country'] = city.country

        # Add coordinates if fetcher needs them
        if self.stage_name in ['weather', 'map', 'nearby', 'best_time']:
            latitude = getattr(attraction, 'latitude', None)
            longitude = getattr(attraction, 'longitude', None)

            # Use defaults for weather and map if coordinates missing
            if self.coordinate_defaults and (latitude is None or longitude is None):
                params['latitude'] = self.coordinate_defaults['latitude']
                params['longitude'] = self.coordinate_defaults['longitude']
            else:
                params['latitude'] = latitude
                params['longitude'] = longitude

        return params


def _update_pipeline_counter(pipeline_run_id: int, counter: str):
    """Update pipeline run counter (attractions_failed or attractions_completed)."""
    session = SessionLocal()
    try:
        session.execute(text(f"""
            UPDATE pipeline_runs
            SET {counter} = {counter} + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id})
        session.commit()
    finally:
        session.close()


def process_stage(
    pipeline_run_id: int,
    attraction_id: int,
    config: StageConfig,
    pipe_logger: logging.Logger
) -> Dict[str, Any]:
    """Generic stage processor - handles ALL stages with configuration.

    This single function replaces ~100 lines of duplicated code per stage.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
        config: Stage configuration (fetcher, storage function, next stage, etc.)
        pipe_logger: Logger for this pipeline run

    Returns:
        Dict with status ('success', 'no_data', 'error', 'timeout', 'not_found')
    """
    try:
        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        if not stage_manager.acquire_stage_slot(config.stage_name, max_concurrent=1, timeout=60):
            pipe_logger.error(f"[Stage {config.stage_number}] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        # Get attraction details
        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage {config.stage_number}] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot(config.stage_name)
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            pipe_logger.info(f"[Stage {config.stage_number}] Processing: {attraction.name}")

            # Build fetcher parameters
            fetch_params = config.build_fetch_params(attraction, city)

            # Log parameters for debugging (especially coordinates)
            if config.coordinate_defaults:
                pipe_logger.info(
                    f"[Stage {config.stage_number}] Parameters - "
                    f"lat: {fetch_params.get('latitude')}, lng: {fetch_params.get('longitude')}"
                )

            # Fetch data
            fetcher = config.fetcher_class()
            
            # Special check for Stage 8 (Social Videos) - check quota before processing
            if config.stage_number == 8 and hasattr(fetcher, 'is_quota_exceeded'):
                if fetcher.is_quota_exceeded():
                    pipe_logger.warning(f"[Stage {config.stage_number}] ⏭️  SKIPPING: YouTube quota exceeded for {attraction.name}")
                    pipe_logger.info(f"[Stage {config.stage_number}] → Stage {config.stage_number + 1} (quota exceeded): {attraction.name}")
                    
                    # Skip to next stage without processing
                    stage_manager.push_to_stage(config.next_stage_name, attraction_id, pipeline_run_id)
                    config.next_stage_task.delay(pipeline_run_id, attraction_id)
                    
                    stage_manager.release_stage_slot(config.stage_name)
                    session.close()
                    return {'status': 'quota_exceeded', 'skipped': True}
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                result = loop.run_until_complete(fetcher.fetch(**fetch_params))

                if result and result.get(config.result_key):
                    # Store result
                    config.storage_function(attraction.id, result[config.result_key])

                    # Log success with count if applicable
                    result_data = result[config.result_key]
                    if isinstance(result_data, list):
                        pipe_logger.info(
                            f"[Stage {config.stage_number}] ✓ Stored {len(result_data)} "
                            f"{config.data_type} for {attraction.name}"
                        )
                    else:
                        pipe_logger.info(
                            f"[Stage {config.stage_number}] ✓ Stored {config.data_type} for {attraction.name}"
                        )
                    status = 'success'
                else:
                    pipe_logger.warning(
                        f"[Stage {config.stage_number}] ⚠ No {config.data_type} found for {attraction.name}"
                    )
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage {config.stage_number}] ✗ Error fetching {config.data_type}: {e}")

                # Check if quota exceeded (especially for Stage 8)
                if "quota" in str(e).lower():
                    pipe_logger.critical(f"[Stage {config.stage_number}] 🚫 QUOTA EXCEEDED - Stopping for all remaining attractions")
                    pipe_logger.info(f"[Stage {config.stage_number}] → Stage {config.stage_number + 1} (quota exceeded): {attraction.name}")
                    
                    # Skip to next stage without storing data
                    stage_manager.push_to_stage(config.next_stage_name, attraction_id, pipeline_run_id)
                    config.next_stage_task.delay(pipeline_run_id, attraction_id)
                    
                    stage_manager.release_stage_slot(config.stage_name)
                    session.close()
                    return {'status': 'quota_exceeded', 'error': str(e)}
                
                # Check if rate limited (for retry queue)
                if "rate" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type=config.data_type,
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        # Release stage slot
        stage_manager.release_stage_slot(config.stage_name)

        # Handle stage completion based on status
        if config.is_final_stage:
            # Final stage - mark pipeline as complete
            if status in ['success', 'no_data']:
                _update_pipeline_counter(pipeline_run_id, 'attractions_completed')
                pipe_logger.info(f"[Stage {config.stage_number}] ✓ Pipeline complete for attraction {attraction_id}")
            else:
                _update_pipeline_counter(pipeline_run_id, 'attractions_failed')
        else:
            # Not final stage - continue to next stage
            should_continue = (
                status == 'success' or
                (status == 'no_data' and config.continue_on_no_data)
            )

            if should_continue:
                stage_manager.push_to_stage(config.next_stage_name, attraction_id, pipeline_run_id)
                pipe_logger.info(
                    f"[Stage {config.stage_number}] → Stage {config.stage_number + 1}: {attraction.name}"
                )
                config.next_stage_task.delay(pipeline_run_id, attraction_id)
            elif status == 'error':
                # Error - mark as failed
                _update_pipeline_counter(pipeline_run_id, 'attractions_failed')

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage {config.stage_number}] Fatal error: {e}")
        stage_manager.release_stage_slot(config.stage_name)
        return {'status': 'error', 'error': str(e)}


# ============================================================================
# STAGE CONFIGURATIONS
# ============================================================================
# This replaces ~900 lines of duplicated code with simple configuration

# Paris coordinates as defaults for weather/map
PARIS_COORDS = {'latitude': 48.8584, 'longitude': 2.2945}

# Stage configurations will be defined after task declarations
# (circular dependency: tasks need configs, configs need next_stage_task references)

# ============================================================================
# STAGE TASKS - Thin wrappers around generic process_stage()
# ============================================================================

@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_metadata")
def process_stage_metadata(pipeline_run_id: int, attraction_id: int):
    """Stage 1: Fetch and store metadata."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_1_METADATA, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_hero_images")
def process_stage_hero_images(pipeline_run_id: int, attraction_id: int):
    """Stage 2: Fetch and store hero images."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_2_HERO_IMAGES, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_best_time")
def process_stage_best_time(pipeline_run_id: int, attraction_id: int):
    """Stage 3: Fetch and store best time data."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_3_BEST_TIME, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_weather")
def process_stage_weather(pipeline_run_id: int, attraction_id: int):
    """Stage 4: Fetch and store weather forecast."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_4_WEATHER, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_tips")
def process_stage_tips(pipeline_run_id: int, attraction_id: int):
    """Stage 5: Fetch and store tips."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_5_TIPS, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_map")
def process_stage_map(pipeline_run_id: int, attraction_id: int):
    """Stage 6: Fetch and store map snapshot."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_6_MAP, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_reviews")
def process_stage_reviews(pipeline_run_id: int, attraction_id: int):
    """Stage 7: Fetch and store reviews."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_7_REVIEWS, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_social_videos")
def process_stage_social_videos(pipeline_run_id: int, attraction_id: int):
    """Stage 8: Fetch and store social videos."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_8_SOCIAL_VIDEOS, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_nearby")
def process_stage_nearby(pipeline_run_id: int, attraction_id: int):
    """Stage 9: Fetch and store nearby attractions."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_9_NEARBY, pipe_logger)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_audiences")
def process_stage_audiences(pipeline_run_id: int, attraction_id: int):
    """Stage 10: Fetch and store audience profiles (FINAL STAGE)."""
    pipe_logger = setup_pipeline_logging(pipeline_run_id)
    return process_stage(pipeline_run_id, attraction_id, STAGE_10_AUDIENCES, pipe_logger)


# ============================================================================
# Define stage configurations (after task declarations to avoid circular refs)
# ============================================================================

STAGE_1_METADATA = StageConfig(
    stage_number=1,
    stage_name='metadata',
    fetcher_class=MetadataFetcherImpl,
    storage_function=store_metadata,
    result_key='metadata',
    data_type='metadata',
    next_stage_name='hero_images',
    next_stage_task=process_stage_hero_images,
    continue_on_no_data=True
)

STAGE_2_HERO_IMAGES = StageConfig(
    stage_number=2,
    stage_name='hero_images',
    fetcher_class=GooglePlacesHeroImagesFetcher,
    storage_function=store_hero_images,
    result_key='images',
    data_type='hero_images',
    next_stage_name='best_time',
    next_stage_task=process_stage_best_time,
    continue_on_no_data=True
)

STAGE_3_BEST_TIME = StageConfig(
    stage_number=3,
    stage_name='best_time',
    fetcher_class=BestTimeFetcherImpl,
    storage_function=store_best_time_data,
    result_key='all_days',
    data_type='best_time',
    next_stage_name='weather',
    next_stage_task=process_stage_weather,
    continue_on_no_data=True
)

STAGE_4_WEATHER = StageConfig(
    stage_number=4,
    stage_name='weather',
    fetcher_class=WeatherFetcherImpl,
    storage_function=store_weather_forecast,
    result_key='forecast_days',
    data_type='weather',
    next_stage_name='tips',
    next_stage_task=process_stage_tips,
    continue_on_no_data=True,
    coordinate_defaults=PARIS_COORDS
)

STAGE_5_TIPS = StageConfig(
    stage_number=5,
    stage_name='tips',
    fetcher_class=TipsFetcherImpl,
    storage_function=store_tips,
    result_key='tips',
    data_type='tips',
    next_stage_name='map',
    next_stage_task=process_stage_map,
    continue_on_no_data=True
)

STAGE_6_MAP = StageConfig(
    stage_number=6,
    stage_name='map',
    fetcher_class=MapFetcherImpl,
    storage_function=store_map_snapshot,
    result_key='card',
    data_type='map',
    next_stage_name='reviews',
    next_stage_task=process_stage_reviews,
    continue_on_no_data=True,
    coordinate_defaults=PARIS_COORDS
)

STAGE_7_REVIEWS = StageConfig(
    stage_number=7,
    stage_name='reviews',
    fetcher_class=ReviewsFetcherImpl,
    storage_function=store_reviews,
    result_key='reviews',
    data_type='reviews',
    next_stage_name='social_videos',
    next_stage_task=process_stage_social_videos,
    continue_on_no_data=True
)

STAGE_8_SOCIAL_VIDEOS = StageConfig(
    stage_number=8,
    stage_name='social_videos',
    fetcher_class=SocialVideosFetcherImpl,
    storage_function=store_social_videos,
    result_key='videos',
    data_type='social_videos',
    next_stage_name='nearby',
    next_stage_task=process_stage_nearby,
    continue_on_no_data=True
)

STAGE_9_NEARBY = StageConfig(
    stage_number=9,
    stage_name='nearby',
    fetcher_class=NearbyAttractionsFetcherImpl,
    storage_function=store_nearby_attractions,
    result_key='nearby',
    data_type='nearby_attractions',
    next_stage_name='audiences',
    next_stage_task=process_stage_audiences,
    continue_on_no_data=True
)

STAGE_10_AUDIENCES = StageConfig(
    stage_number=10,
    stage_name='audiences',
    fetcher_class=AudienceFetcherImpl,
    storage_function=store_audience_profiles,
    result_key='profiles',
    data_type='audiences',
    next_stage_name=None,  # Final stage
    next_stage_task=None,
    continue_on_no_data=True,
    is_final_stage=True
)


# ============================================================================
# ORCHESTRATOR - Seeds Stage 1 and monitors progress
# ============================================================================

@celery_app.task(name="app.tasks.parallel_pipeline_tasks.orchestrate_pipeline")
def orchestrate_pipeline(attraction_slugs: List[str]):
    """Main pipeline orchestrator - seeds Stage 1 queue and monitors progress.

    Args:
        attraction_slugs: List of attraction slugs to process
    """
    session = SessionLocal()

    try:
        # Create pipeline run record
        session.execute(text("""
            INSERT INTO pipeline_runs (started_at, status, attractions_processed, metadata)
            VALUES (CURRENT_TIMESTAMP, 'running', :count, :metadata)
        """), {
            'count': len(attraction_slugs),
            'metadata': '{"attraction_slugs": ' + str(attraction_slugs).replace("'", '"') + '}'
        })
        session.commit()

        # Get the pipeline run ID
        result = session.execute(text("SELECT LAST_INSERT_ID()"))
        pipeline_run_id = result.scalar()

        # Setup pipeline logging
        pipe_logger = setup_pipeline_logging(pipeline_run_id)

        pipe_logger.info("="*80)
        pipe_logger.info(f"PIPELINE START - Run ID: {pipeline_run_id}")
        pipe_logger.info("="*80)
        pipe_logger.info(f"Processing {len(attraction_slugs)} attractions")
        pipe_logger.info(f"Attractions: {', '.join(attraction_slugs)}")
        pipe_logger.info("="*80)

        # Get attraction IDs from slugs
        attractions = []
        for slug in attraction_slugs:
            attr = session.query(models.Attraction).filter_by(slug=slug).first()
            if attr:
                attractions.append(attr)
            else:
                pipe_logger.warning(f"Attraction not found: {slug}")

        pipe_logger.info(f"Found {len(attractions)} attractions in database")

        # Seed Stage 1 queue and kick off processing
        for attraction in attractions:
            stage_manager.push_to_stage('metadata', attraction.id, pipeline_run_id)
            pipe_logger.info(f"Queued for Stage 1: {attraction.name}")

            # Trigger stage 1 processing
            process_stage_metadata.delay(pipeline_run_id, attraction.id)

        pipe_logger.info("="*80)
        pipe_logger.info("PIPELINE INITIALIZED")
        pipe_logger.info(f"Stage 1 queue depth: {stage_manager.get_queue_depth('metadata')}")
        pipe_logger.info("="*80)

        return {
            'status': 'started',
            'pipeline_run_id': pipeline_run_id,
            'attractions_count': len(attractions)
        }

    except Exception as e:
        logger.error(f"Failed to orchestrate pipeline: {e}")
        session.rollback()
        return {
            'status': 'error',
            'error': str(e)
        }
    finally:
        session.close()
