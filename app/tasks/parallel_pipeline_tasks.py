"""Parallel pipeline tasks with staged processing."""
import os
import asyncio
import logging
from typing import List
from datetime import datetime
from pathlib import Path
from sqlalchemy import text

from app.celery_app import celery_app
from app.config import settings
from app.core.stage_manager import stage_manager
from app.core.retry_manager import retry_manager
from app.core.checkpoint_manager import checkpoint_manager
from app.core.data_tracking_manager import data_tracking_manager
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


def should_skip_stage(pipeline_run_id: int, attraction_id: int, stage_name: str, pipe_logger) -> bool:
    """Check if a stage should be skipped (already completed).
    
    Returns:
        True if stage is already completed, False otherwise
    """
    if checkpoint_manager.is_stage_completed(pipeline_run_id, attraction_id, stage_name):
        pipe_logger.info(f"[{stage_name.upper()}] ⊘ Skipping (already completed)")
        return True
    return False


def record_stage_completion(pipeline_run_id: int, attraction_id: int, stage_name: str, status: str, metadata: dict = None):
    """Record that a stage has been completed for an attraction."""
    checkpoint_manager.create_checkpoint(pipeline_run_id, attraction_id, stage_name, status, metadata)


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_metadata")
def process_stage_metadata(pipeline_run_id: int, attraction_id: int):
    """Stage 1: Fetch and store metadata for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    # Setup pipeline logger (finds existing log file for this run)
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'metadata', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('hero_images', attraction_id, pipeline_run_id)
            process_stage_hero_images.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        timeout_seconds = settings.STAGE_SLOT_TIMEOUT_SECONDS
        if not stage_manager.acquire_stage_slot('metadata', max_concurrent=8, timeout=timeout_seconds):
            pipe_logger.error(f"[Stage 1] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        # Get attraction details
        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 1] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('metadata')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            pipe_logger.info(f"[Stage 1] Processing: {attraction.name}")

            # Fetch metadata
            fetcher = MetadataFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        attraction_name=attraction.name,
                        city_name=city.name if city else None
                    )
                )

                if result and result.get('metadata'):
                    store_metadata(attraction.id, result['metadata'])
                    pipe_logger.info(f"[Stage 1] ✓ Stored metadata for {attraction.name}")
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 1] ⚠ No metadata found for {attraction.name}")
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 1] ✗ Error fetching metadata: {e}")
                # Check if rate limited
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='metadata',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        # Release slot and push to next stage
        stage_manager.release_stage_slot('metadata')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'metadata', 'completed')
            
            # Push to Stage 2 (hero images)
            stage_manager.push_to_stage('hero_images', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 1] → Stage 2: {attraction.name}")

            # Trigger stage 2 processing
            process_stage_hero_images.delay(pipeline_run_id, attraction_id)
        elif status == 'error':
            # Record failed checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'metadata', 'failed')

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 1] Fatal error: {e}")
        stage_manager.release_stage_slot('metadata')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_hero_images")
def process_stage_hero_images(pipeline_run_id: int, attraction_id: int):
    """Stage 2: Fetch and store hero images for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    # Get pipeline logger
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'hero_images', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('best_time', attraction_id, pipeline_run_id)
            process_stage_best_time.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        timeout_seconds = settings.STAGE_SLOT_TIMEOUT_SECONDS
        if not stage_manager.acquire_stage_slot('hero_images', max_concurrent=8, timeout=timeout_seconds):
            pipe_logger.error(f"[Stage 2] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        # Get attraction details
        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 2] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('hero_images')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            pipe_logger.info(f"[Stage 2] Processing: {attraction.name}")

            # Fetch hero images
            fetcher = GooglePlacesHeroImagesFetcher()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        attraction_name=attraction.name,
                        city_name=city.name if city else None
                    )
                )

                if result and result.get('images'):
                    store_hero_images(attraction.id, result['images'])
                    image_count = len(result['images'])
                    pipe_logger.info(f"[Stage 2] ✓ Stored {image_count} hero images for {attraction.name}")
                    # Track data
                    data_tracking_manager.update_hero_images_count(pipeline_run_id, attraction_id, image_count)
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 2] ⚠ No hero images found for {attraction.name}")
                    # Track 0 images
                    data_tracking_manager.update_hero_images_count(pipeline_run_id, attraction_id, 0)
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 2] ✗ Error fetching hero images: {e}")
                # Check if rate limited
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='hero_images',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        # Release slot
        stage_manager.release_stage_slot('hero_images')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'hero_images', 'completed')
            
            # Push to Stage 3 (best time)
            stage_manager.push_to_stage('best_time', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 2] → Stage 3: {attraction.name}")

            # Trigger stage 3 processing
            process_stage_best_time.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data':
            # Record checkpoint for no_data (still mark as completed)
            record_stage_completion(pipeline_run_id, attraction_id, 'hero_images', 'completed')
            
            # No images but continue to next stage
            stage_manager.push_to_stage('best_time', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 2] → Stage 3 (no images): {attraction.name}")
            process_stage_best_time.delay(pipeline_run_id, attraction_id)
        else:
            # Record failed checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'hero_images', 'failed')
            
            # Error - mark as failed
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_failed = attractions_failed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
            finally:
                session.close()

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 2] Fatal error: {e}")
        stage_manager.release_stage_slot('hero_images')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_best_time")
def process_stage_best_time(pipeline_run_id: int, attraction_id: int):
    """Stage 3: Fetch and store best time data for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    # Get pipeline logger
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'best_time', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('weather', attraction_id, pipeline_run_id)
            process_stage_weather.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        timeout_seconds = settings.STAGE_SLOT_TIMEOUT_SECONDS
        if not stage_manager.acquire_stage_slot('best_time', max_concurrent=8, timeout=timeout_seconds):
            pipe_logger.error(f"[Stage 3] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        # Get attraction details
        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 3] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('best_time')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            pipe_logger.info(f"[Stage 3] Processing: {attraction.name}")

            # Fetch best time data
            fetcher = BestTimeFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        attraction_name=attraction.name,
                        city_name=city.name if city else None
                    )
                )

                if result:
                    stored_count = 0

                    # Store regular days (day-of-week based)
                    if result.get('regular_days'):
                        # Convert regular_days to the format expected by store_best_time_data
                        regular_days_formatted = []
                        for day in result['regular_days']:
                            regular_days_formatted.append({
                                'day_type': 'regular',
                                'day_int': day['day_int'],
                                'day_name': day['day_name'],
                                'card': day['card'],
                                'section': day['section'],
                                'data_source': day.get('data_source', result.get('data_source', 'besttime'))
                            })
                        store_best_time_data(attraction.id, regular_days_formatted)
                        stored_count += len(regular_days_formatted)
                        pipe_logger.info(f"[Stage 3] ✓ Stored {len(regular_days_formatted)} regular days for {attraction.name}")

                    # Store special days (date-based) - DISABLED FOR NOW
                    # if result.get('special_days'):
                    #     # Convert special_days to the format expected by store_best_time_data
                    #     special_days_formatted = []
                    #     for day in result['special_days']:
                    #         special_days_formatted.append({
                    #             'day_type': 'special',
                    #             'date_local': day['date'],
                    #             'day_name': day['day'],
                    #             'card': {
                    #                 'is_open_today': day['is_open_today'],
                    #                 'is_open_now': False,  # Not applicable for future dates
                    #                 'today_opening_time': day['today_opening_time'],
                    #                 'today_closing_time': day['today_closing_time'],
                    #                 'crowd_level_today': day['crowd_level_today'],
                    #                 'best_time_today': day['best_time_today']
                    #             },
                    #             'section': {
                    #                 'best_time_today': day['best_time_today'],
                    #                 'reason_text': day['reason_text'],
                    #                 'hourly_crowd_levels': day['hourly_crowd_levels']
                    #             },
                    #             'data_source': result.get('data_source', 'besttime')
                    #         })
                    #     store_best_time_data(attraction.id, special_days_formatted)
                    #     stored_count += len(special_days_formatted)
                    #     pipe_logger.info(f"[Stage 3] ✓ Stored {len(special_days_formatted)} special days for {attraction.name}")

                    if stored_count > 0:
                        pipe_logger.info(f"[Stage 3] ✓ Total: Stored {stored_count} days of best time data for {attraction.name}")
                        status = 'success'
                    else:
                        pipe_logger.warning(f"[Stage 3] ⚠ No best time data to store for {attraction.name}")
                        status = 'no_data'
                else:
                    pipe_logger.warning(f"[Stage 3] ⚠ No best time data found for {attraction.name}")
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 3] ✗ Error fetching best time data: {e}")
                # Check if rate limited
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='best_time',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        # Release slot
        stage_manager.release_stage_slot('best_time')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'best_time', 'completed')
            
            # Push to Stage 4 (weather)
            stage_manager.push_to_stage('weather', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 3] → Stage 4: {attraction.name}")

            # Trigger stage 4 processing
            process_stage_weather.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data':
            # Record checkpoint for no_data (still mark as completed)
            record_stage_completion(pipeline_run_id, attraction_id, 'best_time', 'completed')
            
            # No data but continue to next stage
            stage_manager.push_to_stage('weather', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 3] → Stage 4 (no data): {attraction.name}")
            process_stage_weather.delay(pipeline_run_id, attraction_id)
        else:
            # Record failed checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'best_time', 'failed')
            
            # Error - mark as failed
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_failed = attractions_failed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
            finally:
                session.close()

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 3] Fatal error: {e}")
        stage_manager.release_stage_slot('best_time')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_weather")
def process_stage_weather(pipeline_run_id: int, attraction_id: int):
    """Stage 4: Fetch and store weather forecast for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    # Get pipeline logger
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'weather', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('tips', attraction_id, pipeline_run_id)
            process_stage_tips.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        timeout_seconds = settings.STAGE_SLOT_TIMEOUT_SECONDS
        if not stage_manager.acquire_stage_slot('weather', max_concurrent=8, timeout=timeout_seconds):
            pipe_logger.error(f"[Stage 4] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        # Get attraction details
        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 4] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('weather')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            pipe_logger.info(f"[Stage 4] Processing: {attraction.name}")

            # Log parameter values for debugging
            latitude = getattr(attraction, "latitude", None)
            longitude = getattr(attraction, "longitude", None)
            city_name = city.name if city else None
            pipe_logger.info(f"[Stage 4] Parameters - lat: {latitude}, lng: {longitude}, city_name: {city_name}")

            # Fetch weather data
            fetcher = WeatherFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                default_lat = settings.DEFAULT_LATITUDE
                default_lng = settings.DEFAULT_LONGITUDE
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        latitude=latitude if latitude is not None else default_lat,
                        longitude=longitude if longitude is not None else default_lng,
                        attraction_name=attraction.name,
                        city_name=city_name
                    )
                )

                if result and result.get('forecast_days'):
                    store_weather_forecast(attraction.id, result['forecast_days'])
                    pipe_logger.info(f"[Stage 4] ✓ Stored weather forecast for {attraction.name}")
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 4] ⚠ No weather forecast found for {attraction.name}")
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 4] ✗ Error fetching weather forecast: {e}")
                # Check if rate limited
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='weather',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        # Release slot
        stage_manager.release_stage_slot('weather')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'weather', 'completed')
            
            # Push to Stage 5 (tips)
            stage_manager.push_to_stage('tips', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 4] → Stage 5: {attraction.name}")
            process_stage_tips.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data':
            # Record checkpoint for no_data (still mark as completed)
            record_stage_completion(pipeline_run_id, attraction_id, 'weather', 'completed')
            
            # No data but continue to next stage
            stage_manager.push_to_stage('tips', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 4] → Stage 5 (no data): {attraction.name}")
            process_stage_tips.delay(pipeline_run_id, attraction_id)
        else:
            # Record failed checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'weather', 'failed')
            
            # Error - mark as failed
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_failed = attractions_failed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
            finally:
                session.close()

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 4] Fatal error: {e}")
        stage_manager.release_stage_slot('weather')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_tips")
def process_stage_tips(pipeline_run_id: int, attraction_id: int):
    """Stage 5: Fetch and store tips for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'tips', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('map', attraction_id, pipeline_run_id)
            process_stage_map.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        timeout_seconds = settings.STAGE_SLOT_TIMEOUT_SECONDS
        if not stage_manager.acquire_stage_slot('tips', max_concurrent=8, timeout=timeout_seconds):
            pipe_logger.error(f"[Stage 5] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 5] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('tips')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()

            pipe_logger.info(f"[Stage 5] Processing: {attraction.name}")

            fetcher = TipsFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        attraction_name=attraction.name,
                        city_name=city.name if city else None
                    )
                )

                if result and result.get('tips'):
                    store_tips(attraction.id, result['tips'])
                    tips_count = len(result['tips'])
                    pipe_logger.info(f"[Stage 5] ✓ Stored {tips_count} tips for {attraction.name}")
                    # Track data
                    data_tracking_manager.update_tips_count(pipeline_run_id, attraction_id, tips_count)
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 5] ⚠ No tips found for {attraction.name}")
                    # Track 0 tips
                    data_tracking_manager.update_tips_count(pipeline_run_id, attraction_id, 0)
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 5] ✗ Tips error: {e}")
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='tips',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                # Reddit session automatically closed by async context manager
                loop.close()
        finally:
            session.close()

        # Release slot
        stage_manager.release_stage_slot('tips')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'tips', 'completed')
            
            # Push to Stage 6 (map)
            stage_manager.push_to_stage('map', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 5] → Stage 6: {attraction.name}")
            process_stage_map.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data' or status == 'error':
            # Record checkpoint (mark as completed even if no data or error)
            record_stage_completion(pipeline_run_id, attraction_id, 'tips', 'completed')
            
            # No data or error - continue to next stage anyway
            stage_manager.push_to_stage('map', attraction_id, pipeline_run_id)
            if status == 'error':
                pipe_logger.info(f"[Stage 5] → Stage 6 (error, continuing): {attraction.name}")
            else:
                pipe_logger.info(f"[Stage 5] → Stage 6 (no data): {attraction.name}")
            process_stage_map.delay(pipeline_run_id, attraction_id)

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 5] Fatal error: {e}")
        stage_manager.release_stage_slot('tips')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_map")
def process_stage_map(pipeline_run_id: int, attraction_id: int):
    """Stage 6: Fetch and store map snapshot for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'map', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('reviews', attraction_id, pipeline_run_id)
            process_stage_reviews.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        # Acquire stage slot (max 1 concurrent - sequential pipeline flow)
        timeout_seconds = settings.STAGE_SLOT_TIMEOUT_SECONDS
        if not stage_manager.acquire_stage_slot('map', max_concurrent=8, timeout=timeout_seconds):
            pipe_logger.error(f"[Stage 6] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 6] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('map')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()
            pipe_logger.info(f"[Stage 6] Processing: {attraction.name}")

            # Log parameter values for debugging
            latitude = getattr(attraction, "latitude", None)
            longitude = getattr(attraction, "longitude", None)
            city_name = city.name if city else None
            pipe_logger.info(f"[Stage 6] Parameters - lat: {latitude}, lng: {longitude}, city_name: {city_name}")

            fetcher = MapFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                default_lat = settings.DEFAULT_LATITUDE
                default_lng = settings.DEFAULT_LONGITUDE
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        latitude=latitude if latitude is not None else default_lat,
                        longitude=longitude if longitude is not None else default_lng,
                        attraction_name=attraction.name,
                        city_name=city_name
                    )
                )

                if result and result.get('card'):
                    store_map_snapshot(attraction.id, result['card'], result.get('section', {}))
                    pipe_logger.info(f"[Stage 6] ✓ Stored map snapshot for {attraction.name}")
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 6] ⚠ No map data found for {attraction.name}")
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 6] ✗ Map error: {e}")
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='map',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        stage_manager.release_stage_slot('map')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'map', 'completed')
            
            stage_manager.push_to_stage('reviews', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 6] → Stage 7: {attraction.name}")
            process_stage_reviews.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data' or status == 'error':
            # Record checkpoint (mark as completed even if no data or error)
            record_stage_completion(pipeline_run_id, attraction_id, 'map', 'completed')
            
            stage_manager.push_to_stage('reviews', attraction_id, pipeline_run_id)
            if status == 'error':
                pipe_logger.info(f"[Stage 6] → Stage 7 (error, continuing): {attraction.name}")
            else:
                pipe_logger.info(f"[Stage 6] → Stage 7 (no data): {attraction.name}")
            process_stage_reviews.delay(pipeline_run_id, attraction_id)

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 6] Fatal error: {e}")
        stage_manager.release_stage_slot('map')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_reviews")
def process_stage_reviews(pipeline_run_id: int, attraction_id: int):
    """Stage 7: Fetch and store reviews for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'reviews', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('social_videos', attraction_id, pipeline_run_id)
            process_stage_social_videos.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        if not stage_manager.acquire_stage_slot('reviews', max_concurrent=8, timeout=60):
            pipe_logger.error(f"[Stage 7] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 7] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('reviews')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()
            pipe_logger.info(f"[Stage 7] Processing: {attraction.name}")

            fetcher = ReviewsFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        place_id=attraction.place_id,
                        attraction_name=attraction.name,
                        city_name=city.name if city else None
                    )
                )

                if result and result.get('reviews'):
                    store_reviews(attraction.id, result.get('card', {}), result['reviews'])
                    review_count = len(result['reviews'])
                    pipe_logger.info(f"[Stage 7] ✓ Stored {review_count} reviews for {attraction.name}")
                    # Track data
                    data_tracking_manager.update_reviews_count(pipeline_run_id, attraction_id, review_count)
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 7] ⚠ No reviews found for {attraction.name}")
                    # Track 0 reviews
                    data_tracking_manager.update_reviews_count(pipeline_run_id, attraction_id, 0)
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 7] ✗ Reviews error: {e}")
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='reviews',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        stage_manager.release_stage_slot('reviews')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'reviews', 'completed')
            
            # Push to Stage 8 (social videos)
            stage_manager.push_to_stage('social_videos', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 7] → Stage 8: {attraction.name}")
            process_stage_social_videos.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data' or status == 'error':
            # Record checkpoint (mark as completed even if no data or error)
            record_stage_completion(pipeline_run_id, attraction_id, 'reviews', 'completed')
            
            # No data or error - continue to next stage anyway
            stage_manager.push_to_stage('social_videos', attraction_id, pipeline_run_id)
            if status == 'error':
                pipe_logger.info(f"[Stage 7] → Stage 8 (error, continuing): {attraction.name}")
            else:
                pipe_logger.info(f"[Stage 7] → Stage 8 (no data): {attraction.name}")
            process_stage_social_videos.delay(pipeline_run_id, attraction_id)

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 7] Fatal error: {e}")
        stage_manager.release_stage_slot('reviews')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_social_videos")
def process_stage_social_videos(pipeline_run_id: int, attraction_id: int):
    """Stage 8: Fetch and store social videos for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'social_videos', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('nearby', attraction_id, pipeline_run_id)
            process_stage_nearby.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        if not stage_manager.acquire_stage_slot('social_videos', max_concurrent=8, timeout=60):
            pipe_logger.error(f"[Stage 8] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 8] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('social_videos')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()
            
            # Check if YouTube quota is exceeded BEFORE processing
            fetcher = SocialVideosFetcherImpl()
            if fetcher.is_quota_exceeded():
                pipe_logger.warning(f"[Stage 8] ⏭️  SKIPPING: YouTube quota exceeded for {attraction.name}")
                pipe_logger.info(f"[Stage 8] → Stage 9 (quota exceeded): {attraction.name}")
                
                # Skip to Stage 9 without processing
                stage_manager.push_to_stage('nearby', attraction_id, pipeline_run_id)
                process_stage_nearby.delay(pipeline_run_id, attraction_id)
                
                stage_manager.release_stage_slot('social_videos')
                return {'status': 'quota_exceeded', 'skipped': True}
            
            pipe_logger.info(f"[Stage 8] Processing: {attraction.name}")

            # Log parameter values for debugging
            city_name = city.name if city else None
            pipe_logger.info(f"[Stage 8] Parameters - city_name: {city_name}")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        attraction_name=attraction.name,
                        city_name=city_name if city_name else "Unknown City"  # Provide default
                    )
                )

                if result and result.get('videos'):
                    store_social_videos(attraction.id, result['videos'])
                    videos_count = len(result['videos'])
                    pipe_logger.info(f"[Stage 8] ✓ Stored {videos_count} social videos for {attraction.name}")
                    # Track data
                    data_tracking_manager.update_social_videos_count(pipeline_run_id, attraction_id, videos_count)
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 8] ⚠ No social videos found for {attraction.name}")
                    # Track 0 videos
                    data_tracking_manager.update_social_videos_count(pipeline_run_id, attraction_id, 0)
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 8] ✗ Social videos error: {e}")
                
                # Check if this is a quota error
                if "quota" in str(e).lower():
                    pipe_logger.critical(f"[Stage 8] 🚫 QUOTA EXCEEDED - Stopping Stage 8 for all remaining attractions")
                    pipe_logger.info(f"[Stage 8] → Stage 9 (quota exceeded): {attraction.name}")
                    
                    # Skip to Stage 9 without storing data
                    stage_manager.push_to_stage('nearby', attraction_id, pipeline_run_id)
                    process_stage_nearby.delay(pipeline_run_id, attraction_id)
                    
                    stage_manager.release_stage_slot('social_videos')
                    return {'status': 'quota_exceeded', 'error': str(e)}
                
                # For other rate limit errors, add to retry queue
                if "rate" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='social_videos',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        stage_manager.release_stage_slot('social_videos')

        # Push to Stage 9 (nearby attractions)
        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'social_videos', 'completed')
            
            stage_manager.push_to_stage('nearby', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 8] → Stage 9: {attraction.name}")
            process_stage_nearby.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data':
            # Record checkpoint (mark as completed even if no data)
            record_stage_completion(pipeline_run_id, attraction_id, 'social_videos', 'completed')
            
            # No data but continue to next stage
            stage_manager.push_to_stage('nearby', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 8] → Stage 9 (no data): {attraction.name}")
            process_stage_nearby.delay(pipeline_run_id, attraction_id)
        else:
            # Failed - mark as failed
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_failed = attractions_failed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
            finally:
                session.close()

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 8] Fatal error: {e}")
        stage_manager.release_stage_slot('social_videos')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_nearby")
def process_stage_nearby(pipeline_run_id: int, attraction_id: int):
    """Stage 9: Fetch and store nearby attractions.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'nearby', pipe_logger):
            # Push to next stage
            stage_manager.push_to_stage('audiences', attraction_id, pipeline_run_id)
            process_stage_audiences.delay(pipeline_run_id, attraction_id)
            return {'status': 'skipped'}

        if not stage_manager.acquire_stage_slot('nearby', max_concurrent=8, timeout=60):
            pipe_logger.error(f"[Stage 9] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 9] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('nearby')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()
            pipe_logger.info(f"[Stage 9] Processing: {attraction.name}")

            # Validate coordinates
            latitude = getattr(attraction, "latitude", None)
            longitude = getattr(attraction, "longitude", None)
            city_name = city.name if city else None
            pipe_logger.info(f"[Stage 9] Parameters - lat: {latitude}, lng: {longitude}, city_name: {city_name}")

            if latitude is None or longitude is None:
                pipe_logger.error(f"[Stage 9] Missing coordinates for {attraction.name}; skipping nearby fetch")
                return {'status': 'error', 'error': 'missing_coordinates'}

            fetcher = NearbyAttractionsFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Cast lat/lng to float to avoid Decimal math issues downstream
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        attraction_name=attraction.name,
                        city_name=city_name if city_name else "Unknown City",  # Provide default
                        latitude=float(latitude),
                        longitude=float(longitude),
                        place_id=attraction.place_id
                    )
                )

                if result and result.get('nearby'):
                    store_nearby_attractions(attraction.id, result['nearby'])
                    nearby_count = len(result['nearby'])
                    pipe_logger.info(f"[Stage 9] ✓ Stored {nearby_count} nearby attractions for {attraction.name}")
                    # Track data
                    data_tracking_manager.update_nearby_attractions_count(pipeline_run_id, attraction_id, nearby_count)
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 9] ⚠ No nearby attractions found for {attraction.name}")
                    # Track 0 nearby attractions
                    data_tracking_manager.update_nearby_attractions_count(pipeline_run_id, attraction_id, 0)
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 9] ✗ Nearby attractions error: {e}")
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='nearby',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        stage_manager.release_stage_slot('nearby')

        if status == 'success':
            # Record checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'nearby', 'completed')
            
            stage_manager.push_to_stage('audiences', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 9] → Stage 10: {attraction.name}")
            process_stage_audiences.delay(pipeline_run_id, attraction_id)
        elif status == 'no_data':
            # Record checkpoint (mark as completed even if no data)
            record_stage_completion(pipeline_run_id, attraction_id, 'nearby', 'completed')
            
            stage_manager.push_to_stage('audiences', attraction_id, pipeline_run_id)
            pipe_logger.info(f"[Stage 9] → Stage 10 (no data): {attraction.name}")
            process_stage_audiences.delay(pipeline_run_id, attraction_id)
        else:
            # Record failed checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'nearby', 'failed')
            
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_failed = attractions_failed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
            finally:
                session.close()

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 9] Fatal error: {e}")
        stage_manager.release_stage_slot('nearby')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.process_stage_audiences")
def process_stage_audiences(pipeline_run_id: int, attraction_id: int):
    """Stage 10: Fetch and store audience profiles for an attraction.

    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction to process
    """
    pipe_logger = setup_pipeline_logging(pipeline_run_id)

    try:
        # Check if stage already completed (resume logic)
        if should_skip_stage(pipeline_run_id, attraction_id, 'audiences', pipe_logger):
            # This is the final stage - mark pipeline run as complete
            pipe_logger.info(f"[Stage 10] ✓ Attraction {attraction_id} fully processed (all stages complete)")
            return {'status': 'skipped'}

        if not stage_manager.acquire_stage_slot('audiences', max_concurrent=8, timeout=60):
            pipe_logger.error(f"[Stage 10] Timeout acquiring slot for attraction {attraction_id}")
            return {'status': 'timeout'}

        session = SessionLocal()
        try:
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            if not attraction:
                pipe_logger.error(f"[Stage 10] Attraction {attraction_id} not found")
                stage_manager.release_stage_slot('audiences')
                return {'status': 'not_found'}

            city = session.query(models.City).filter_by(id=attraction.city_id).first()
            pipe_logger.info(f"[Stage 10] Processing: {attraction.name}")

            # Log parameter values for debugging
            city_name = city.name if city else None
            pipe_logger.info(f"[Stage 10] Parameters - city_name: {city_name}")

            fetcher = AudienceFetcherImpl()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    fetcher.fetch(
                        attraction_id=attraction.id,
                        attraction_name=attraction.name,
                        city_name=city_name if city_name else "Unknown City"  # Provide default
                    )
                )

                if result and result.get('profiles'):
                    store_audience_profiles(attraction.id, result['profiles'])
                    profiles_count = len(result['profiles'])
                    pipe_logger.info(f"[Stage 10] ✓ Stored {profiles_count} audience profiles for {attraction.name}")
                    # Track data
                    data_tracking_manager.update_audience_profiles_count(pipeline_run_id, attraction_id, profiles_count)
                    status = 'success'
                else:
                    pipe_logger.warning(f"[Stage 10] ⚠ No audience profiles found for {attraction.name}")
                    # Track 0 profiles
                    data_tracking_manager.update_audience_profiles_count(pipeline_run_id, attraction_id, 0)
                    status = 'no_data'
            except Exception as e:
                pipe_logger.error(f"[Stage 10] ✗ Audience profiles error: {e}")
                if "rate" in str(e).lower() or "quota" in str(e).lower():
                    retry_manager.add_to_retry_queue(
                        attraction_id=attraction.id,
                        data_type='audiences',
                        error_message=str(e)
                    )
                status = 'error'
            finally:
                loop.close()
        finally:
            session.close()

        stage_manager.release_stage_slot('audiences')

        # FINAL STAGE - mark pipeline as complete
        if status == 'success' or status == 'no_data':
            # Record checkpoint for final stage
            record_stage_completion(pipeline_run_id, attraction_id, 'audiences', 'completed')
            
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_completed = attractions_completed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
                pipe_logger.info(f"[Stage 10] ✓ Pipeline complete for {attraction.name}")
            finally:
                session.close()
        else:
            # Record failed checkpoint
            record_stage_completion(pipeline_run_id, attraction_id, 'audiences', 'failed')
            
            session = SessionLocal()
            try:
                session.execute(text("""
                    UPDATE pipeline_runs
                    SET attractions_failed = attractions_failed + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :pipeline_run_id
                """), {'pipeline_run_id': pipeline_run_id})
                session.commit()
            finally:
                session.close()

        # Check if pipeline is complete and cleanup if needed
        from app.tasks.pipeline_cleanup import check_and_cleanup_pipeline
        check_and_cleanup_pipeline.delay(pipeline_run_id)

        return {'status': status}

    except Exception as e:
        pipe_logger.error(f"[Stage 10] Fatal error: {e}")
        stage_manager.release_stage_slot('audiences')
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name="app.tasks.parallel_pipeline_tasks.orchestrate_pipeline")
def orchestrate_pipeline(attraction_slugs: List[str]):
    """Main pipeline orchestrator - seeds Stage 1 queue and monitors progress.

    Args:
        attraction_slugs: List of attraction slugs to process
    """
    session = SessionLocal()

    try:
        # Check if a pipeline is already running with the same attractions (deduplication)
        # This prevents duplicate runs if the task is triggered multiple times within 10 seconds
        # (e.g., file watcher triggering multiple times)
        import json
        metadata_json = json.dumps({"attraction_slugs": sorted(attraction_slugs) if attraction_slugs else []})
        
        existing_run = session.execute(text("""
            SELECT id FROM pipeline_runs 
            WHERE status = 'running' 
            AND metadata = :metadata
            AND started_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)
            LIMIT 1
        """), {'metadata': metadata_json}).scalar()
        
        if existing_run:
            logger.warning(f"Pipeline run {existing_run} already running with same attractions, skipping duplicate (triggered within 10 seconds)")
            session.close()
            return {
                'status': 'skipped',
                'reason': 'duplicate_run_detected',
                'existing_pipeline_run_id': existing_run
            }
        
        # Create pipeline run record
        session.execute(text("""
            INSERT INTO pipeline_runs (started_at, status, attractions_processed, metadata)
            VALUES (CURRENT_TIMESTAMP, 'running', :count, :metadata)
        """), {
            'count': len(attraction_slugs),
            'metadata': metadata_json
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
            # Create tracking record for this attraction
            data_tracking_manager.create_tracking_record(pipeline_run_id, attraction.id)
            
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
