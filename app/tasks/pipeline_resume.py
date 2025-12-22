"""Pipeline resume functionality for interrupted pipelines."""
import logging
from typing import List
from sqlalchemy import text

from app.celery_app import celery_app
from app.core.checkpoint_manager import checkpoint_manager
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.tasks.parallel_pipeline_tasks import (
    process_stage_metadata, process_stage_hero_images, process_stage_best_time,
    process_stage_weather, process_stage_tips, process_stage_map,
    process_stage_reviews, process_stage_social_videos, process_stage_nearby,
    process_stage_audiences, setup_pipeline_logging
)

logger = logging.getLogger(__name__)

# Map stage names to their task functions
STAGE_TASK_MAP = {
    'metadata': process_stage_metadata,
    'hero_images': process_stage_hero_images,
    'best_time': process_stage_best_time,
    'weather': process_stage_weather,
    'tips': process_stage_tips,
    'map': process_stage_map,
    'reviews': process_stage_reviews,
    'social_videos': process_stage_social_videos,
    'nearby': process_stage_nearby,
    'audiences': process_stage_audiences,
}

# Define stage order
STAGE_ORDER = [
    'metadata', 'hero_images', 'best_time', 'weather', 
    'tips', 'map', 'reviews', 'social_videos', 'nearby', 'audiences'
]


@celery_app.task(name="app.tasks.pipeline_resume.resume_pipeline")
def resume_pipeline(pipeline_run_id: int):
    """Resume a pipeline run from where it left off.
    
    This function:
    1. Finds all attractions that have partial progress
    2. Determines the next stage for each attraction
    3. Restarts processing from that stage
    
    Args:
        pipeline_run_id: ID of the pipeline run to resume
    """
    session = SessionLocal()
    
    try:
        # Get pipeline run details
        pipeline_run = session.execute(text("""
            SELECT id, metadata, status
            FROM pipeline_runs
            WHERE id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id}).fetchone()
        
        if not pipeline_run:
            logger.error(f"Pipeline run {pipeline_run_id} not found")
            return {'status': 'error', 'error': 'Pipeline run not found'}
        
        # Setup logging
        pipe_logger = setup_pipeline_logging(pipeline_run_id)
        
        pipe_logger.info("="*80)
        pipe_logger.info(f"PIPELINE RESUME - Run ID: {pipeline_run_id}")
        pipe_logger.info("="*80)
        
        # Get all attractions that have partial progress
        resumable_attractions = checkpoint_manager.get_resumable_attractions(pipeline_run_id)
        
        if not resumable_attractions:
            pipe_logger.warning("No resumable attractions found")
            return {'status': 'no_progress', 'message': 'No partial progress found'}
        
        pipe_logger.info(f"Found {len(resumable_attractions)} attractions with partial progress")
        
        # Get all attractions in this pipeline run
        all_attractions = session.execute(text("""
            SELECT DISTINCT attraction_id
            FROM pipeline_checkpoints
            WHERE pipeline_run_id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id}).fetchall()
        
        resumed_count = 0
        
        for attraction_id, last_completed_stage in resumable_attractions:
            # Find the next stage to process
            next_stage_index = STAGE_ORDER.index(last_completed_stage) + 1
            
            if next_stage_index >= len(STAGE_ORDER):
                # All stages completed for this attraction
                pipe_logger.info(f"Attraction {attraction_id}: All stages completed")
                continue
            
            next_stage = STAGE_ORDER[next_stage_index]
            
            # Get attraction name for logging
            attraction = session.query(models.Attraction).filter_by(id=attraction_id).first()
            attraction_name = attraction.name if attraction else f"ID:{attraction_id}"
            
            pipe_logger.info(f"Resuming {attraction_name} from stage: {next_stage}")
            
            # Trigger the next stage task
            task_func = STAGE_TASK_MAP.get(next_stage)
            if task_func:
                task_func.delay(pipeline_run_id, attraction_id)
                resumed_count += 1
            else:
                pipe_logger.error(f"Unknown stage: {next_stage}")
        
        pipe_logger.info("="*80)
        pipe_logger.info(f"PIPELINE RESUME INITIATED")
        pipe_logger.info(f"Resumed {resumed_count} attractions")
        pipe_logger.info("="*80)
        
        return {
            'status': 'resumed',
            'pipeline_run_id': pipeline_run_id,
            'attractions_resumed': resumed_count
        }
        
    except Exception as e:
        logger.error(f"Failed to resume pipeline: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
    finally:
        session.close()


@celery_app.task(name="app.tasks.pipeline_resume.get_pipeline_status")
def get_pipeline_status(pipeline_run_id: int):
    """Get the current status and progress of a pipeline run.
    
    Args:
        pipeline_run_id: ID of the pipeline run
        
    Returns:
        Dict with pipeline status and progress information
    """
    try:
        progress = checkpoint_manager.get_pipeline_progress(pipeline_run_id)
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'progress': progress
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
