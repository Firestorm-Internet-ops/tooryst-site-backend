"""Pipeline cleanup and completion tasks."""
import logging
from datetime import datetime
from sqlalchemy import text
from app.celery_app import celery_app
from app.infrastructure.persistence.db import SessionLocal

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.pipeline_cleanup.check_and_cleanup_pipeline")
def check_and_cleanup_pipeline(pipeline_run_id: int):
    """Check if pipeline is complete and cleanup if all attractions are done.
    
    This task is called after each attraction completes Stage 10.
    It checks if all attractions have been processed, and if so:
    1. Marks the pipeline as 'completed'
    2. Deletes all checkpoints for this pipeline
    3. Logs the completion
    
    Args:
        pipeline_run_id: ID of the pipeline run to check
    """
    session = SessionLocal()
    
    try:
        # Get pipeline run details
        pipeline_run = session.execute(text("""
            SELECT id, attractions_processed, attractions_completed, attractions_failed, status
            FROM pipeline_runs
            WHERE id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id}).fetchone()
        
        if not pipeline_run:
            logger.error(f"Pipeline run {pipeline_run_id} not found")
            return {'status': 'error', 'error': 'Pipeline run not found'}
        
        pipeline_id, total_attractions, completed, failed, status = pipeline_run
        
        # Check if all attractions are processed
        total_processed = completed + failed
        
        if total_processed >= total_attractions:
            # All attractions are done - cleanup
            logger.info(f"Pipeline {pipeline_run_id} complete: {completed} completed, {failed} failed")
            
            # Mark as completed
            session.execute(text("""
                UPDATE pipeline_runs
                SET status = 'completed',
                    completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :pipeline_run_id
            """), {'pipeline_run_id': pipeline_run_id})
            session.commit()
            
            # Delete checkpoints for this pipeline (cleanup)
            deleted_count = session.execute(text("""
                DELETE FROM pipeline_checkpoints
                WHERE pipeline_run_id = :pipeline_run_id
            """), {'pipeline_run_id': pipeline_run_id}).rowcount
            session.commit()
            
            logger.info(f"Pipeline {pipeline_run_id} cleanup complete: deleted {deleted_count} checkpoints")
            
            return {
                'status': 'completed',
                'pipeline_run_id': pipeline_run_id,
                'attractions_completed': completed,
                'attractions_failed': failed,
                'checkpoints_deleted': deleted_count
            }
        else:
            # Still processing
            return {
                'status': 'in_progress',
                'pipeline_run_id': pipeline_run_id,
                'attractions_completed': completed,
                'attractions_failed': failed,
                'attractions_remaining': total_attractions - total_processed
            }
    
    except Exception as e:
        logger.error(f"Error checking pipeline {pipeline_run_id}: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
    finally:
        session.close()


@celery_app.task(name="app.tasks.pipeline_cleanup.force_cleanup_pipeline")
def force_cleanup_pipeline(pipeline_run_id: int):
    """Force cleanup of a pipeline run (delete checkpoints and mark as completed).
    
    Use this if you want to manually cleanup a pipeline.
    
    Args:
        pipeline_run_id: ID of the pipeline run to cleanup
    """
    session = SessionLocal()
    
    try:
        # Mark as completed
        session.execute(text("""
            UPDATE pipeline_runs
            SET status = 'completed',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id})
        session.commit()
        
        # Delete checkpoints
        deleted_count = session.execute(text("""
            DELETE FROM pipeline_checkpoints
            WHERE pipeline_run_id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id}).rowcount
        session.commit()
        
        logger.info(f"Pipeline {pipeline_run_id} force cleanup complete: deleted {deleted_count} checkpoints")
        
        return {
            'status': 'cleaned',
            'pipeline_run_id': pipeline_run_id,
            'checkpoints_deleted': deleted_count
        }
    
    except Exception as e:
        logger.error(f"Error force cleaning pipeline {pipeline_run_id}: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
    finally:
        session.close()
