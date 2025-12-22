"""Pipeline checkpoint and resume management."""
import logging
from datetime import datetime
from sqlalchemy import text
from app.infrastructure.persistence.db import SessionLocal

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages pipeline checkpoints for resumable processing."""

    @staticmethod
    def create_checkpoint(pipeline_run_id: int, attraction_id: int, stage_name: str, status: str, metadata: dict = None):
        """Record a checkpoint for an attraction at a specific stage.
        
        Args:
            pipeline_run_id: ID of the pipeline run
            attraction_id: ID of the attraction
            stage_name: Name of the stage (e.g., 'metadata', 'hero_images')
            status: Status of the stage ('completed', 'failed', 'skipped')
            metadata: Optional metadata about the checkpoint
        """
        session = SessionLocal()
        try:
            # Insert or update checkpoint
            session.execute(text("""
                INSERT INTO pipeline_checkpoints 
                (pipeline_run_id, attraction_id, stage_name, status, metadata, created_at, updated_at)
                VALUES (:pipeline_run_id, :attraction_id, :stage_name, :status, :metadata, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    status = :status,
                    metadata = :metadata,
                    updated_at = CURRENT_TIMESTAMP
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'stage_name': stage_name,
                'status': status,
                'metadata': str(metadata) if metadata else None
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to create checkpoint: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def get_checkpoint(pipeline_run_id: int, attraction_id: int, stage_name: str):
        """Get checkpoint status for an attraction at a specific stage.
        
        Returns:
            Checkpoint record or None if not found
        """
        session = SessionLocal()
        try:
            result = session.execute(text("""
                SELECT status, metadata, created_at, updated_at
                FROM pipeline_checkpoints
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
                  AND stage_name = :stage_name
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'stage_name': stage_name
            }).fetchone()
            return result
        finally:
            session.close()

    @staticmethod
    def get_last_completed_stage(pipeline_run_id: int, attraction_id: int):
        """Get the last completed stage for an attraction.
        
        Returns:
            Stage name or None if no stages completed
        """
        session = SessionLocal()
        try:
            # Define stage order
            stage_order = [
                'metadata', 'hero_images', 'best_time', 'weather', 
                'tips', 'map', 'reviews', 'social_videos', 'nearby', 'audiences'
            ]
            
            result = session.execute(text("""
                SELECT stage_name
                FROM pipeline_checkpoints
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
                  AND status = 'completed'
                ORDER BY created_at DESC
                LIMIT 1
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id
            }).scalar()
            
            return result
        finally:
            session.close()

    @staticmethod
    def get_resumable_attractions(pipeline_run_id: int):
        """Get attractions that can be resumed (have partial progress).
        
        Returns:
            List of (attraction_id, last_completed_stage) tuples
        """
        session = SessionLocal()
        try:
            # Use a subquery to get the latest checkpoint for each attraction
            results = session.execute(text("""
                SELECT attraction_id, stage_name
                FROM pipeline_checkpoints pc1
                WHERE pipeline_run_id = :pipeline_run_id
                  AND status = 'completed'
                  AND created_at = (
                    SELECT MAX(created_at)
                    FROM pipeline_checkpoints pc2
                    WHERE pc2.pipeline_run_id = pc1.pipeline_run_id
                      AND pc2.attraction_id = pc1.attraction_id
                      AND pc2.status = 'completed'
                  )
            """), {
                'pipeline_run_id': pipeline_run_id
            }).fetchall()
            
            return list(results)
        finally:
            session.close()

    @staticmethod
    def is_stage_completed(pipeline_run_id: int, attraction_id: int, stage_name: str) -> bool:
        """Check if a stage is already completed for an attraction.
        
        Returns:
            True if stage is completed, False otherwise
        """
        checkpoint = CheckpointManager.get_checkpoint(pipeline_run_id, attraction_id, stage_name)
        return checkpoint is not None and checkpoint[0] == 'completed'

    @staticmethod
    def get_pipeline_progress(pipeline_run_id: int):
        """Get overall progress of a pipeline run.
        
        Returns:
            Dict with progress statistics
        """
        session = SessionLocal()
        try:
            # Get total attractions
            total = session.execute(text("""
                SELECT COUNT(DISTINCT attraction_id)
                FROM pipeline_checkpoints
                WHERE pipeline_run_id = :pipeline_run_id
            """), {'pipeline_run_id': pipeline_run_id}).scalar() or 0
            
            # Get completed attractions (all 10 stages done)
            completed = session.execute(text("""
                SELECT COUNT(DISTINCT attraction_id)
                FROM (
                    SELECT attraction_id
                    FROM pipeline_checkpoints
                    WHERE pipeline_run_id = :pipeline_run_id
                      AND status = 'completed'
                    GROUP BY attraction_id
                    HAVING COUNT(DISTINCT stage_name) = 10
                ) t
            """), {'pipeline_run_id': pipeline_run_id}).scalar() or 0
            
            # Get failed attractions
            failed = session.execute(text("""
                SELECT COUNT(DISTINCT attraction_id)
                FROM pipeline_checkpoints
                WHERE pipeline_run_id = :pipeline_run_id
                  AND status = 'failed'
            """), {'pipeline_run_id': pipeline_run_id}).scalar() or 0
            
            return {
                'total_attractions': total,
                'completed_attractions': completed,
                'failed_attractions': failed,
                'in_progress': total - completed - failed
            }
        finally:
            session.close()


checkpoint_manager = CheckpointManager()
