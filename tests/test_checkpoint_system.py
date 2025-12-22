"""Tests for pipeline checkpoint and resume system."""
import pytest
from sqlalchemy import text
from app.core.checkpoint_manager import checkpoint_manager
from app.infrastructure.persistence.db import SessionLocal


class TestCheckpointManager:
    """Test checkpoint manager functionality."""

    def test_create_checkpoint(self):
        """Test creating a checkpoint."""
        checkpoint_manager.create_checkpoint(
            pipeline_run_id=1,
            attraction_id=100,
            stage_name='metadata',
            status='completed'
        )
        
        checkpoint = checkpoint_manager.get_checkpoint(1, 100, 'metadata')
        assert checkpoint is not None
        assert checkpoint[0] == 'completed'

    def test_is_stage_completed(self):
        """Test checking if stage is completed."""
        checkpoint_manager.create_checkpoint(
            pipeline_run_id=2,
            attraction_id=101,
            stage_name='hero_images',
            status='completed'
        )
        
        assert checkpoint_manager.is_stage_completed(2, 101, 'hero_images')
        assert not checkpoint_manager.is_stage_completed(2, 101, 'best_time')

    def test_get_last_completed_stage(self):
        """Test getting the last completed stage."""
        # Create multiple checkpoints
        checkpoint_manager.create_checkpoint(3, 102, 'metadata', 'completed')
        checkpoint_manager.create_checkpoint(3, 102, 'hero_images', 'completed')
        checkpoint_manager.create_checkpoint(3, 102, 'best_time', 'completed')
        
        last_stage = checkpoint_manager.get_last_completed_stage(3, 102)
        assert last_stage in ['metadata', 'hero_images', 'best_time']

    def test_get_resumable_attractions(self):
        """Test getting attractions that can be resumed."""
        # Create checkpoints for multiple attractions
        checkpoint_manager.create_checkpoint(4, 103, 'metadata', 'completed')
        checkpoint_manager.create_checkpoint(4, 103, 'hero_images', 'completed')
        checkpoint_manager.create_checkpoint(4, 104, 'metadata', 'completed')
        
        resumable = checkpoint_manager.get_resumable_attractions(4)
        assert len(resumable) >= 2
        
        # Check that we have the right attractions
        attraction_ids = [a[0] for a in resumable]
        assert 103 in attraction_ids
        assert 104 in attraction_ids

    def test_get_pipeline_progress(self):
        """Test getting pipeline progress."""
        # Create checkpoints for a pipeline
        pipeline_id = 5
        
        # 3 attractions with all 10 stages completed
        for i in range(3):
            for stage in ['metadata', 'hero_images', 'best_time', 'weather', 'tips',
                         'map', 'reviews', 'social_videos', 'nearby', 'audiences']:
                checkpoint_manager.create_checkpoint(
                    pipeline_id, 200 + i, stage, 'completed'
                )
        
        # 2 attractions with partial progress
        for i in range(2):
            checkpoint_manager.create_checkpoint(pipeline_id, 210 + i, 'metadata', 'completed')
            checkpoint_manager.create_checkpoint(pipeline_id, 210 + i, 'hero_images', 'completed')
        
        # 1 attraction with failed stage
        checkpoint_manager.create_checkpoint(pipeline_id, 220, 'metadata', 'completed')
        checkpoint_manager.create_checkpoint(pipeline_id, 220, 'hero_images', 'failed')
        
        progress = checkpoint_manager.get_pipeline_progress(pipeline_id)
        
        assert progress['total_attractions'] >= 6
        assert progress['completed_attractions'] >= 3
        assert progress['failed_attractions'] >= 1

    def test_checkpoint_with_metadata(self):
        """Test creating checkpoint with metadata."""
        metadata = {'reason': 'test', 'retry_count': 2}
        checkpoint_manager.create_checkpoint(
            pipeline_run_id=6,
            attraction_id=300,
            stage_name='metadata',
            status='completed',
            metadata=metadata
        )
        
        checkpoint = checkpoint_manager.get_checkpoint(6, 300, 'metadata')
        assert checkpoint is not None

    def test_checkpoint_update(self):
        """Test updating an existing checkpoint."""
        # Create initial checkpoint
        checkpoint_manager.create_checkpoint(7, 301, 'metadata', 'completed')
        
        # Update it
        checkpoint_manager.create_checkpoint(7, 301, 'metadata', 'failed')
        
        checkpoint = checkpoint_manager.get_checkpoint(7, 301, 'metadata')
        assert checkpoint[0] == 'failed'

    def test_failed_checkpoint(self):
        """Test creating a failed checkpoint."""
        checkpoint_manager.create_checkpoint(
            pipeline_run_id=8,
            attraction_id=302,
            stage_name='best_time',
            status='failed'
        )
        
        checkpoint = checkpoint_manager.get_checkpoint(8, 302, 'best_time')
        assert checkpoint[0] == 'failed'

    def test_skipped_checkpoint(self):
        """Test creating a skipped checkpoint."""
        checkpoint_manager.create_checkpoint(
            pipeline_run_id=9,
            attraction_id=303,
            stage_name='weather',
            status='skipped'
        )
        
        checkpoint = checkpoint_manager.get_checkpoint(9, 303, 'weather')
        assert checkpoint[0] == 'skipped'


class TestCheckpointIntegration:
    """Integration tests for checkpoint system."""

    def test_full_pipeline_checkpoint_flow(self):
        """Test a full pipeline checkpoint flow."""
        pipeline_id = 100
        attraction_id = 400
        
        stages = ['metadata', 'hero_images', 'best_time', 'weather', 'tips']
        
        # Simulate pipeline processing
        for stage in stages:
            # Check if already completed (should be false first time)
            is_completed = checkpoint_manager.is_stage_completed(pipeline_id, attraction_id, stage)
            assert not is_completed
            
            # Process stage (simulated)
            # ...
            
            # Record completion
            checkpoint_manager.create_checkpoint(
                pipeline_id, attraction_id, stage, 'completed'
            )
            
            # Verify it's now completed
            is_completed = checkpoint_manager.is_stage_completed(pipeline_id, attraction_id, stage)
            assert is_completed
        
        # Get last completed stage
        last_stage = checkpoint_manager.get_last_completed_stage(pipeline_id, attraction_id)
        assert last_stage == 'tips'

    def test_resume_scenario(self):
        """Test a realistic resume scenario."""
        pipeline_id = 101
        
        # Simulate 3 attractions at different stages
        attractions = [
            (401, ['metadata', 'hero_images', 'best_time']),  # 3 stages done
            (402, ['metadata', 'hero_images']),                # 2 stages done
            (403, ['metadata']),                               # 1 stage done
        ]
        
        for attraction_id, completed_stages in attractions:
            for stage in completed_stages:
                checkpoint_manager.create_checkpoint(
                    pipeline_id, attraction_id, stage, 'completed'
                )
        
        # Get resumable attractions
        resumable = checkpoint_manager.get_resumable_attractions(pipeline_id)
        assert len(resumable) == 3
        
        # Verify each attraction's last stage
        for attraction_id, last_stage in resumable:
            if attraction_id == 401:
                assert last_stage == 'best_time'
            elif attraction_id == 402:
                assert last_stage == 'hero_images'
            elif attraction_id == 403:
                assert last_stage == 'metadata'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
