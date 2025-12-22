"""Pipeline stage coordination and queue management."""
import os
import time
import logging
from typing import Optional, List
import redis

logger = logging.getLogger(__name__)


class StageManager:
    """Manages pipeline stages with Redis-based queues and semaphores."""

    def __init__(self):
        """Initialize stage manager with Redis connection."""
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_client = None

        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=4,  # Use separate DB for stage management
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("✓ Stage manager connected to Redis")
        except Exception as e:
            logger.error(f"✗ Stage manager: Redis not available: {e}")
            raise RuntimeError(f"Stage manager requires Redis for coordination: {e}")

    def acquire_stage_slot(self, stage_name: str, max_concurrent: int = 5, timeout: int = 30) -> bool:
        """Try to acquire a processing slot for this stage.

        Args:
            stage_name: Name of the stage (e.g., 'metadata', 'hero_images')
            max_concurrent: Maximum concurrent processes for this stage
            timeout: How long to wait for a slot (seconds)

        Returns:
            True if slot acquired, False if timeout
        """
        key = f"stage_semaphore:{stage_name}"
        start_time = time.time()

        while True:
            try:
                # Atomically increment and get current value
                current = self.redis_client.incr(key)

                if current <= max_concurrent:
                    # Got a slot
                    logger.debug(f"Acquired slot {current}/{max_concurrent} for stage '{stage_name}'")
                    return True
                else:
                    # No slots available, decrement back
                    self.redis_client.decr(key)

                    # Check timeout
                    if time.time() - start_time >= timeout:
                        logger.warning(f"Timeout waiting for slot in stage '{stage_name}'")
                        return False

                    # Wait briefly before retry
                    time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error acquiring stage slot: {e}")
                return False

    def release_stage_slot(self, stage_name: str):
        """Release a processing slot for this stage.

        Args:
            stage_name: Name of the stage
        """
        key = f"stage_semaphore:{stage_name}"

        try:
            current = self.redis_client.decr(key)
            # Ensure it doesn't go below 0
            if current < 0:
                self.redis_client.set(key, 0)
            logger.debug(f"Released slot for stage '{stage_name}' (now {max(0, current)} active)")
        except Exception as e:
            logger.error(f"Error releasing stage slot: {e}")

    def push_to_stage(self, stage_name: str, attraction_id: int, pipeline_run_id: int):
        """Add attraction to stage queue.

        Args:
            stage_name: Name of the stage
            attraction_id: ID of the attraction
            pipeline_run_id: ID of the pipeline run
        """
        queue_key = f"stage_queue:{stage_name}"

        try:
            # Use sorted set with timestamp as score for FIFO ordering
            # Store as "pipeline_run_id:attraction_id" for tracking
            member = f"{pipeline_run_id}:{attraction_id}"
            score = time.time()

            self.redis_client.zadd(queue_key, {member: score})
            logger.debug(f"Pushed attraction {attraction_id} to stage '{stage_name}' queue")
        except Exception as e:
            logger.error(f"Error pushing to stage queue: {e}")
            raise

    def pop_from_stage(self, stage_name: str) -> Optional[tuple[int, int]]:
        """Get next attraction from stage queue.

        Args:
            stage_name: Name of the stage

        Returns:
            Tuple of (pipeline_run_id, attraction_id) or None if queue empty
        """
        queue_key = f"stage_queue:{stage_name}"

        try:
            # Pop minimum (oldest) item from sorted set
            items = self.redis_client.zpopmin(queue_key, 1)

            if not items:
                return None

            # Parse "pipeline_run_id:attraction_id"
            member, score = items[0]
            pipeline_run_id, attraction_id = map(int, member.split(':'))

            logger.debug(f"Popped attraction {attraction_id} from stage '{stage_name}' queue")
            return (pipeline_run_id, attraction_id)
        except Exception as e:
            logger.error(f"Error popping from stage queue: {e}")
            return None

    def get_queue_depth(self, stage_name: str) -> int:
        """Get number of items waiting in stage queue.

        Args:
            stage_name: Name of the stage

        Returns:
            Number of attractions in queue
        """
        queue_key = f"stage_queue:{stage_name}"

        try:
            return self.redis_client.zcard(queue_key)
        except Exception as e:
            logger.error(f"Error getting queue depth: {e}")
            return 0

    def get_active_count(self, stage_name: str) -> int:
        """Get number of actively processing items in stage.

        Args:
            stage_name: Name of the stage

        Returns:
            Number of active processes
        """
        key = f"stage_semaphore:{stage_name}"

        try:
            count = self.redis_client.get(key)
            return int(count) if count else 0
        except Exception as e:
            logger.error(f"Error getting active count: {e}")
            return 0

    def clear_stage_queue(self, stage_name: str):
        """Clear all items from stage queue (for cleanup/testing).

        Args:
            stage_name: Name of the stage
        """
        queue_key = f"stage_queue:{stage_name}"

        try:
            self.redis_client.delete(queue_key)
            logger.info(f"Cleared stage queue: {stage_name}")
        except Exception as e:
            logger.error(f"Error clearing stage queue: {e}")

    def reset_stage_semaphore(self, stage_name: str):
        """Reset semaphore counter (for cleanup/recovery).

        Args:
            stage_name: Name of the stage
        """
        key = f"stage_semaphore:{stage_name}"

        try:
            self.redis_client.set(key, 0)
            logger.info(f"Reset semaphore for stage: {stage_name}")
        except Exception as e:
            logger.error(f"Error resetting semaphore: {e}")

    def get_pipeline_progress(self, pipeline_run_id: int) -> dict:
        """Get progress statistics for a pipeline run across all stages.

        Args:
            pipeline_run_id: ID of the pipeline run

        Returns:
            Dictionary with stage statistics
        """
        stages = ['metadata', 'hero_images']  # Currently active stages
        progress = {
            'pipeline_run_id': pipeline_run_id,
            'stages': {}
        }

        try:
            for stage in stages:
                queue_key = f"stage_queue:{stage}"

                # Count items for this pipeline run in queue
                all_items = self.redis_client.zrange(queue_key, 0, -1)
                in_queue = sum(1 for item in all_items if item.startswith(f"{pipeline_run_id}:"))

                progress['stages'][stage] = {
                    'in_queue': in_queue,
                    'active': self.get_active_count(stage),
                    'total_queue_depth': self.get_queue_depth(stage)
                }
        except Exception as e:
            logger.error(f"Error getting pipeline progress: {e}")

        return progress


# Global stage manager instance
stage_manager = StageManager()
