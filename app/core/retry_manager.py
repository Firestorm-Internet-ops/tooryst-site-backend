"""Retry queue management for rate-limited API calls."""
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy import text
from app.infrastructure.persistence.db import SessionLocal

logger = logging.getLogger(__name__)


class RetryManager:
    """Manages retry queues for rate-limited API fetchers."""

    def add_to_retry_queue(
        self,
        attraction_id: int,
        data_type: str,
        retry_after_seconds: int = 3600,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Add attraction to retry queue for a specific data type.

        Args:
            attraction_id: ID of the attraction
            data_type: Type of data (e.g., 'metadata', 'hero_images', 'weather')
            retry_after_seconds: How long to wait before retry (default 1 hour)
            error_message: Error message to store
            metadata: Additional metadata (JSON)
        """
        import json
        session = SessionLocal()

        try:
            next_run_at = datetime.utcnow() + timedelta(seconds=retry_after_seconds)

            # Use INSERT ... ON DUPLICATE KEY UPDATE for upsert
            session.execute(text("""
                INSERT INTO data_fetch_runs (
                    attraction_id, data_type, status, items_target, items_collected,
                    last_error, retry_count, next_run_at, metadata,
                    created_at, updated_at
                ) VALUES (
                    :attraction_id, :data_type, 'RATE_LIMITED', 0, 0,
                    :error_message, 1, :next_run_at, :metadata,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON DUPLICATE KEY UPDATE
                    status = 'RATE_LIMITED',
                    last_error = :error_message,
                    retry_count = retry_count + 1,
                    next_run_at = :next_run_at,
                    metadata = :metadata,
                    updated_at = CURRENT_TIMESTAMP
            """), {
                'attraction_id': attraction_id,
                'data_type': data_type,
                'error_message': error_message or f"Rate limited, retry after {retry_after_seconds}s",
                'next_run_at': next_run_at,
                'metadata': json.dumps(metadata) if metadata else None
            })

            session.commit()
            logger.info(f"Added attraction {attraction_id} to retry queue for '{data_type}' (retry at {next_run_at})")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to add to retry queue: {e}")
            raise
        finally:
            session.close()

    def get_retry_queue(
        self,
        data_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get attractions ready for retry.

        Args:
            data_type: Filter by data type (None = all types)
            limit: Maximum number to return

        Returns:
            List of retry queue items
        """
        session = SessionLocal()

        try:
            if data_type:
                query = text("""
                    SELECT
                        dfr.id, dfr.attraction_id, dfr.data_type,
                        dfr.retry_count, dfr.last_error, dfr.next_run_at,
                        dfr.metadata,
                        a.name as attraction_name, a.slug as attraction_slug,
                        c.name as city_name, c.country
                    FROM data_fetch_runs dfr
                    JOIN attractions a ON dfr.attraction_id = a.id
                    JOIN cities c ON a.city_id = c.id
                    WHERE dfr.status = 'RATE_LIMITED'
                      AND dfr.data_type = :data_type
                      AND (dfr.next_run_at IS NULL OR dfr.next_run_at <= CURRENT_TIMESTAMP)
                      AND dfr.retry_count < dfr.max_retries
                    ORDER BY dfr.next_run_at ASC
                    LIMIT :limit
                """)
                result = session.execute(query, {'data_type': data_type, 'limit': limit})
            else:
                query = text("""
                    SELECT
                        dfr.id, dfr.attraction_id, dfr.data_type,
                        dfr.retry_count, dfr.last_error, dfr.next_run_at,
                        dfr.metadata,
                        a.name as attraction_name, a.slug as attraction_slug,
                        c.name as city_name, c.country
                    FROM data_fetch_runs dfr
                    JOIN attractions a ON dfr.attraction_id = a.id
                    JOIN cities c ON a.city_id = c.id
                    WHERE dfr.status = 'RATE_LIMITED'
                      AND (dfr.next_run_at IS NULL OR dfr.next_run_at <= CURRENT_TIMESTAMP)
                      AND dfr.retry_count < dfr.max_retries
                    ORDER BY dfr.next_run_at ASC
                    LIMIT :limit
                """)
                result = session.execute(query, {'limit': limit})

            items = []
            for row in result:
                items.append({
                    'id': row.id,
                    'attraction_id': row.attraction_id,
                    'attraction_name': row.attraction_name,
                    'attraction_slug': row.attraction_slug,
                    'city_name': row.city_name,
                    'country': row.country,
                    'data_type': row.data_type,
                    'retry_count': row.retry_count,
                    'last_error': row.last_error,
                    'next_run_at': row.next_run_at,
                    'metadata': row.metadata
                })

            return items
        except Exception as e:
            logger.error(f"Failed to get retry queue: {e}")
            return []
        finally:
            session.close()

    def mark_retry_success(self, retry_id: int):
        """Mark retry attempt as successful.

        Args:
            retry_id: ID of the data_fetch_runs record
        """
        session = SessionLocal()

        try:
            session.execute(text("""
                UPDATE data_fetch_runs
                SET status = 'DONE',
                    completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :retry_id
            """), {'retry_id': retry_id})

            session.commit()
            logger.info(f"Marked retry {retry_id} as successful")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to mark retry success: {e}")
        finally:
            session.close()

    def mark_retry_failed(
        self,
        retry_id: int,
        error_message: str,
        retry_after_seconds: int = 3600
    ):
        """Mark retry attempt as failed and schedule next retry.

        Args:
            retry_id: ID of the data_fetch_runs record
            error_message: Error message
            retry_after_seconds: How long to wait before next retry
        """
        session = SessionLocal()

        try:
            next_run_at = datetime.utcnow() + timedelta(seconds=retry_after_seconds)

            session.execute(text("""
                UPDATE data_fetch_runs
                SET retry_count = retry_count + 1,
                    last_error = :error_message,
                    next_run_at = :next_run_at,
                    status = CASE
                        WHEN retry_count + 1 >= max_retries THEN 'FAILED'
                        ELSE 'RATE_LIMITED'
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :retry_id
            """), {
                'retry_id': retry_id,
                'error_message': error_message,
                'next_run_at': next_run_at
            })

            session.commit()
            logger.info(f"Marked retry {retry_id} as failed, will retry at {next_run_at}")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to mark retry failed: {e}")
        finally:
            session.close()

    def get_retry_stats(self) -> Dict[str, Any]:
        """Get statistics about retry queue.

        Returns:
            Dictionary with retry queue statistics
        """
        session = SessionLocal()

        try:
            result = session.execute(text("""
                SELECT
                    data_type,
                    status,
                    COUNT(*) as count
                FROM data_fetch_runs
                GROUP BY data_type, status
            """))

            stats = {}
            for row in result:
                if row.data_type not in stats:
                    stats[row.data_type] = {}
                stats[row.data_type][row.status] = row.count

            return stats
        except Exception as e:
            logger.error(f"Failed to get retry stats: {e}")
            return {}
        finally:
            session.close()


# Global retry manager instance
retry_manager = RetryManager()
