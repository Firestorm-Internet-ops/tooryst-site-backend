"""Create production improvement tables (system_alerts and pipeline_runs)."""
import os
import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from infrastructure.persistence.storage_functions import get_db_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tables():
    """Create system_alerts and pipeline_runs tables."""
    logger.info("Creating production improvement tables...")
    
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # Create system_alerts table
                logger.info("Creating system_alerts table...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS system_alerts (
                        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        title VARCHAR(255) NOT NULL,
                        message TEXT NOT NULL,
                        metadata JSON,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        acknowledged BOOLEAN DEFAULT FALSE,
                        acknowledged_at TIMESTAMP NULL,
                        acknowledged_by VARCHAR(100) NULL,
                        INDEX idx_system_alerts_created_at (created_at),
                        INDEX idx_system_alerts_alert_type (alert_type),
                        INDEX idx_system_alerts_severity (severity),
                        INDEX idx_system_alerts_acknowledged (acknowledged)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                logger.info("✓ system_alerts table created")
                
                # Create pipeline_runs table
                logger.info("Creating pipeline_runs table...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP NULL,
                        status VARCHAR(20) NOT NULL,
                        attractions_processed INT DEFAULT 0,
                        error_message TEXT,
                        INDEX idx_pipeline_runs_started_at (started_at),
                        INDEX idx_pipeline_runs_status (status)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                logger.info("✓ pipeline_runs table created")
                
                conn.commit()
                
                # Verify tables exist
                cursor.execute("SHOW TABLES LIKE 'system_alerts'")
                if cursor.fetchone():
                    logger.info("✓ Verified: system_alerts table exists")
                
                cursor.execute("SHOW TABLES LIKE 'pipeline_runs'")
                if cursor.fetchone():
                    logger.info("✓ Verified: pipeline_runs table exists")
                
                logger.info("\n✓ All production tables created successfully!")
                return True
                
        finally:
            conn.close()
        
    except Exception as e:
        logger.error(f"✗ Failed to create tables: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = create_tables()
    sys.exit(0 if success else 1)
