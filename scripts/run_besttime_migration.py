"""Run migration to add missing columns to best_time_data table."""
import os
import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from infrastructure.persistence.storage_functions import get_db_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_migration():
    """Recreate best_time_data table with correct schema."""
    logger.info("Running best_time_data table recreation...")

    try:
        # Execute migration
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # Drop existing table
                logger.info("Dropping existing best_time_data table...")
                cursor.execute("DROP TABLE IF EXISTS best_time_data")

                # Recreate table with correct schema
                logger.info("Creating best_time_data table with correct schema...")
                cursor.execute("""
                    CREATE TABLE best_time_data (
                        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        attraction_id BIGINT UNSIGNED NOT NULL,
                        day_type ENUM('regular', 'special') NOT NULL DEFAULT 'regular',
                        date_local DATE NULL,
                        day_int TINYINT NULL,
                        day_name VARCHAR(16) NOT NULL,
                        -- Card data
                        is_open_today BOOLEAN NOT NULL DEFAULT FALSE,
                        is_open_now BOOLEAN NOT NULL DEFAULT FALSE,
                        today_opening_time TIME,
                        today_closing_time TIME,
                        crowd_level_today INT,
                        best_time_today VARCHAR(64),
                        -- Section data
                        reason_text VARCHAR(1024),
                        hourly_crowd_levels JSON,
                        -- Metadata
                        data_source VARCHAR(32) DEFAULT 'besttime',
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uq_best_time_attraction_type_day (attraction_id, day_type, day_int, date_local),
                        INDEX idx_best_time_attraction (attraction_id),
                        INDEX idx_best_time_date (date_local),
                        INDEX idx_best_time_day_int (day_int),
                        INDEX idx_best_time_type (day_type),
                        CONSTRAINT fk_best_time_attraction FOREIGN KEY (attraction_id) REFERENCES attractions(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)

                conn.commit()
                logger.info("✓ Table recreation completed successfully")

                # Verify columns exist
                cursor.execute("DESCRIBE best_time_data")
                columns = cursor.fetchall()
                column_names = [col['Field'] for col in columns]

                if 'is_open_now' in column_names:
                    logger.info("✓ Verified: is_open_now column exists")
                else:
                    logger.error("✗ is_open_now column not found after recreation")
                    return False

                # Check crowd_level_today type
                crowd_col = next((col for col in columns if col['Field'] == 'crowd_level_today'), None)
                if crowd_col and 'int' in crowd_col['Type'].lower():
                    logger.info("✓ Verified: crowd_level_today is INT type")
                else:
                    logger.warning(f"✗ crowd_level_today type is {crowd_col['Type'] if crowd_col else 'unknown'}")

                # Show relevant columns
                logger.info("\nBest time data columns:")
                for col in columns:
                    if col['Field'] in ['is_open_today', 'is_open_now', 'crowd_level_today']:
                        logger.info(f"  {col['Field']}: {col['Type']}")

        finally:
            conn.close()

        return True

    except Exception as e:
        logger.error(f"✗ Migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)