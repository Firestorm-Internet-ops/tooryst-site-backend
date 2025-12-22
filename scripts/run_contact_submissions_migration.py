"""Migration script to create contact_submissions table."""
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
    """Create contact_submissions table."""
    logger.info("Running contact_submissions table creation...")

    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # Create contact_submissions table
                logger.info("Creating contact_submissions table...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS contact_submissions (
                        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        email VARCHAR(255) NOT NULL,
                        subject VARCHAR(512),
                        message TEXT NOT NULL,
                        status ENUM('new', 'read', 'responded') NOT NULL DEFAULT 'new',
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_contact_status (status),
                        INDEX idx_contact_created (created_at DESC),
                        INDEX idx_contact_email (email)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)

                conn.commit()
                logger.info("✓ contact_submissions table created successfully")

                # Verify table exists
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    AND table_name = 'contact_submissions'
                """)
                result = cursor.fetchone()
                if result['count'] == 1:
                    logger.info("✓ Table verified in database")

                    # Show table structure
                    cursor.execute("DESCRIBE contact_submissions")
                    columns = cursor.fetchall()
                    logger.info("\nContact submissions table structure:")
                    for col in columns:
                        logger.info(f"  {col['Field']}: {col['Type']} {col['Null']} {col['Key']} {col['Default']}")
                else:
                    logger.error("✗ Warning: Table not found after creation")
                    return False

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
    if success:
        print("\n✓ Migration completed successfully!")
    else:
        print("\n✗ Migration failed!")
        sys.exit(1)
