"""Run migration to create system_alerts table."""
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
    """Create system_alerts table."""
    migration_file = Path(__file__).parent.parent / "migrations" / "create_system_alerts_table.sql"
    
    logger.info("Running system_alerts table migration...")
    
    try:
        # Read migration SQL
        with open(migration_file, 'r') as f:
            sql = f.read()
        
        # Execute migration
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                conn.commit()
                logger.info("✓ system_alerts table created successfully")
                
                # Verify table exists
                cursor.execute("SHOW TABLES LIKE 'system_alerts'")
                result = cursor.fetchone()
                if result:
                    logger.info("✓ Verified: system_alerts table exists")
                    
                    # Show table structure
                    cursor.execute("DESCRIBE system_alerts")
                    columns = cursor.fetchall()
                    logger.info("\nTable structure:")
                    for col in columns:
                        logger.info(f"  {col['Field']}: {col['Type']}")
                else:
                    logger.error("✗ Table creation verification failed")
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
    sys.exit(0 if success else 1)
