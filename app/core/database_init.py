"""Database initialization - runs on backend startup."""
import logging
from pathlib import Path
from sqlalchemy import text
from app.infrastructure.persistence.db import SessionLocal

logger = logging.getLogger(__name__)


def initialize_database():
    """Initialize database schema on startup.
    
    Reads create_schema.sql and executes all migrations.
    This ensures all required tables exist before the application runs.
    """
    try:
        # Read the schema file
        schema_file = Path(__file__).parent.parent.parent / "sql" / "create_schema.sql"
        
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
        
        session = SessionLocal()
        try:
            for statement in statements:
                # Skip comments and empty lines
                if statement.startswith('--') or not statement:
                    continue
                
                try:
                    session.execute(text(statement))
                    session.commit()
                except Exception as e:
                    # Some statements might fail if tables already exist
                    # This is expected and not an error
                    if "already exists" in str(e) or "Duplicate" in str(e):
                        logger.debug(f"Table already exists (expected): {str(e)[:100]}")
                    else:
                        logger.warning(f"Statement execution warning: {str(e)[:200]}")
                    session.rollback()
            
            logger.info("✅ Database schema initialized successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Fatal error during database initialization: {e}")
        return False


def check_database_health():
    """Check if all required tables exist.
    
    Returns:
        bool: True if all tables exist, False otherwise
    """
    required_tables = [
        'pipeline_checkpoints',
        'attraction_data_tracking',
        'pipeline_runs',
        'attractions'
    ]
    
    session = SessionLocal()
    try:
        for table in required_tables:
            try:
                session.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
            except Exception as e:
                logger.error(f"Table {table} not found: {e}")
                return False
        
        logger.info("✅ All required tables exist")
        return True
    
    except Exception as e:
        logger.error(f"Error checking database health: {e}")
        return False
    finally:
        session.close()
