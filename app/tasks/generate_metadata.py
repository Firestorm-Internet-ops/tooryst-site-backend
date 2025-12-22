"""CLI task for bulk generating metadata for all pages."""
import logging
import sys
from sqlalchemy.orm import Session
from app.infrastructure.persistence.db import SessionLocal
from app.services.metadata.bulk_generator import BulkMetadataGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_all_metadata():
    """Generate metadata for all attractions, cities, and static pages."""
    db: Session = SessionLocal()
    
    try:
        generator = BulkMetadataGenerator()
        stats = generator.generate_all(db)
        
        # Print summary
        print("\n" + "=" * 60)
        print("METADATA GENERATION SUMMARY")
        print("=" * 60)
        print(f"Attractions: {stats['attractions']['generated']} generated, {stats['attractions']['failed']} failed")
        print(f"Cities: {stats['cities']['generated']} generated, {stats['cities']['failed']} failed")
        print(f"Static Pages: {stats['static_pages']['generated']} generated")
        print(f"TOTAL: {stats['total']['generated']} generated, {stats['total']['failed']} failed")
        print("=" * 60)
        
        if stats['total']['failed'] > 0:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Failed to generate metadata: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    generate_all_metadata()
