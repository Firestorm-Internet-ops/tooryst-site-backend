"""Update city coordinates from their attractions."""
import sys
import os
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from app.infrastructure.persistence.models import City, Attraction
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Update city coordinates based on average of their attractions."""
    
    # Database connection
    db_url = os.getenv('DATABASE_URL', 'mysql+pymysql://root:root@localhost:3306/storyboard')
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # Get all cities
        cities = session.query(City).all()
        
        logger.info(f"Updating coordinates for {len(cities)} cities...")
        
        updated_count = 0
        for city in cities:
            # Get average lat/lng from attractions
            result = session.query(
                func.avg(Attraction.latitude).label('avg_lat'),
                func.avg(Attraction.longitude).label('avg_lng'),
                func.count(Attraction.id).label('count')
            ).filter(
                Attraction.city_id == city.id,
                Attraction.latitude.isnot(None),
                Attraction.longitude.isnot(None)
            ).first()
            
            if result and result.count > 0:
                city.latitude = result.avg_lat
                city.longitude = result.avg_lng
                updated_count += 1
                logger.info(f"✓ {city.name}: lat={result.avg_lat:.4f}, lng={result.avg_lng:.4f} (from {result.count} attractions)")
            else:
                logger.warning(f"✗ {city.name}: No attractions with coordinates")
        
        session.commit()
        logger.info(f"\n{'='*80}")
        logger.info(f"Updated {updated_count}/{len(cities)} cities")
        logger.info(f"{'='*80}")
        
    finally:
        session.close()


if __name__ == "__main__":
    main()
