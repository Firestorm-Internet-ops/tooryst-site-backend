"""Script to identify and fix attractions with invalid opening hours."""
import asyncio
import sys
import os
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.infrastructure.persistence.models import Attraction
from app.infrastructure.external_apis.metadata_fetcher import MetadataFetcherImpl
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_invalid_opening_hours(opening_hours):
    """Check if opening hours data is invalid."""
    if not opening_hours:
        return False
    
    for day_hours in opening_hours:
        is_closed = day_hours.get('is_closed', False)
        open_time = day_hours.get('open_time')
        close_time = day_hours.get('close_time')
        
        # Invalid: is_closed=false but open_time or close_time is null
        if not is_closed and (open_time is None or close_time is None):
            return True
    
    return False


async def fix_attraction_hours(attraction, metadata_fetcher, session):
    """Re-fetch metadata for an attraction with invalid hours."""
    logger.info(f"Re-fetching metadata for: {attraction.name}")
    
    try:
        # Fetch new metadata
        result = await metadata_fetcher.fetch(
            attraction_id=attraction.id,
            place_id=attraction.place_id,
            attraction_name=attraction.name,
            city_name=attraction.city.name if attraction.city else "Unknown"
        )
        
        if not result:
            logger.error(f"Failed to fetch metadata for {attraction.name}")
            return False
        
        metadata = result.get('metadata', {})
        new_opening_hours = metadata.get('opening_hours', [])
        
        # Check if new hours are valid
        if check_invalid_opening_hours(new_opening_hours):
            logger.warning(f"New opening hours for {attraction.name} are still invalid!")
            return False
        
        # Update the attraction
        attraction.opening_hours = new_opening_hours
        attraction.contact_info = metadata.get('contact_info', {})
        attraction.accessibility_info = metadata.get('accessibility_info')
        attraction.best_season = metadata.get('best_season')
        attraction.short_description = metadata.get('short_description')
        attraction.recommended_duration_minutes = metadata.get('recommended_duration_minutes')
        attraction.highlights = metadata.get('highlights', [])
        
        session.commit()
        logger.info(f"✓ Successfully updated {attraction.name}")
        return True
        
    except Exception as e:
        logger.error(f"Error fixing {attraction.name}: {e}")
        session.rollback()
        return False


async def main():
    """Main function to identify and fix invalid opening hours."""
    
    # Database connection
    db_url = os.getenv('DATABASE_URL', 'mysql+pymysql://root:root@localhost:3306/storyboard')
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # Find all attractions
        attractions = session.execute(
            select(Attraction)
        ).scalars().all()
        
        logger.info(f"Checking {len(attractions)} attractions for invalid opening hours...")
        
        # Identify attractions with invalid hours
        invalid_attractions = []
        for attraction in attractions:
            if check_invalid_opening_hours(attraction.opening_hours):
                invalid_attractions.append(attraction)
                logger.warning(f"Invalid hours: {attraction.name} (ID: {attraction.id})")
        
        if not invalid_attractions:
            logger.info("✓ No attractions with invalid opening hours found!")
            return
        
        logger.info(f"\nFound {len(invalid_attractions)} attractions with invalid opening hours")
        
        # Ask for confirmation
        print("\nAttractions to fix:")
        for attr in invalid_attractions:
            print(f"  - {attr.name} (ID: {attr.id})")
        
        response = input(f"\nRe-fetch metadata for these {len(invalid_attractions)} attractions? (y/n): ")
        if response.lower() != 'y':
            logger.info("Cancelled by user")
            return
        
        # Initialize services
        metadata_fetcher = MetadataFetcherImpl()
        
        # Fix each attraction
        success_count = 0
        for attraction in invalid_attractions:
            if await fix_attraction_hours(attraction, metadata_fetcher, session):
                success_count += 1
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Fixed {success_count}/{len(invalid_attractions)} attractions")
        logger.info(f"{'='*80}")
        
    finally:
        session.close()


if __name__ == "__main__":
    asyncio.run(main())
