"""Script to fetch and update place_ids for existing attractions."""
import sys
import asyncio
from pathlib import Path
from datetime import datetime
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.infrastructure.external_apis.google_places_client import GooglePlacesClient
from app.infrastructure.persistence.storage_functions import get_all_attractions, get_db_connection
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def update_place_ids():
    """Fetch and update place_ids for all attractions."""
    logger.info("=" * 80)
    logger.info("UPDATING PLACE IDS FOR ATTRACTIONS")
    logger.info("=" * 80)
    logger.info("")
    
    # Get all attractions
    attractions = get_all_attractions()
    logger.info(f"Found {len(attractions)} attractions")
    logger.info("")
    
    # Initialize Google Places client
    places_client = GooglePlacesClient()
    
    # Track results
    updated = 0
    already_had = 0
    not_found = 0
    errors = 0
    
    for idx, attraction in enumerate(attractions, 1):
        attraction_id = attraction['id']
        name = attraction['name']
        city = attraction['city_name']
        lat = attraction['latitude']
        lng = attraction['longitude']
        existing_place_id = attraction['place_id']
        
        logger.info(f"[{idx}/{len(attractions)}] {name} ({city})")
        
        # Skip if already has place_id
        if existing_place_id:
            logger.info(f"  ✓ Already has place_id: {existing_place_id}")
            already_had += 1
            continue
        
        # Build search query
        query = f"{name} {city}"
        
        try:
            # Find place
            result = await places_client.find_place(
                query=query,
                latitude=float(lat) if lat else None,
                longitude=float(lng) if lng else None
            )
            
            if result and result.get('place_id'):
                place_id = result['place_id']
                
                # Update database
                conn = get_db_connection()
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE attractions SET place_id = %s WHERE id = %s",
                            (place_id, attraction_id)
                        )
                    conn.commit()
                    logger.info(f"  ✓ Updated with place_id: {place_id}")
                    updated += 1
                except Exception as e:
                    logger.error(f"  ✗ Database error: {e}")
                    errors += 1
                finally:
                    conn.close()
            else:
                logger.warning(f"  ⚠ No place found")
                not_found += 1
                
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            errors += 1
        
        logger.info("")
    
    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total attractions: {len(attractions)}")
    logger.info(f"  ✓ Updated: {updated}")
    logger.info(f"  ✓ Already had place_id: {already_had}")
    logger.info(f"  ⚠ Not found: {not_found}")
    logger.info(f"  ✗ Errors: {errors}")
    logger.info("")


if __name__ == "__main__":
    asyncio.run(update_place_ids())
