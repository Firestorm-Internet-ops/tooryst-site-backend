#!/usr/bin/env python3
"""
Script to populate missing data in nearby_attractions table.

Handles two cases:
1. Nearby attractions in our database (nearby_attraction_id is NOT NULL):
   - Fetches rating, user_ratings_total, review_count from attractions table
   - Fetches image_url from hero_images table

2. Nearby attractions from Google Places (nearby_attraction_id is NULL):
   - Fetches rating, user_ratings_total, image_url from Google Places API
"""
import sys
import asyncio
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add backend to path
sys.path.insert(0, '/Users/deepak/Desktop/storyboard/backend')

from app.config import settings
from app.infrastructure.persistence import models
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.external_apis.google_places_client import GooglePlacesClient

# Hardcoded Google Places API Key
GOOGLE_PLACES_API_KEY = "AIzaSyAR1m6tnIeBJV0pqUpDXhmI3gW1DcJW3nI"  # Replace with your actual API key


async def enrich_from_google_places(nearby: models.NearbyAttraction, places_client: GooglePlacesClient) -> bool:
    """
    Enrich nearby attraction with data from Google Places API.
    
    Args:
        nearby: NearbyAttraction record to enrich
        places_client: GooglePlacesClient instance
    
    Returns:
        True if any data was updated, False otherwise
    """
    if not nearby.place_id:
        logger.warning(f"  No place_id for {nearby.name}, skipping Google enrichment")
        return False
    
    try:
        # Fetch place details from Google Places API
        place_details = await places_client.get_place_details(nearby.place_id)
        
        if not place_details:
            logger.warning(f"  Failed to fetch place details from Google for {nearby.name}")
            return False
        
        # Debug: Log what we got from API
        logger.info(f"  API Response keys: {list(place_details.keys())}")
        logger.info(f"  rating: {place_details.get('rating')}")
        logger.info(f"  userRatingCount: {place_details.get('userRatingCount')}")
        logger.info(f"  photos: {len(place_details.get('photos', []))} photos")
        
        needs_update = False
        
        # Update rating if missing
        if nearby.rating is None and place_details.get('rating'):
            nearby.rating = float(place_details.get('rating'))
            needs_update = True
            logger.info(f"  ✓ Updated rating from Google: {nearby.rating}")
        
        # Update user_ratings_total if missing
        if nearby.user_ratings_total is None and place_details.get('userRatingCount'):
            nearby.user_ratings_total = place_details.get('userRatingCount')
            needs_update = True
            logger.info(f"  ✓ Updated user_ratings_total from Google: {nearby.user_ratings_total}")
        
        # Update review_count if missing
        if nearby.review_count is None and place_details.get('userRatingCount'):
            nearby.review_count = place_details.get('userRatingCount')
            needs_update = True
            logger.info(f"  ✓ Updated review_count from Google: {nearby.review_count}")
        
        # Update image_url if missing
        if nearby.image_url is None and place_details.get('photos'):
            photos = place_details.get('photos', [])
            if photos:
                photo_name = photos[0].get('name')
                if photo_name:
                    # Construct photo URL from Places API v1
                    image_url = f"https://places.googleapis.com/v1/{photo_name}/media?maxWidthPx=400&key={places_client.api_key}"
                    nearby.image_url = image_url
                    needs_update = True
                    logger.info(f"  ✓ Updated image_url from Google")
        
        if not needs_update:
            logger.info(f"  ⚠ No data to update (all fields empty or already have values)")
        
        return needs_update
        
    except Exception as e:
        logger.error(f"  Error enriching from Google Places: {e}")
        return False


def populate_nearby_attractions_data():
    """
    Populate missing data in nearby_attractions table.
    
    Handles two cases:
    1. Nearby attractions in database (nearby_attraction_id is NOT NULL):
       - Fetches from attractions and hero_images tables
    
    2. Nearby attractions from Google Places (nearby_attraction_id is NULL):
       - Fetches from Google Places API
    """
    session = SessionLocal()
    
    try:
        # Debug: Check total count of nearby attractions
        total_count = session.query(models.NearbyAttraction).count()
        logger.info(f"Total nearby attractions in database: {total_count}")
        
        # Debug: Check each condition separately
        rating_null = session.query(models.NearbyAttraction).filter(models.NearbyAttraction.rating.is_(None)).count()
        user_ratings_null = session.query(models.NearbyAttraction).filter(models.NearbyAttraction.user_ratings_total.is_(None)).count()
        review_count_null = session.query(models.NearbyAttraction).filter(models.NearbyAttraction.review_count.is_(None)).count()
        image_url_null = session.query(models.NearbyAttraction).filter(models.NearbyAttraction.image_url.is_(None)).count()
        
        logger.info(f"Debug - rating IS NULL: {rating_null}")
        logger.info(f"Debug - user_ratings_total IS NULL: {user_ratings_null}")
        logger.info(f"Debug - review_count IS NULL: {review_count_null}")
        logger.info(f"Debug - image_url IS NULL: {image_url_null}")
        
        # Find all nearby attractions with missing data
        nearby_rows = (
            session.query(models.NearbyAttraction)
            .filter(
                (models.NearbyAttraction.rating.is_(None)) |
                (models.NearbyAttraction.user_ratings_total.is_(None)) |
                (models.NearbyAttraction.review_count.is_(None)) |
                (models.NearbyAttraction.image_url.is_(None))
            )
            .all()
        )
        
        logger.info(f"Found {len(nearby_rows)} nearby attractions with missing data")
        
        # Debug: Show sample of what's in the database
        if len(nearby_rows) == 0:
            sample = session.query(models.NearbyAttraction).limit(5).all()
            logger.info(f"Sample of nearby attractions in DB:")
            for s in sample:
                logger.info(f"  - {s.name}: rating={s.rating}, user_ratings_total={s.user_ratings_total}, review_count={s.review_count}, image_url={s.image_url}")
        else:
            logger.info(f"First 5 attractions with missing data:")
            for s in nearby_rows[:5]:
                logger.info(f"  - {s.name}: rating={s.rating}, user_ratings_total={s.user_ratings_total}, review_count={s.review_count}, image_url={s.image_url}")
        
        updated_count = 0
        db_count = 0
        google_count = 0
        
        # Initialize Google Places client with hardcoded API key
        places_client = GooglePlacesClient(api_key=GOOGLE_PLACES_API_KEY)
        
        for nearby in nearby_rows:
            logger.info(f"\nProcessing: {nearby.name} (id: {nearby.id})")
            logger.info(f"  Current - rating: {nearby.rating}, user_ratings_total: {nearby.user_ratings_total}, review_count: {nearby.review_count}, image: {nearby.image_url}")
            
            needs_update = False
            
            # Case 1: Nearby attraction is in our database
            if nearby.nearby_attraction_id is not None:
                logger.info(f"  Type: Database attraction (nearby_attraction_id: {nearby.nearby_attraction_id})")
                
                # Get the attraction data
                attraction = (
                    session.query(models.Attraction)
                    .filter(models.Attraction.id == nearby.nearby_attraction_id)
                    .first()
                )
                
                if attraction:
                    # Fill in missing rating
                    if nearby.rating is None and attraction.rating is not None:
                        nearby.rating = attraction.rating
                        needs_update = True
                        logger.info(f"  Updated rating from DB: {attraction.rating}")
                    
                    # Fill in missing user_ratings_total
                    if nearby.user_ratings_total is None and attraction.review_count is not None:
                        nearby.user_ratings_total = attraction.review_count
                        needs_update = True
                        logger.info(f"  Updated user_ratings_total from DB: {attraction.review_count}")
                    
                    # Fill in missing review_count
                    if nearby.review_count is None and attraction.review_count is not None:
                        nearby.review_count = attraction.review_count
                        needs_update = True
                        logger.info(f"  Updated review_count from DB: {attraction.review_count}")
                    
                    # Fill in missing image_url from hero_images
                    if nearby.image_url is None:
                        hero_image = (
                            session.query(models.HeroImage)
                            .filter(models.HeroImage.attraction_id == nearby.nearby_attraction_id)
                            .order_by(models.HeroImage.position.asc())
                            .first()
                        )
                        
                        if hero_image:
                            nearby.image_url = hero_image.url
                            needs_update = True
                            logger.info(f"  Updated image_url from DB")
                        else:
                            logger.warning(f"  No hero image found for attraction {nearby.nearby_attraction_id}")
                else:
                    logger.warning(f"  Attraction not found for nearby_attraction_id: {nearby.nearby_attraction_id}")
                
                if needs_update:
                    updated_count += 1
                    db_count += 1
                    # Mark object as modified for SQLAlchemy
                    session.add(nearby)
            
            # Case 2: Nearby attraction is from Google Places
            else:
                logger.info(f"  Type: Google Places attraction (place_id: {nearby.place_id})")
                
                if not nearby.place_id:
                    logger.warning(f"  No place_id, skipping enrichment")
                else:
                    # Fetch from Google Places API
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        logger.info(f"  Calling enrich_from_google_places...")
                        result = loop.run_until_complete(enrich_from_google_places(nearby, places_client))
                        logger.info(f"  Enrichment result: {result}")
                        
                        if result:
                            needs_update = True
                            updated_count += 1
                            google_count += 1
                            # Mark object as modified for SQLAlchemy
                            session.add(nearby)
                            logger.info(f"  Marked for update")
                    except Exception as e:
                        logger.error(f"  Error during enrichment: {e}", exc_info=True)
                    finally:
                        loop.close()
        
        # Commit all changes
        logger.info(f"\nAttempting to commit changes...")
        logger.info(f"Updated count: {updated_count}, DB count: {db_count}, Google count: {google_count}")
        
        if updated_count > 0:
            try:
                session.commit()
                logger.info(f"\n✓ Successfully updated {updated_count} nearby attractions")
                logger.info(f"  - From database: {db_count}")
                logger.info(f"  - From Google Places: {google_count}")
            except Exception as commit_error:
                logger.error(f"Error during commit: {commit_error}", exc_info=True)
                session.rollback()
        else:
            logger.info("\nNo updates needed")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    logger.info("Starting nearby attractions data population...")
    populate_nearby_attractions_data()
    logger.info("Done!")
