#!/usr/bin/env python3
"""
One-time migration script to process all existing hero images to GCS.

This script:
1. Fetches all attractions with place_id
2. For each attraction, downloads photos from Google Places API
3. Converts to WebP format (1600px for hero slider, 400px card for first image only)
4. Uploads to GCS bucket
5. Updates database with GCS URLs

Usage:
    python scripts/migrate_hero_images_to_gcs.py [--batch-size 35] [--start-from 0] [--dry-run]

Options:
    --batch-size: Number of attractions to process (default: all)
    --start-from: Start from this attraction index (default: 0)
    --dry-run: Don't actually upload or update database
"""
import asyncio
import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Add the backend directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.tasks.hero_images_refresh_tasks import process_card_image
from app.infrastructure.external_apis.google_places_client import GooglePlacesClient

# Ensure logs directory exists
log_dir = Path(__file__).parent.parent / 'logs'
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


def get_all_attractions():
    """Get all attractions with place_id."""
    session = SessionLocal()
    try:
        # Subquery to find attractions with GCS images
        processed_subquery = (
            session.query(models.HeroImage.attraction_id)
            .filter(models.HeroImage.position == 1)
            .filter(models.HeroImage.gcs_url_hero.isnot(None))
            .filter(models.HeroImage.gcs_url_hero != "")
        )

        attractions = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .filter(models.Attraction.place_id.isnot(None))
            .filter(models.Attraction.place_id != "")
            .filter(models.Attraction.id.notin_(processed_subquery))
            .order_by(models.Attraction.id)
            .all()
        )

        result = []
        for attraction, city in attractions:
            result.append({
                'id': attraction.id,
                'place_id': attraction.place_id,
                'name': attraction.name,
                'city_name': city.name,
                'slug': attraction.slug
            })

        return result
    finally:
        session.close()


async def migrate_single_attraction(attraction: dict, dry_run: bool = False) -> dict:
    """Migrate a single attraction's hero images to GCS."""
    if dry_run:
        logger.info(f"[DRY RUN] Would process: {attraction['name']}")
        return {"status": "dry_run", "attraction": attraction['name'], "id": attraction['id']}

    try:
        result = await process_card_image(
            attraction_id=attraction['id'],
            place_id=attraction['place_id'],
            attraction_name=attraction['name']
        )

        # Handle invalid place_id by fetching a new one
        if result.get('status') == 'invalid_place_id':
            logger.info(f"Attempting to fetch new place_id for {attraction['name']}")

            client = GooglePlacesClient()
            search_query = f"{attraction['name']} {attraction['city_name']}"
            new_place = await client.find_place(search_query)

            if new_place and new_place.get('place_id'):
                new_place_id = new_place['place_id']
                logger.info(f"Found new place_id: {new_place_id}")

                # Update database
                session = SessionLocal()
                try:
                    db_attraction = session.query(models.Attraction).filter_by(id=attraction['id']).first()
                    if db_attraction:
                        db_attraction.place_id = new_place_id
                        session.commit()
                        logger.info(f"Updated place_id in database for {attraction['name']}")
                finally:
                    session.close()

                # Retry with new place_id
                result = await process_card_image(
                    attraction_id=attraction['id'],
                    place_id=new_place_id,
                    attraction_name=attraction['name']
                )
            else:
                logger.error(f"Could not find new place_id for {attraction['name']}")

        return {
            "attraction": attraction['name'],
            "slug": attraction['slug'],
            "id": attraction['id'],
            **result
        }
    except Exception as e:
        logger.error(f"Error processing {attraction['name']}: {e}")
        return {
            "attraction": attraction['name'],
            "slug": attraction['slug'],
            "status": "error",
            "error": str(e),
            "id": attraction['id']
        }


async def run_migration(batch_size: int = None, start_from: int = 0, dry_run: bool = False):
    """Run the migration for all attractions."""
    logger.info("=" * 70)
    logger.info("Starting Hero Images GCS Migration")
    logger.info("=" * 70)

    # Get all attractions
    attractions = get_all_attractions()
    total = len(attractions)
    logger.info(f"Found {total} attractions with place_id")

    # Apply start_from and batch_size
    if start_from > 0:
        attractions = attractions[start_from:]
        logger.info(f"Starting from index {start_from}")

    if batch_size:
        attractions = attractions[:batch_size]
        logger.info(f"Processing batch of {len(attractions)} attractions")

    # Process attractions
    success_count = 0
    error_count = 0
    no_photos_count = 0
    results = []

    for idx, attraction in enumerate(attractions):
        global_idx = start_from + idx
        logger.info(f"[{global_idx + 1}/{total}] Processing: {attraction['name']} ({attraction['city_name']})")

        result = await migrate_single_attraction(attraction, dry_run)
        results.append(result)

        if result.get('status') == 'success':
            success_count += 1
            logger.info(f"  ✓ Success: {result.get('count', 0)} images processed")
        elif result.get('status') == 'no_photos':
            no_photos_count += 1
            logger.warning(f"  ⚠ No photos found")
        elif result.get('status') == 'dry_run':
            pass
        else:
            error_count += 1
            logger.error(f"  ✗ Error: {result.get('error', 'Unknown error')}")

        # Add delay between attractions to respect rate limits
        if not dry_run and idx < len(attractions) - 1:
            await asyncio.sleep(1)

    # Summary
    logger.info("=" * 70)
    logger.info("Migration Complete")
    logger.info("=" * 70)
    logger.info(f"Total processed: {len(attractions)}")
    logger.info(f"Success: {success_count}")
    logger.info(f"No photos: {no_photos_count}")
    logger.info(f"Errors: {error_count}")

    if error_count > 0:
        logger.info("\nFailed attractions:")
        for r in results:
            if r.get('status') == 'error':
                logger.info(f"  - {r['attraction']}: {r.get('error', 'Unknown')}")
            elif r.get('status') == 'invalid_place_id':
                logger.info(f"  - {r['attraction']}: Invalid place_id (could not find replacement)")
    
    # Log successful additions (or dry run processed)
    logger.info("\nAdded images for processing:")
    for r in results:
        if r.get('status') == 'success' or (dry_run and r.get('status') == 'dry_run'):
             logger.info(f"Added images for attraction: ID={r['id']}, Name={r['attraction']}")

    return {
        "total": len(attractions),
        "success": success_count,
        "no_photos": no_photos_count,
        "errors": error_count,
        "results": results
    }


def main():
    parser = argparse.ArgumentParser(description='Migrate hero images to GCS')
    parser.add_argument('--batch-size', type=int, default=None,
                        help='Number of attractions to process (default: all)')
    parser.add_argument('--start-from', type=int, default=0,
                        help='Start from this attraction index (default: 0)')
    parser.add_argument('--dry-run', action='store_true',
                        help="Don't actually upload or update database")

    args = parser.parse_args()

    # Run migration
    result = asyncio.run(run_migration(
        batch_size=args.batch_size,
        start_from=args.start_from,
        dry_run=args.dry_run
    ))

    # Exit with error code if there were errors
    if result['errors'] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
