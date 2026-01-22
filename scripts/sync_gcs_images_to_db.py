#!/usr/bin/env python3
"""
Sync existing GCS images to database.

Scans the GCS bucket for existing attraction images and updates
the hero_images table with the correct GCS URLs.

This script fixes the case where images exist in GCS but the database
has NULL values for gcs_url_card and gcs_url_hero columns.

It also handles cases where only hero_1.webp exists (no card.webp) by
creating a resized card image from the hero image.

Usage:
    python scripts/sync_gcs_images_to_db.py [--dry-run]

Options:
    --dry-run: Show what would be updated without making changes
"""
import argparse
import logging
import sys
import os
from datetime import datetime
from io import BytesIO
from pathlib import Path

# Add the backend directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from google.cloud import storage
from PIL import Image
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.config import settings

# Ensure logs directory exists
log_dir = Path(__file__).parent.parent / 'logs'
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / f'sync_gcs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

GCS_BUCKET = "images.tooryst.co"
GCS_BASE_URL = f"https://storage.googleapis.com/{GCS_BUCKET}"

# Image settings
IMAGE_SIZE_CARD = getattr(settings, 'IMAGE_SIZE_CARD', 400)
IMAGE_QUALITY_WEBP = getattr(settings, 'IMAGE_QUALITY_WEBP', 85)


def get_gcs_client_and_bucket():
    """Get GCS client and bucket."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    return client, bucket


def get_existing_gcs_images():
    """Scan GCS bucket for existing attraction images.

    Returns:
        Dictionary: attraction_id -> {card: url, hero: url, hero_1: url}
    """
    client, bucket = get_gcs_client_and_bucket()

    # Dictionary: attraction_id -> {card: url, hero: url, hero_1: url}
    images = {}

    # List all blobs in attractions/ prefix
    logger.info(f"Scanning GCS bucket: {GCS_BUCKET}/attractions/")
    blobs = bucket.list_blobs(prefix="attractions/")

    for blob in blobs:
        # Parse path: attractions/{id}/card.webp, attractions/{id}/hero.webp, or attractions/{id}/hero_1.webp
        parts = blob.name.split("/")
        if len(parts) == 3 and parts[2] in ("card.webp", "hero.webp", "hero_1.webp"):
            try:
                attraction_id = int(parts[1])
                filename = parts[2]

                # Map filename to type
                if filename == "card.webp":
                    image_type = "card"
                elif filename == "hero.webp":
                    image_type = "hero"
                elif filename == "hero_1.webp":
                    image_type = "hero_1"
                else:
                    continue

                if attraction_id not in images:
                    images[attraction_id] = {}

                images[attraction_id][image_type] = f"{GCS_BASE_URL}/{blob.name}"
            except ValueError:
                # Skip if attraction_id is not a valid integer
                logger.warning(f"Skipping invalid path: {blob.name}")
                continue

    return images


def resize_image_to_card(image_bytes: bytes) -> bytes:
    """Resize image to card dimensions (400px width).

    Args:
        image_bytes: Raw image bytes

    Returns:
        Resized image as WebP bytes
    """
    img = Image.open(BytesIO(image_bytes))

    # Convert to RGB if necessary
    if img.mode in ('RGBA', 'P', 'LA'):
        background = Image.new('RGB', img.size, (255, 255, 255))
        if img.mode == 'P':
            img = img.convert('RGBA')
        background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
        img = background
    elif img.mode != 'RGB':
        img = img.convert('RGB')

    # Calculate new height maintaining aspect ratio
    original_width, original_height = img.size

    if original_width > IMAGE_SIZE_CARD:
        ratio = IMAGE_SIZE_CARD / original_width
        target_height = int(original_height * ratio)
        img = img.resize((IMAGE_SIZE_CARD, target_height), Image.Resampling.LANCZOS)

    # Convert to WebP
    output = BytesIO()
    img.save(output, format='WEBP', quality=IMAGE_QUALITY_WEBP, method=6)
    return output.getvalue()


def create_card_from_hero(bucket, attraction_id: int, hero_blob_name: str, dry_run: bool = False) -> str:
    """Download hero image and create a card-sized version.

    Args:
        bucket: GCS bucket object
        attraction_id: Attraction ID
        hero_blob_name: Name of the hero blob (e.g., "attractions/123/hero_1.webp")
        dry_run: If True, don't actually upload

    Returns:
        URL of the created card image, or None if failed
    """
    try:
        # Download the hero image
        hero_blob = bucket.blob(hero_blob_name)
        hero_bytes = hero_blob.download_as_bytes()
        logger.info(f"Downloaded {hero_blob_name} ({len(hero_bytes)} bytes)")

        # Resize to card dimensions
        card_bytes = resize_image_to_card(hero_bytes)
        logger.info(f"Resized to card dimensions ({len(card_bytes)} bytes)")

        if dry_run:
            return f"{GCS_BASE_URL}/attractions/{attraction_id}/card.webp"

        # Upload the card image
        card_blob_path = f"attractions/{attraction_id}/card.webp"
        card_blob = bucket.blob(card_blob_path)
        card_blob.upload_from_string(card_bytes, content_type="image/webp")
        card_blob.cache_control = "public, max-age=31536000"
        card_blob.patch()

        card_url = f"{GCS_BASE_URL}/{card_blob_path}"
        logger.info(f"Uploaded card image to {card_url}")
        return card_url

    except Exception as e:
        logger.error(f"Error creating card from hero for attraction {attraction_id}: {e}")
        return None


def sync_to_database(images: dict, dry_run: bool = False):
    """Update HeroImage records with GCS URLs."""
    session = SessionLocal()
    _, bucket = get_gcs_client_and_bucket()

    updated = 0
    created = 0
    skipped = 0
    cards_generated = 0

    try:
        for attraction_id, urls in sorted(images.items()):
            # Find existing HeroImage with position=1
            hero_image = session.query(models.HeroImage).filter(
                models.HeroImage.attraction_id == attraction_id,
                models.HeroImage.position == 1
            ).first()

            card_url = urls.get("card")
            hero_url = urls.get("hero")
            hero_1_url = urls.get("hero_1")

            # If no card.webp but hero_1.webp exists, create card from hero_1
            if not card_url and hero_1_url:
                logger.info(f"Attraction {attraction_id}: No card.webp, creating from hero_1.webp")
                hero_1_blob_name = f"attractions/{attraction_id}/hero_1.webp"
                card_url = create_card_from_hero(bucket, attraction_id, hero_1_blob_name, dry_run)
                if card_url:
                    cards_generated += 1

            # Use hero_1 as hero fallback if no hero.webp
            if not hero_url and hero_1_url:
                hero_url = hero_1_url

            if hero_image:
                # Update existing record if GCS URLs are missing
                needs_update = False

                if card_url and not hero_image.gcs_url_card:
                    if not dry_run:
                        hero_image.gcs_url_card = card_url
                    needs_update = True

                if hero_url and not hero_image.gcs_url_hero:
                    if not dry_run:
                        hero_image.gcs_url_hero = hero_url
                    needs_update = True

                if needs_update:
                    updated += 1
                    prefix = "[DRY RUN] " if dry_run else ""
                    logger.info(f"{prefix}Updated attraction {attraction_id}: card={card_url}, hero={hero_url}")
                else:
                    skipped += 1
                    logger.debug(f"Skipped attraction {attraction_id}: URLs already set")
            else:
                # Check if attraction exists
                attraction = session.query(models.Attraction).filter(
                    models.Attraction.id == attraction_id
                ).first()

                if not attraction:
                    logger.warning(f"Attraction {attraction_id} not found in database, skipping")
                    skipped += 1
                    continue

                # Create new HeroImage record
                if not dry_run:
                    new_image = models.HeroImage(
                        attraction_id=attraction_id,
                        position=1,
                        url=card_url or hero_url or "",  # Use card or hero as fallback URL
                        gcs_url_card=card_url,
                        gcs_url_hero=hero_url,
                    )
                    session.add(new_image)

                created += 1
                prefix = "[DRY RUN] " if dry_run else ""
                logger.info(f"{prefix}Created HeroImage for attraction {attraction_id}")

        if not dry_run:
            session.commit()

        logger.info("=" * 50)
        logger.info("Sync Complete")
        logger.info("=" * 50)
        logger.info(f"Updated: {updated}")
        logger.info(f"Created: {created}")
        logger.info(f"Cards generated from hero_1: {cards_generated}")
        logger.info(f"Skipped (already set or not found): {skipped}")

        return {"updated": updated, "created": created, "cards_generated": cards_generated, "skipped": skipped}

    except Exception as e:
        session.rollback()
        logger.error(f"Error syncing: {e}")
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description='Sync GCS images to database')
    parser.add_argument('--dry-run', action='store_true',
                        help="Show what would be updated without making changes")

    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("GCS Images to Database Sync")
    logger.info("=" * 50)

    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    logger.info("Scanning GCS bucket for existing images...")
    images = get_existing_gcs_images()
    logger.info(f"Found images for {len(images)} attractions")

    if not images:
        logger.info("No images found in GCS bucket")
        return

    logger.info("Syncing to database...")
    sync_to_database(images, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
