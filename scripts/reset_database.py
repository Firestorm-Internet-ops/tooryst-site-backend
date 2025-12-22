#!/usr/bin/env python3
"""Reset database to start fresh pipeline from Excel."""
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.infrastructure.persistence.db import SessionLocal, engine
from app.infrastructure.persistence import models
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def reset_database():
    """Delete all attractions and related data to start fresh."""
    session = SessionLocal()
    try:
        logger.info("Resetting database...")
        
        # Delete in order of dependencies
        logger.info("Deleting hero images...")
        session.query(models.HeroImage).delete()
        
        logger.info("Deleting reviews...")
        session.query(models.Review).delete()
        
        logger.info("Deleting tips...")
        session.query(models.Tip).delete()
        
        logger.info("Deleting weather forecasts...")
        session.query(models.WeatherForecast).delete()
        
        logger.info("Deleting best time data...")
        session.query(models.BestTimeData).delete()
        
        logger.info("Deleting nearby attractions...")
        session.query(models.NearbyAttraction).delete()
        
        logger.info("Deleting social videos...")
        session.query(models.SocialVideo).delete()
        
        logger.info("Deleting widget configs...")
        session.query(models.WidgetConfig).delete()
        
        logger.info("Deleting attraction metadata...")
        session.query(models.AttractionMetadata).delete()
        
        logger.info("Deleting map snapshots...")
        session.query(models.MapSnapshot).delete()
        
        logger.info("Deleting audience profiles...")
        session.query(models.AudienceProfile).delete()
        
        logger.info("Deleting attractions...")
        session.query(models.Attraction).delete()
        
        logger.info("Deleting cities...")
        session.query(models.City).delete()
        
        session.commit()
        logger.info("✓ Database reset complete - all attractions and data deleted")
        logger.info("✓ Ready to restart pipeline from Excel")
        
    except Exception as e:
        session.rollback()
        logger.error(f"✗ Failed to reset database: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    confirm = input("⚠️  This will DELETE all attractions and data from the database. Continue? (yes/no): ")
    if confirm.lower() == "yes":
        reset_database()
    else:
        logger.info("Reset cancelled")
