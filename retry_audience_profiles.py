"""Retry audience profile fetching for attractions with 0 profiles."""
import sys
import asyncio
import logging
from datetime import datetime
from pathlib import Path

from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.external_apis.audience_fetcher import AudienceFetcherImpl
from sqlalchemy import text
import json

# Setup logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = log_dir / f"retry_audience_profiles_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def retry_audience_profile(attraction_id: int, attraction_name: str, city_name: str, attempt: int = 1) -> bool:
    """Retry fetching audience profile for a single attraction.
    
    Args:
        attraction_id: ID of the attraction
        attraction_name: Name of the attraction
        city_name: City name
        attempt: Current attempt number
    
    Returns:
        True if successful, False otherwise
    """
    session = SessionLocal()
    
    try:
        logger.info(f"[Attempt {attempt}] Fetching audience profiles for {attraction_name} ({attraction_id})")
        
        fetcher = AudienceFetcherImpl()
        result = await fetcher.fetch(
            attraction_id=attraction_id,
            attraction_name=attraction_name,
            city_name=city_name
        )
        
        if result and result.get('profiles'):
            profiles = result['profiles']
            profiles_count = len(profiles)
            
            # Delete existing profiles for this attraction
            session.execute(text("""
                DELETE FROM audience_profiles
                WHERE attraction_id = :attraction_id
            """), {'attraction_id': attraction_id})
            
            # Insert new profiles
            for profile in profiles:
                session.execute(text("""
                    INSERT INTO audience_profiles 
                    (attraction_id, audience_type, description, emoji, created_at)
                    VALUES (:attraction_id, :audience_type, :description, :emoji, CURRENT_TIMESTAMP)
                """), {
                    'attraction_id': attraction_id,
                    'audience_type': profile.get('audience_type', ''),
                    'description': profile.get('description', ''),
                    'emoji': profile.get('emoji', '')
                })
            
            # Update counter in attraction_data_tracking
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET audience_profiles_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE attraction_id = :attraction_id
            """), {
                'attraction_id': attraction_id,
                'count': profiles_count
            })
            
            session.commit()
            logger.info(f"✓ Successfully stored {profiles_count} audience profiles for {attraction_name}")
            return True
        else:
            logger.warning(f"⚠ No profiles returned for {attraction_name}")
            return False
    
    except Exception as e:
        session.rollback()
        logger.error(f"✗ Error fetching profiles for {attraction_name}: {e}")
        return False
    
    finally:
        session.close()


async def retry_all_zero_audience_profiles(max_retries: int = 3) -> dict:
    """Retry fetching audience profiles for all attractions with 0 profiles.
    
    Args:
        max_retries: Maximum number of retry attempts per attraction
    
    Returns:
        Dictionary with retry statistics
    """
    session = SessionLocal()
    
    try:
        logger.info("="*80)
        logger.info("AUDIENCE PROFILE RETRY - START")
        logger.info("="*80)
        
        # Find all attractions with 0 audience profiles
        attractions = session.execute(text("""
            SELECT a.id, a.name, c.name as city_name
            FROM attraction_data_tracking adt
            JOIN attractions a ON adt.attraction_id = a.id
            JOIN cities c ON a.city_id = c.id
            WHERE adt.audience_profiles_count = 0
            ORDER BY a.name
        """)).fetchall()
        
        logger.info(f"Found {len(attractions)} attractions with 0 audience profiles")
        logger.info("="*80)
        
        stats = {
            'total': len(attractions),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'attractions': []
        }
        
        # Retry each attraction
        for idx, (attraction_id, attraction_name, city_name) in enumerate(attractions, 1):
            logger.info(f"\n[{idx}/{len(attractions)}] {attraction_name} (ID: {attraction_id})")
            
            success = False
            for attempt in range(1, max_retries + 1):
                success = await retry_audience_profile(
                    attraction_id=attraction_id,
                    attraction_name=attraction_name,
                    city_name=city_name,
                    attempt=attempt
                )
                
                if success:
                    stats['successful'] += 1
                    stats['attractions'].append({
                        'id': attraction_id,
                        'name': attraction_name,
                        'status': 'success',
                        'attempts': attempt
                    })
                    break
                
                # Wait before retrying
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                    logger.info(f"  Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
            
            if not success:
                stats['failed'] += 1
                stats['attractions'].append({
                    'id': attraction_id,
                    'name': attraction_name,
                    'status': 'failed',
                    'attempts': max_retries
                })
                logger.error(f"✗ Failed after {max_retries} attempts")
        
        logger.info("\n" + "="*80)
        logger.info("RETRY SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Attractions: {stats['total']}")
        logger.info(f"Successful: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Success Rate: {(stats['successful']/stats['total']*100):.1f}%" if stats['total'] > 0 else "N/A")
        logger.info("="*80)
        
        return stats
    
    finally:
        session.close()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Retry audience profile fetching for attractions with 0 profiles')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts per attraction (default: 3)')
    args = parser.parse_args()
    
    logger.info(f"Starting audience profile retry with max {args.max_retries} attempts per attraction")
    
    # Run async function
    stats = asyncio.run(retry_all_zero_audience_profiles(max_retries=args.max_retries))
    
    # Exit with appropriate code
    return 0 if stats['failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
