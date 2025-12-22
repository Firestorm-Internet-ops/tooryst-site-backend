"""YouTube retry tasks - automatically fetch videos for attractions with less than MIN_VIDEO_COUNT_THRESHOLD videos."""
import asyncio
import logging
from typing import Dict, Any
from app.celery_app import celery_app
from app.config import settings

logger = logging.getLogger(__name__)


async def fetch_missing_youtube_videos_async() -> Dict[str, Any]:
    """Fetch YouTube videos for attractions with < MIN_VIDEO_COUNT_THRESHOLD videos."""
    from app.infrastructure.persistence.db import SessionLocal
    from app.infrastructure.persistence import models
    from app.infrastructure.external_apis.social_videos_fetcher import SocialVideosFetcherImpl
    from app.infrastructure.persistence.storage_functions import store_social_videos
    from app.core.quota_manager import quota_manager
    from sqlalchemy import func, text

    logger.info("=" * 80)
    logger.info("YOUTUBE RETRY: Fetching videos for attractions with < 3 videos")
    logger.info("=" * 80)

    session = SessionLocal()
    all_attractions = []

    try:
        # Get all attractions with < MIN_VIDEO_COUNT_THRESHOLD videos
        min_video_threshold = settings.MIN_VIDEO_COUNT_THRESHOLD
        
        logger.info(f"Threshold: {min_video_threshold} videos")
        logger.info(f"Scanning for attractions with < {min_video_threshold} videos...")
        
        # Get all attractions with < MIN_VIDEO_COUNT_THRESHOLD videos
        attractions_needing_videos = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(models.SocialVideo, models.Attraction.id == models.SocialVideo.attraction_id)
            .group_by(models.Attraction.id)
            .having(func.count(models.SocialVideo.id) < min_video_threshold)
            .order_by(models.Attraction.id.asc())
            .all()
        )
        
        logger.info(f"Found {len(attractions_needing_videos)} attractions with < {min_video_threshold} videos")
        
        # Get attraction IDs already in youtube_retry_queue
        in_queue_ids = set()
        queue_result = session.execute(text("""
            SELECT DISTINCT attraction_id FROM youtube_retry_queue
        """)).fetchall()
        
        for row in queue_result:
            in_queue_ids.add(row[0])
        
        logger.info(f"Found {len(in_queue_ids)} attractions already in retry queue")
        
        # Filter: only process attractions NOT in queue
        for attraction, city in attractions_needing_videos:
            if attraction.id not in in_queue_ids:
                all_attractions.append({
                    'attraction_id': attraction.id,
                    'attraction_name': attraction.name,
                    'city_name': city.name,
                    'country': city.country
                })

        if not all_attractions:
            logger.info("✓ All attractions with < 3 videos are already in retry queue!")
            return {
                'status': 'success',
                'message': 'No new attractions to process',
                'processed': 0
            }

        logger.info(f"Processing {len(all_attractions)} attractions not yet in queue")

    finally:
        session.close()
    
    # Create fetcher
    fetcher = SocialVideosFetcherImpl()
    
    # Statistics
    stats = {
        'total': len(all_attractions),
        'success': 0,
        'failed': 0,
        'quota_exceeded': False,
        'quota_exceeded_at': None,
        'from_queue_success': 0,
        'from_queue_failed': 0
    }

    # Process each attraction
    for idx, attr_data in enumerate(all_attractions, 1):
        logger.info(f"[{idx}/{stats['total']}] Processing: {attr_data['attraction_name']}")

        try:
            # Fetch videos
            result = await fetcher.fetch(
                attraction_id=attr_data['attraction_id'],
                attraction_name=attr_data['attraction_name'],
                city_name=attr_data['city_name'],
                country=attr_data['country']
            )

            if result and result.get('videos'):
                # Store videos in social_videos table
                if store_social_videos(attr_data['attraction_id'], result['videos']):
                    stats['success'] += 1
                    logger.info(f"  ✓ Stored {len(result['videos'])} videos")
                else:
                    stats['failed'] += 1
                    logger.error(f"  ❌ Failed to store videos")
            else:
                stats['failed'] += 1
                logger.warning(f"  ⚠ No videos found")

        except Exception as e:
            error_msg = str(e)

            # Check for quota exceeded
            if "quota" in error_msg.lower() or "403" in error_msg or fetcher.is_quota_exceeded():
                logger.warning(f"  🚫 YouTube quota exceeded!")
                quota_manager.mark_quota_exceeded('youtube')
                stats['quota_exceeded'] = True
                stats['quota_exceeded_at'] = {
                    'attraction': attr_data['attraction_name'],
                    'position': f"{idx}/{stats['total']}"
                }
                break  # Stop processing
            else:
                stats['failed'] += 1
                logger.error(f"  ❌ Error: {e}")

        # Add to youtube_retry_queue (so we don't process again)
        try:
            add_session = SessionLocal()
            add_session.execute(text("""
                INSERT INTO youtube_retry_queue (attraction_id, status, added_at)
                VALUES (:attraction_id, 'pending', CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    last_retry_at = CURRENT_TIMESTAMP
            """), {'attraction_id': attr_data['attraction_id']})
            add_session.commit()
            add_session.close()
        except Exception as queue_error:
            logger.warning(f"  ⚠ Failed to add to queue: {queue_error}")
        
        # Small delay between requests
        retry_delay = settings.YOUTUBE_RETRY_DELAY_SECONDS
        await asyncio.sleep(retry_delay)
    
    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("YOUTUBE RETRY COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total Processed: {stats['total']}")
    logger.info(f"✅ Success: {stats['success']}")
    logger.info(f"❌ Failed: {stats['failed']}")

    if stats['quota_exceeded']:
        logger.info("")
        logger.info(f"⚠️  Quota exceeded at: {stats['quota_exceeded_at']['attraction']}")
        logger.info(f"   Position: {stats['quota_exceeded_at']['position']}")
        logger.info(f"   Will resume tomorrow at 8 AM UTC")

    logger.info("=" * 80)

    return stats


@celery_app.task(name="app.tasks.youtube_retry_tasks.fetch_missing_youtube_videos")
def fetch_missing_youtube_videos():
    """
    Celery task to fetch YouTube videos for attractions with less than MIN_VIDEO_COUNT_THRESHOLD videos.
    
    Scheduled to run daily at 8 AM UTC (midnight PT).
    Stops gracefully when quota is exceeded and resumes next day.
    
    Configuration:
    - MIN_VIDEO_COUNT_THRESHOLD: Minimum videos per attraction (default: 3)
    - YOUTUBE_RETRY_DELAY_SECONDS: Delay between requests (default: 1)
    """
    logger.info("Starting YouTube retry task")
    
    try:
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(fetch_missing_youtube_videos_async())
        loop.close()
        
        return result
        
    except Exception as e:
        logger.error(f"YouTube retry task failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'error': str(e)
        }
