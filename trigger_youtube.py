#!/usr/bin/env python3
"""Manually trigger YouTube video fetcher for attractions with < 3 videos."""
import os
import sys
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Add backend to path
sys.path.insert(0, os.path.dirname(__file__))

from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.infrastructure.external_apis.social_videos_fetcher import SocialVideosFetcherImpl
from app.infrastructure.persistence.storage_functions import store_social_videos
from app.core.quota_manager import quota_manager
from sqlalchemy import func, text
import asyncio

async def fetch_youtube_videos():
    """Fetch YouTube videos for attractions with < 3 videos."""
    
    print("🎬 YouTube Video Fetcher")
    print("=" * 80)
    
    session = SessionLocal()
    
    try:
        # Reset YouTube quota flag before starting
        print("🔄 Resetting YouTube quota flag...")
        quota_manager.reset_quota('youtube')
        print("✅ YouTube quota flag reset to FALSE")
        print("")
        
        # Get all attractions with < 3 videos
        print(f"🔍 Scanning for attractions with < 3 videos...")
        
        # Get all attractions with < 3 videos
        attractions_needing_videos = (
            session.query(models.Attraction, models.City)
            .join(models.City, models.Attraction.city_id == models.City.id)
            .outerjoin(models.SocialVideo, models.Attraction.id == models.SocialVideo.attraction_id)
            .group_by(models.Attraction.id)
            .having(func.count(models.SocialVideo.id) < 3)  # Less than 3 videos
            .order_by(models.Attraction.id.asc())
            .all()
        )
        
        # Get attraction IDs already in youtube_retry_queue
        in_queue_ids = set()
        queue_result = session.execute(text("""
            SELECT DISTINCT attraction_id FROM youtube_retry_queue
        """)).fetchall()
        
        for row in queue_result:
            in_queue_ids.add(row[0])
        
        print(f"Found {len(attractions_needing_videos)} attractions with < 3 videos")
        print(f"Found {len(in_queue_ids)} attractions already in retry queue")
        
        # Filter: only process attractions NOT in queue
        attractions_to_process = []
        for attraction, city in attractions_needing_videos:
            if attraction.id not in in_queue_ids:
                attractions_to_process.append((attraction, city))
        
        if not attractions_to_process:
            print("✅ All attractions with < 3 videos are already in retry queue!")
            print("=" * 80)
            return True
        
        print(f"🎯 Processing {len(attractions_to_process)} attractions not yet in queue")
        print("")
        print("Processing attractions:")
        print("-" * 80)
        
        # Create fetcher
        fetcher = SocialVideosFetcherImpl()
        
        # Statistics
        stats = {
            'total': len(attractions_to_process),
            'success': 0,
            'failed': 0,
            'quota_exceeded': False,
            'quota_exceeded_at': None
        }
        
        # Process each attraction
        for idx, (attraction, city) in enumerate(attractions_to_process, 1):
            current_videos = session.query(models.SocialVideo).filter_by(
                attraction_id=attraction.id
            ).count()
            
            print(f"[{idx}/{stats['total']}] {attraction.name} ({current_videos} videos)")
            
            try:
                # Fetch videos
                result = await fetcher.fetch(
                    attraction_id=attraction.id,
                    attraction_name=attraction.name,
                    city_name=city.name,
                    country=city.country
                )
                
                if result and result.get('videos'):
                    # Store videos
                    if store_social_videos(attraction.id, result['videos']):
                        stats['success'] += 1
                        print(f"  ✅ Stored {len(result['videos'])} videos")
                    else:
                        stats['failed'] += 1
                        print(f"  ❌ Failed to store videos")
                else:
                    stats['failed'] += 1
                    print(f"  ⚠️  No videos found")
            
            except Exception as e:
                error_msg = str(e)
                
                # Check for quota exceeded
                if "quota" in error_msg.lower() or "403" in error_msg or fetcher.is_quota_exceeded():
                    print(f"  🚫 YouTube quota exceeded!")
                    quota_manager.mark_quota_exceeded('youtube')
                    stats['quota_exceeded'] = True
                    stats['quota_exceeded_at'] = {
                        'attraction': attraction.name,
                        'position': f"{idx}/{stats['total']}"
                    }
                    break  # Stop processing
                else:
                    stats['failed'] += 1
                    print(f"  ❌ Error: {e}")
            
            # Add to youtube_retry_queue (so we don't process again)
            try:
                add_session = SessionLocal()
                add_session.execute(text("""
                    INSERT INTO youtube_retry_queue (attraction_id, status, added_at)
                    VALUES (:attraction_id, 'pending', CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                        last_retry_at = CURRENT_TIMESTAMP
                """), {'attraction_id': attraction.id})
                add_session.commit()
                add_session.close()
            except Exception as queue_error:
                print(f"  ⚠️  Failed to add to queue: {queue_error}")
            
            # Small delay between requests
            await asyncio.sleep(1)
        
        # Summary
        print("-" * 80)
        print("")
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total processed: {stats['total']}")
        print(f"✅ Success: {stats['success']}")
        print(f"❌ Failed: {stats['failed']}")
        
        if stats['quota_exceeded']:
            print(f"\n⚠️  Quota exceeded at: {stats['quota_exceeded_at']['attraction']}")
            print(f"   Position: {stats['quota_exceeded_at']['position']}")
            print(f"   Will resume tomorrow at 8 AM UTC (13:30 IST)")
        else:
            print(f"\n✅ All {stats['total']} attractions processed!")
        
        print("=" * 80)
        
        return not stats['quota_exceeded']
    
    finally:
        session.close()

if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        success = loop.run_until_complete(fetch_youtube_videos())
        loop.close()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
