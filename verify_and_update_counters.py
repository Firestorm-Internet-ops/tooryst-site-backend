"""Verify and update data tracking counters for all attractions.

This script:
1. Lists all attractions
2. Checks if each has a counter in attraction_data_tracking
3. If missing, counts all related data and creates/updates the counter
"""
import sys
from datetime import datetime
from sqlalchemy import text

from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models


def count_attraction_data(session, attraction_id: int) -> dict:
    """Count all data for an attraction across all tables.
    
    Args:
        session: Database session
        attraction_id: ID of the attraction
        
    Returns:
        Dict with counts for each data type
    """
    counts = {
        'hero_images': 0,
        'reviews': 0,
        'tips': 0,
        'social_videos': 0,
        'nearby_attractions': 0,
        'audience_profiles': 0
    }
    
    try:
        # Count hero images
        counts['hero_images'] = session.query(models.HeroImage).filter_by(
            attraction_id=attraction_id
        ).count()
        
        # Count reviews
        counts['reviews'] = session.query(models.Review).filter_by(
            attraction_id=attraction_id
        ).count()
        
        # Count tips
        counts['tips'] = session.query(models.Tip).filter_by(
            attraction_id=attraction_id
        ).count()
        
        # Count social videos
        counts['social_videos'] = session.query(models.SocialVideo).filter_by(
            attraction_id=attraction_id
        ).count()
        
        # Count nearby attractions
        counts['nearby_attractions'] = session.query(models.NearbyAttraction).filter_by(
            attraction_id=attraction_id
        ).count()
        
        # Count audience profiles
        counts['audience_profiles'] = session.query(models.AudienceProfile).filter_by(
            attraction_id=attraction_id
        ).count()
    except Exception as e:
        print(f"  ✗ Error counting data: {e}")
    
    return counts


def main():
    """Main function to verify and update all counters."""
    session = SessionLocal()
    
    try:
        print("="*80)
        print("ATTRACTION DATA TRACKING VERIFICATION")
        print("="*80)
        print()
        
        # Get all attractions
        attractions = session.query(models.Attraction).order_by(models.Attraction.name).all()
        print(f"Found {len(attractions)} attractions")
        print()
        
        # Track statistics
        total_attractions = len(attractions)
        with_counters = 0
        without_counters = 0
        updated_counters = 0
        
        # Process each attraction
        for idx, attraction in enumerate(attractions, 1):
            print(f"[{idx}/{total_attractions}] {attraction.name} (ID: {attraction.id})")
            
            # Check if counter exists using raw SQL
            existing_counter = session.execute(text("""
                SELECT id, hero_images_count, reviews_count, tips_count, 
                       social_videos_count, nearby_attractions_count, audience_profiles_count
                FROM attraction_data_tracking
                WHERE attraction_id = :attraction_id
                LIMIT 1
            """), {'attraction_id': attraction.id}).fetchone()
            
            if existing_counter:
                print(f"  ✓ Counter exists")
                print(f"    - Hero Images: {existing_counter[1]}")
                print(f"    - Reviews: {existing_counter[2]}")
                print(f"    - Tips: {existing_counter[3]}")
                print(f"    - Social Videos: {existing_counter[4]}")
                print(f"    - Nearby Attractions: {existing_counter[5]}")
                print(f"    - Audience Profiles: {existing_counter[6]}")
                with_counters += 1
            else:
                print(f"  ⚠ No counter found, counting data from tables...")
                
                # Count all data
                counts = count_attraction_data(session, attraction.id)
                total_data = sum(counts.values())
                
                print(f"    - Hero Images: {counts['hero_images']}")
                print(f"    - Reviews: {counts['reviews']}")
                print(f"    - Tips: {counts['tips']}")
                print(f"    - Social Videos: {counts['social_videos']}")
                print(f"    - Nearby Attractions: {counts['nearby_attractions']}")
                print(f"    - Audience Profiles: {counts['audience_profiles']}")
                print(f"    - Total Data Points: {total_data}")
                
                # Create counter record
                try:
                    # Use raw SQL to insert with all counts
                    # Use pipeline_run_id = 1 (first completed pipeline) as reference
                    session.execute(text("""
                        INSERT INTO attraction_data_tracking 
                        (attraction_id, pipeline_run_id, hero_images_count, reviews_count, 
                         tips_count, social_videos_count, nearby_attractions_count, 
                         audience_profiles_count, created_at, updated_at)
                        VALUES (:attraction_id, 1, :hero_images, :reviews, :tips, 
                                :social_videos, :nearby_attractions, :audience_profiles,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON DUPLICATE KEY UPDATE
                            hero_images_count = :hero_images,
                            reviews_count = :reviews,
                            tips_count = :tips,
                            social_videos_count = :social_videos,
                            nearby_attractions_count = :nearby_attractions,
                            audience_profiles_count = :audience_profiles,
                            updated_at = CURRENT_TIMESTAMP
                    """), {
                        'attraction_id': attraction.id,
                        'hero_images': counts['hero_images'],
                        'reviews': counts['reviews'],
                        'tips': counts['tips'],
                        'social_videos': counts['social_videos'],
                        'nearby_attractions': counts['nearby_attractions'],
                        'audience_profiles': counts['audience_profiles']
                    })
                    session.commit()
                    print(f"  ✓ Counter created/updated successfully")
                    updated_counters += 1
                    without_counters += 1
                except Exception as e:
                    print(f"  ✗ Error creating counter: {e}")
                    session.rollback()
            
            print()
        
        # Print summary
        print("="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total Attractions: {total_attractions}")
        print(f"With Counters: {with_counters}")
        print(f"Without Counters (Created): {without_counters}")
        print(f"Updated: {updated_counters}")
        print()
        
        if without_counters == 0:
            print("✅ All attractions have data tracking counters!")
        else:
            print(f"✅ Created/updated {updated_counters} counters")
        
        print("="*80)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        session.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
