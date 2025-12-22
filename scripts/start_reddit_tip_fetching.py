"""Script to start Reddit tip fetching for attractions."""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import text
from app.core.database import get_db
from app.tasks.reddit_tip_fetcher_task import fetch_reddit_tips_batch

def start_tip_fetching_for_all_attractions():
    """Start tip fetching for all attractions that don't have tips yet."""
    db = next(get_db())
    
    try:
        # Find attractions without tips or with incomplete tip runs
        attractions = db.execute(
            text("""
                SELECT a.id, a.name, a.city,
                       COUNT(t.id) as tip_count,
                       r.status as run_status
                FROM attraction a
                LEFT JOIN tip t ON a.id = t.attraction_id
                LEFT JOIN data_fetch_runs r ON a.id = r.attraction_id AND r.data_type = 'tips'
                WHERE a.name IS NOT NULL
                GROUP BY a.id, a.name, a.city, r.status
                HAVING COUNT(t.id) < 50 
                   OR r.status IS NULL
                   OR r.status IN ('FAILED', 'PENDING')
                ORDER BY COUNT(t.id) ASC
                LIMIT 100
            """)
        ).fetchall()
        
        print(f"Found {len(attractions)} attractions to fetch tips for")
        
        for attraction in attractions:
            print(f"Starting tip fetching for: {attraction.name} (ID: {attraction.id})")
            print(f"  Current tips: {attraction.tip_count}")
            print(f"  Run status: {attraction.run_status or 'None'}")
            
            # Trigger the Celery task
            fetch_reddit_tips_batch.delay(attraction.id)
            print(f"  ✓ Task queued\n")
        
        print(f"\nSuccessfully queued {len(attractions)} tip fetching tasks")
        print("Monitor progress with: celery -A app.core.celery_app flower")
    
    finally:
        db.close()


def start_tip_fetching_for_attraction(attraction_id: int):
    """Start tip fetching for a specific attraction."""
    db = next(get_db())
    
    try:
        attraction = db.execute(
            text("SELECT id, name FROM attraction WHERE id = :id"),
            {"id": attraction_id}
        ).fetchone()
        
        if not attraction:
            print(f"Attraction {attraction_id} not found")
            return
        
        print(f"Starting tip fetching for: {attraction.name} (ID: {attraction.id})")
        fetch_reddit_tips_batch.delay(attraction.id)
        print("✓ Task queued")
    
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Start Reddit tip fetching")
    parser.add_argument(
        "--attraction-id",
        type=int,
        help="Fetch tips for a specific attraction ID"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch tips for all attractions"
    )
    
    args = parser.parse_args()
    
    if args.attraction_id:
        start_tip_fetching_for_attraction(args.attraction_id)
    elif args.all:
        start_tip_fetching_for_all_attractions()
    else:
        print("Usage:")
        print("  python scripts/start_reddit_tip_fetching.py --all")
        print("  python scripts/start_reddit_tip_fetching.py --attraction-id 123")
