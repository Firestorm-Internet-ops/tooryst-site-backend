"""Celery task for fetching Reddit tips with rate limit handling."""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from celery import Task
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.celery_app import celery_app
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.external_apis.reddit_client import RedditClient
from app.infrastructure.external_apis.gemini_client import GeminiClient

logger = logging.getLogger(__name__)

# Batch size for each run
BATCH_SIZE = 50
# Delay when rate limited (15 minutes)
RATE_LIMIT_DELAY_MINUTES = 15
# Delay between successful batches (30 seconds to be safe)
BATCH_DELAY_SECONDS = 30


class RedditTipFetcherTask(Task):
    """Custom task class with shared Reddit client."""
    
    _reddit_client: Optional[RedditClient] = None
    _gemini_client: Optional[GeminiClient] = None
    
    @property
    def reddit_client(self) -> RedditClient:
        if self._reddit_client is None:
            self._reddit_client = RedditClient()
        return self._reddit_client
    
    @property
    def gemini_client(self) -> GeminiClient:
        if self._gemini_client is None:
            self._gemini_client = GeminiClient()
        return self._gemini_client


async def _fetch_and_process_tips(
    reddit_client: RedditClient,
    gemini_client: GeminiClient,
    attraction_name: str
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Async wrapper to fetch and process tips."""
    # Fetch Reddit posts
    posts = await reddit_client.get_attraction_specific_posts(
        attraction_name=attraction_name,
        limit=BATCH_SIZE
    )

    if not posts:
        return [], None

    # Process posts into tips
    tips_data = await _process_posts_to_tips(
        gemini_client,
        attraction_name,
        posts
    )

    return posts, tips_data


@celery_app.task(
    bind=True,
    base=RedditTipFetcherTask,
    max_retries=5,
    default_retry_delay=60
)
def fetch_reddit_tips_batch(self, attraction_id: int, run_id: Optional[int] = None):
    """
    Fetch a batch of Reddit tips for an attraction.

    This task:
    1. Fetches tips in small batches (BATCH_SIZE)
    2. Saves progress to database
    3. Re-enqueues itself if more tips needed
    4. Handles rate limits gracefully with delays
    """
    db: Session = SessionLocal()

    try:
        # Get or create run record
        if run_id:
            run = db.execute(
                text("SELECT * FROM data_fetch_runs WHERE id = :run_id AND data_type = 'tips'"),
                {"run_id": run_id}
            ).fetchone()
        else:
            # Create new run
            result = db.execute(
                text("""
                    INSERT INTO data_fetch_runs (
                        attraction_id,
                        data_type,
                        status,
                        items_target,
                        items_collected,
                        started_at,
                        metadata
                    )
                    VALUES (
                        :attraction_id,
                        'tips',
                        'RUNNING',
                        300,
                        0,
                        CURRENT_TIMESTAMP,
                        '{"source": "reddit"}'::jsonb
                    )
                    ON CONFLICT (attraction_id, data_type)
                    DO UPDATE SET
                        status = 'RUNNING',
                        started_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING *
                """),
                {"attraction_id": attraction_id}
            )
            db.commit()
            run = result.fetchone()
            run_id = run.id

        if not run:
            logger.error(f"Run {run_id} not found")
            return

        # Check if already done
        if run.status == 'DONE':
            logger.info(f"Run {run_id} already completed")
            return

        # Get attraction details
        attraction = db.execute(
            text("SELECT id, name, city FROM attraction WHERE id = :id"),
            {"id": attraction_id}
        ).fetchone()

        if not attraction:
            logger.error(f"Attraction {attraction_id} not found")
            _update_run_status(db, run_id, 'FAILED', 'Attraction not found')
            return

        logger.info(f"Fetching Reddit tips batch for {attraction.name} (Run {run_id})")

        try:
            # Fetch and process tips
            posts, tips_data = asyncio.run(_fetch_and_process_tips(
                self.reddit_client,
                self.gemini_client,
                attraction.name
            ))

            if not posts:
                logger.info(f"No more posts found for {attraction.name}")
                _update_run_status(db, run_id, 'DONE')
                return
            
            if tips_data and tips_data.get('tips'):
                # Save tips to database
                tips_saved = _save_tips_to_db(
                    db,
                    attraction_id,
                    tips_data['tips']
                )
                
                # Update run progress
                new_total = run.items_collected + tips_saved
                new_cursor = posts[-1].get('name') if posts else None
                
                # Update cursor_data JSON
                cursor_json = run.cursor_data if run.cursor_data else {}
                if new_cursor:
                    cursor_json['after'] = new_cursor
                
                db.execute(
                    text("""
                        UPDATE data_fetch_runs 
                        SET items_collected = :collected,
                            cursor_data = :cursor_data::jsonb,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :run_id
                    """),
                    {
                        "collected": new_total,
                        "cursor_data": str(cursor_json).replace("'", '"'),
                        "run_id": run_id
                    }
                )
                db.commit()
                
                logger.info(f"Saved {tips_saved} tips for {attraction.name}. Total: {new_total}/{run.items_target}")
                
                # Check if we need more tips
                if new_total < run.items_target:
                    # Schedule next batch with delay
                    fetch_reddit_tips_batch.apply_async(
                        args=[attraction_id, run_id],
                        countdown=BATCH_DELAY_SECONDS
                    )
                    logger.info(f"Scheduled next batch in {BATCH_DELAY_SECONDS}s")
                else:
                    # We're done!
                    _update_run_status(db, run_id, 'DONE')
                    logger.info(f"Completed fetching tips for {attraction.name}")
            else:
                logger.warning(f"No tips extracted from posts for {attraction.name}")
                # Try again with next batch
                if run.items_collected < run.items_target:
                    fetch_reddit_tips_batch.apply_async(
                        args=[attraction_id, run_id],
                        countdown=BATCH_DELAY_SECONDS
                    )
        
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a rate limit error
            if '429' in error_msg or 'rate limit' in error_msg.lower():
                logger.warning(f"Rate limit hit for {attraction.name}. Scheduling retry in {RATE_LIMIT_DELAY_MINUTES} minutes")
                
                # Update status to RATE_LIMITED
                next_run = datetime.utcnow() + timedelta(minutes=RATE_LIMIT_DELAY_MINUTES)
                db.execute(
                    text("""
                        UPDATE data_fetch_runs 
                        SET status = 'RATE_LIMITED',
                            next_run_at = :next_run,
                            last_error = :error,
                            retry_count = retry_count + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :run_id
                    """),
                    {
                        "next_run": next_run,
                        "error": error_msg,
                        "run_id": run_id
                    }
                )
                db.commit()
                
                # Schedule retry after delay
                fetch_reddit_tips_batch.apply_async(
                    args=[attraction_id, run_id],
                    countdown=RATE_LIMIT_DELAY_MINUTES * 60
                )
            else:
                # Other error - retry with exponential backoff
                logger.error(f"Error fetching tips for {attraction.name}: {e}")
                
                retry_count = run.retry_count + 1
                if retry_count < run.max_retries:
                    db.execute(
                        text("""
                            UPDATE data_fetch_runs 
                            SET last_error = :error,
                                retry_count = :retry_count,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = :run_id
                        """),
                        {
                            "error": error_msg,
                            "retry_count": retry_count,
                            "run_id": run_id
                        }
                    )
                    db.commit()
                    
                    # Retry with exponential backoff
                    raise self.retry(exc=e, countdown=60 * (2 ** retry_count))
                else:
                    _update_run_status(db, run_id, 'FAILED', error_msg)
    
    finally:
        db.close()


def _update_run_status(
    db: Session,
    run_id: int,
    status: str,
    error: Optional[str] = None
):
    """Update run status."""
    params = {
        "status": status,
        "run_id": run_id,
        "error": error,
        "completed_at": datetime.utcnow() if status in ('DONE', 'FAILED') else None
    }
    
    db.execute(
        text("""
            UPDATE data_fetch_runs 
            SET status = :status,
                last_error = :error,
                completed_at = :completed_at,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :run_id
        """),
        params
    )
    db.commit()


async def _process_posts_to_tips(
    gemini_client: GeminiClient,
    attraction_name: str,
    posts: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Process Reddit posts into structured tips."""
    # Extract content from posts
    content_snippets = []
    
    for post in posts[:20]:  # Use top 20 posts
        if post.get('title'):
            content_snippets.append(f"Post: {post['title']}")
        if post.get('selftext') and len(post['selftext']) > 20:
            content_snippets.append(f"Content: {post['selftext'][:500]}")
        
        for comment in post.get('comments', [])[:3]:
            if comment.get('body') and len(comment['body']) > 20:
                content_snippets.append(f"Comment: {comment['body'][:300]}")
    
    if not content_snippets:
        return None
    
    # Generate tips using Gemini
    prompt = f"""Based on these Reddit posts about {attraction_name}, extract practical travel tips.

Reddit Content:
{chr(10).join(content_snippets[:30])}

Generate tips in JSON format:
{{
  "safety": [
    {{"text": "<safety tip>", "source": "Reddit Community", "position": 1}}
  ],
  "insider": [
    {{"text": "<insider tip>", "source": "Reddit Community", "position": 1}}
  ]
}}

Focus on tourist-relevant advice: timing, tickets, crowds, safety, money-saving.
Return ONLY the JSON."""
    
    result = await gemini_client.generate_json(prompt)
    
    if not result:
        return None
    
    # Convert to DB format
    tips = []
    for tip in result.get("safety", []):
        tips.append({
            "tip_type": "SAFETY",
            "text": tip.get("text", ""),
            "source": tip.get("source", "Reddit"),
            "position": tip.get("position", 1)
        })
    
    for tip in result.get("insider", []):
        tips.append({
            "tip_type": "INSIDER",
            "text": tip.get("text", ""),
            "source": tip.get("source", "Reddit"),
            "position": tip.get("position", 1)
        })
    
    return {"tips": tips}


def _save_tips_to_db(
    db: Session,
    attraction_id: int,
    tips: List[Dict[str, Any]]
) -> int:
    """Save tips to database, avoiding duplicates."""
    saved_count = 0
    
    for tip in tips:
        try:
            # Check if tip already exists (by text similarity)
            existing = db.execute(
                text("""
                    SELECT id FROM tip 
                    WHERE attraction_id = :attraction_id 
                    AND tip_type = :tip_type
                    AND text = :text
                """),
                {
                    "attraction_id": attraction_id,
                    "tip_type": tip['tip_type'],
                    "text": tip['text']
                }
            ).fetchone()
            
            if not existing:
                db.execute(
                    text("""
                        INSERT INTO tip (attraction_id, tip_type, text, source, position)
                        VALUES (:attraction_id, :tip_type, :text, :source, :position)
                    """),
                    {
                        "attraction_id": attraction_id,
                        "tip_type": tip['tip_type'],
                        "text": tip['text'],
                        "source": tip.get('source', 'Reddit'),
                        "position": tip.get('position', 1)
                    }
                )
                saved_count += 1
        except Exception as e:
            logger.error(f"Error saving tip: {e}")
            continue
    
    db.commit()
    return saved_count


@celery_app.task
def resume_rate_limited_runs():
    """
    Periodic task to resume rate-limited runs for all data types.
    Should be scheduled to run every 5 minutes.
    """
    db: Session = SessionLocal()
    
    try:
        # Find runs that are ready to resume
        runs = db.execute(
            text("""
                SELECT id, attraction_id, data_type 
                FROM data_fetch_runs 
                WHERE status = 'RATE_LIMITED'
                AND next_run_at <= CURRENT_TIMESTAMP
            """)
        ).fetchall()
        
        for run in runs:
            logger.info(f"Resuming rate-limited {run.data_type} run {run.id} for attraction {run.attraction_id}")
            
            # Update status back to RUNNING
            db.execute(
                text("""
                    UPDATE data_fetch_runs 
                    SET status = 'RUNNING',
                        next_run_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :run_id
                """),
                {"run_id": run.id}
            )
            db.commit()
            
            # Trigger the appropriate task based on data_type
            if run.data_type == 'tips':
                fetch_reddit_tips_batch.delay(run.attraction_id, run.id)
            # Add more data types here as needed
            # elif run.data_type == 'reviews':
            #     fetch_reviews_batch.delay(run.attraction_id, run.id)
            # elif run.data_type == 'photos':
            #     fetch_photos_batch.delay(run.attraction_id, run.id)
    
    finally:
        db.close()
