"""Reddit API client for fetching travel tips and advice."""
import os
import re
import asyncpraw
import redis
import hashlib
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
import logging
from app.config import settings

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    """Normalize text for simple fuzzy matching."""
    text = text.lower()
    # strip basic punctuation / symbols
    return re.sub(r"[\W_]+", " ", text).strip()


class RedditClient:
    """Client for Reddit API using Async PRAW."""
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        user_agent: Optional[str] = None
    ):
        # Store credentials (no event loop needed)
        self.client_id = client_id or os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("REDDIT_CLIENT_SECRET")
        self.user_agent = user_agent or os.getenv("REDDIT_USER_AGENT", "Storyboard:v1.0.0 (by /u/storyboard_app)")

        # Don't create asyncpraw.Reddit here - will be created in __aenter__
        # This prevents event loop issues when used in Celery tasks
        self.reddit = None

        # Initialize Redis client (synchronous, safe to do in __init__)
        self.redis_client = None
        try:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", "6379"))
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=2,  # DB 2 for Reddit cache
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Redis cache initialized for Reddit client")
        except Exception as e:
            logger.warning(f"Redis unavailable for caching: {e}. Continuing without cache.")

    async def __aenter__(self):
        """Initialize asyncpraw.Reddit when entering async context.

        This ensures the aiohttp session is created in the CURRENT event loop,
        preventing 'event loop is closed' errors.
        """
        if not self.client_id or not self.client_secret:
            logger.warning("Reddit API credentials not set - Reddit client will not be available")
            return self

        try:
            # Create asyncpraw.Reddit INSIDE async context
            # This ties it to the CURRENT event loop
            self.reddit = asyncpraw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            logger.info("Reddit client initialized in async context")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit client: {e}")
            self.reddit = None

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close asyncpraw.Reddit when exiting async context.

        Ensures proper cleanup of aiohttp session in the same event loop
        where it was created.
        """
        if self.reddit:
            try:
                await self.reddit.close()
                logger.info("Reddit session closed")
            except Exception as e:
                logger.warning(f"Error closing Reddit session: {e}")

        # Return False to propagate any exceptions
        return False

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """Generate cache key from parameters."""
        sorted_params = sorted(kwargs.items())
        params_str = json.dumps(sorted_params, sort_keys=True)
        hash_digest = hashlib.md5(params_str.encode()).hexdigest()
        return f"reddit:{prefix}:{hash_digest}"

    def _get_cached(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached Reddit response."""
        if not self.redis_client:
            return None

        try:
            cached = self.redis_client.get(cache_key)
            if cached:
                logger.info(f"Cache HIT: {cache_key[:50]}...")
                return json.loads(cached)
        except Exception as e:
            logger.warning(f"Cache read error: {e}")

        return None

    def _set_cached(self, cache_key: str, data: List[Dict[str, Any]], ttl: int = None):
        """Cache Reddit response with TTL."""
        if not self.redis_client:
            return

        if ttl is None:
            ttl = settings.CACHE_TTL_REDDIT

        try:
            self.redis_client.setex(cache_key, ttl, json.dumps(data))
            logger.info(f"Cache SET: {cache_key[:50]}... (TTL: {ttl}s)")
        except Exception as e:
            logger.warning(f"Cache write error: {e}")

    async def _fetch_submission_comments(
        self,
        submission: "asyncpraw.models.Submission",
        max_comments: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch up to `max_comments` reasonably long comments for a submission."""
        top_comments: List[Dict[str, Any]] = []
        
        try:
            # This is the pattern recommended in the asyncpraw docs
            comments_forest = await submission.comments()
            await comments_forest.replace_more(limit=0)
            
            comment_list = await comments_forest.list()
        except Exception as e:
            logger.warning(
                "Error fetching comments for submission %s: %s",
                getattr(submission, "id", "unknown"),
                e,
            )
            return top_comments  # empty list
        
        # Safeguard: comments_forest.list() *should* return a list, but let's be defensive
        if comment_list is None:
            return top_comments
        
        for comment in comment_list[:max_comments]:
            body = getattr(comment, "body", None)
            if not body or len(body) <= 20:
                continue
            
            score = getattr(comment, "score", 0)
            top_comments.append({
                "body": body,
                "score": score,
            })
        
        return top_comments
    
    async def search_posts(
        self,
        query: str,
        subreddits: Optional[List[str]] = None,
        limit: int = 50,
        time_filter: str = "year"
    ) -> List[Dict[str, Any]]:
        """Search Reddit posts for a query.
        
        Args:
            query: Search query (e.g., "Eiffel Tower tips")
            subreddits: List of subreddits to search (default: travel-related)
            limit: Maximum number of posts to fetch
            time_filter: Time filter (hour, day, week, month, year, all)
        
        Returns:
            List of post dictionaries with title, selftext, score, comments
        """
        if not self.reddit:
            logger.error("Reddit client not initialized")
            return []
        
        if subreddits is None:
            subreddits = ["travel", "solotravel", "TravelHacks", "backpacking", "Shoestring"]
        
        posts: List[Dict[str, Any]] = []
        
        # Avoid a 0 limit per subreddit (which could cause weird behaviour)
        total_limit = max(1, limit)
        per_subreddit_limit = max(1, total_limit // len(subreddits))
        
        try:
            for subreddit_name in subreddits:
                try:
                    subreddit = await self.reddit.subreddit(subreddit_name)
                    
                    search_results = subreddit.search(
                        query,
                        limit=per_subreddit_limit,
                        time_filter=time_filter,
                        sort="relevance",
                    )
                    
                    async for submission in search_results:
                        # Fetch up to 10 decent comments, but don't let failures kill the whole subreddit
                        top_comments = await self._fetch_submission_comments(
                            submission,
                            max_comments=10,
                        )
                        
                        posts.append({
                            "id": submission.id,
                            "title": submission.title,
                            "selftext": submission.selftext,
                            "score": submission.score,
                            "url": f"https://reddit.com{submission.permalink}",
                            "subreddit": subreddit_name,
                            "created_utc": getattr(submission, "created_utc", None),
                            "comments": top_comments,
                        })
                
                except Exception as e:
                    logger.warning(
                        "Error searching subreddit %s for query %r: %s",
                        subreddit_name,
                        query,
                        e,
                    )
                    continue
            
            logger.info("Found %d Reddit posts for query: %s", len(posts), query)
            return posts
        
        except Exception as e:
            logger.error("Error searching Reddit for query %r: %s", query, e)
            return []
    
    async def get_attraction_specific_posts(
        self,
        attraction_name: str,
        limit: int = 50,
        city: Optional[str] = None,
        min_score: int = 1,
        max_age_days: int = 1095,  # 3 years for more historical data
    ) -> List[Dict[str, Any]]:
        """Get posts specific to an attraction (optionally scoped by city).

        Args:
            attraction_name: Name of the attraction
            limit: Maximum number of posts
            city: Optional city name to narrow the search
            min_score: Minimum Reddit score required for a post
            max_age_days: Ignore posts older than this many days

        Returns:
            List of relevant posts, ranked by score + recency
        """
        if not self.reddit:
            logger.error("Reddit client not initialized")
            return []

        # Check cache first
        cache_key = self._generate_cache_key(
            "attraction",
            name=attraction_name,
            city=city or "",
            limit=limit,
            min_score=min_score,
            max_age_days=max_age_days
        )

        cached_posts = self._get_cached(cache_key)
        if cached_posts is not None:
            return cached_posts

        total_limit = max(1, limit)
        
        # Build queries with multiple strategies to maximize Reddit coverage
        # Strategy 1: Exact phrase with city (most specific)
        # Strategy 2: Exact phrase without city (still specific)
        # Strategy 3: Loose terms with city (broader)
        # Strategy 4: Loose terms without city (broadest)
        
        queries = []
        
        if city:
            # Exact phrase queries with city
            queries.extend([
                f'"{attraction_name}" "{city}"',
                f'"{attraction_name}" {city}',
                f'{attraction_name} {city} tips',
                f'{attraction_name} {city} visit',
                f'{attraction_name} {city} review',
            ])
        
        # Exact phrase queries without city
        queries.extend([
            f'"{attraction_name}"',
            f'{attraction_name} tips',
            f'{attraction_name} visit',
            f'{attraction_name} review',
            f'{attraction_name} advice',
            f'{attraction_name} experience',
        ])
        
        # Loose term queries (catch more posts)
        if city:
            queries.extend([
                f'{attraction_name} {city}',
                f'visiting {attraction_name}',
                f'things to do {city}',
            ])
        else:
            queries.extend([
                f'visiting {attraction_name}',
                f'best {attraction_name}',
            ])

        attraction_norm = _normalize(attraction_name)
        now = datetime.now(timezone.utc)
        min_created = now - timedelta(days=max_age_days)
        seen_ids: set[str] = set()
        all_posts: List[Dict[str, Any]] = []

        async def process_query(query: str, per_query_limit: int) -> None:
            nonlocal all_posts
            try:
                posts = await self.search_posts(
                    query=query,
                    limit=per_query_limit,
                    time_filter="year",
                )
            except Exception as e:
                logger.warning("Reddit search failed for query %r: %s", query, e)
                return

            for post in posts:
                post_id = post.get("id")
                if not post_id or post_id in seen_ids:
                    continue

                title = post.get("title") or ""
                body = post.get("selftext") or ""
                combined = _normalize(f"{title} {body}")

                # Require attraction name in title/body; fallback to URL
                if attraction_norm not in combined:
                    url_norm = _normalize(post.get("url") or "")
                    if attraction_norm not in url_norm:
                        continue

                score = int(post.get("score") or 0)
                if score < min_score:
                    continue

                created_utc = post.get("created_utc")
                try:
                    created_dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
                except Exception:
                    created_dt = now

                if created_dt < min_created:
                    continue

                seen_ids.add(post_id)

                # Store datetime as ISO string for JSON serialization (caching)
                post["created_dt_iso"] = created_dt.isoformat()
                all_posts.append(post)

        # Distribute limit across all queries - more aggressive search
        per_query_limit = max(3, total_limit // max(1, len(queries)))

        # Run all queries in parallel-like fashion, stopping early if we have enough
        for query in queries:
            if len(all_posts) >= total_limit:
                break
            await process_query(query, per_query_limit)

        # Ranking: score minus age penalty (1 point per month)
        def post_rank(p: Dict[str, Any]) -> float:
            score = int(p.get("score") or 0)
            # Parse ISO string back to datetime for ranking
            created_dt_iso = p.get("created_dt_iso")
            if created_dt_iso:
                try:
                    created_dt = datetime.fromisoformat(created_dt_iso)
                except Exception:
                    created_dt = now
            else:
                created_dt = now
            age_days = max((now - created_dt).days, 0)
            return score - (age_days / 30.0)

        all_posts.sort(key=post_rank, reverse=True)

        result = all_posts[:total_limit]

        # Cache the results
        self._set_cached(cache_key, result)

        logger.info(
            "Found %d unique relevant posts for attraction: %s",
            len(result),
            attraction_name,
        )

        return result

    async def get_city_specific_posts(
        self,
        city_name: str,
        limit: int = 30,
        min_score: int = 3,
        max_age_days: int = 365
    ) -> List[Dict[str, Any]]:
        """Get city-wide travel tips from Reddit.

        Args:
            city_name: Name of the city
            limit: Maximum number of posts
            min_score: Minimum Reddit score required for a post
            max_age_days: Ignore posts older than this many days

        Returns:
            List of relevant city-wide posts, ranked by score + recency
        """
        if not self.reddit:
            logger.error("Reddit client not initialized")
            return []

        total_limit = max(1, limit)

        # City-wide queries
        queries = [
            f'"{city_name}" travel tips',
            f'"{city_name}" safety advice',
            f'"{city_name}" insider tips',
            f'visiting "{city_name}" advice',
            f'"{city_name}" things to know'
        ]

        city_norm = _normalize(city_name)
        now = datetime.now(timezone.utc)
        min_created = now - timedelta(days=max_age_days)
        seen_ids: set[str] = set()
        all_posts: List[Dict[str, Any]] = []

        per_query_limit = max(3, total_limit // max(1, len(queries)))

        for query in queries:
            if len(all_posts) >= total_limit:
                break

            try:
                posts = await self.search_posts(
                    query=query,
                    limit=per_query_limit,
                    time_filter="year"
                )
            except Exception as e:
                logger.warning("Reddit search failed for query %r: %s", query, e)
                continue

            for post in posts:
                post_id = post.get("id")
                if not post_id or post_id in seen_ids:
                    continue

                title = post.get("title") or ""
                body = post.get("selftext") or ""
                combined = _normalize(f"{title} {body}")

                # Require city name in title/body
                if city_norm not in combined:
                    url_norm = _normalize(post.get("url") or "")
                    if city_norm not in url_norm:
                        continue

                score = int(post.get("score") or 0)
                if score < min_score:
                    continue

                created_utc = post.get("created_utc")
                try:
                    created_dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
                except Exception:
                    created_dt = now

                if created_dt < min_created:
                    continue

                seen_ids.add(post_id)
                # Store datetime as ISO string for JSON serialization (caching)
                post["created_dt_iso"] = created_dt.isoformat()
                all_posts.append(post)

        # Ranking: score minus age penalty
        def post_rank(p: Dict[str, Any]) -> float:
            score = int(p.get("score") or 0)
            # Parse ISO string back to datetime for ranking
            created_dt_iso = p.get("created_dt_iso")
            if created_dt_iso:
                try:
                    created_dt = datetime.fromisoformat(created_dt_iso)
                except Exception:
                    created_dt = now
            else:
                created_dt = now
            age_days = max((now - created_dt).days, 0)
            return score - (age_days / 30.0)

        all_posts.sort(key=post_rank, reverse=True)
        result = all_posts[:total_limit]

        logger.info(
            "Found %d unique city-wide posts for: %s",
            len(result),
            city_name
        )

        return result

    async def close(self):
        """Close the Reddit session. Call this after processing all attractions."""
        if self.reddit:
            try:
                await self.reddit.close()
                logger.info("Reddit session closed")
            except Exception as e:
                logger.warning(f"Error closing Reddit session: {e}")
