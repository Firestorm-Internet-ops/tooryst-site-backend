"""Tips Fetcher implementation using Reddit API, Google API, and Gemini."""
import os
from typing import Optional, Dict, Any, List
import logging
from .reddit_client import RedditClient
from .gemini_client import GeminiClient
from .gemini_tips_fallback import GeminiTipsFallback

logger = logging.getLogger(__name__)


class TipsFetcherImpl:
    """Fetches tips using Reddit API with Gemini fallback."""

    def __init__(
        self,
        reddit_client: Optional[RedditClient] = None,  # For testing/dependency injection
        gemini_client: Optional[GeminiClient] = None,
        fallback: Optional[GeminiTipsFallback] = None
    ):
        # Don't create RedditClient here - will be created lazily in fetch()
        self._injected_reddit_client = reddit_client  # Store for testing
        self.gemini_client = gemini_client or GeminiClient()
        self.fallback = fallback or GeminiTipsFallback()
    
    async def fetch(
        self,
        attraction_id: int,
        place_id: Optional[str],
        attraction_name: Optional[str] = None,
        city_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch tips for an attraction.

        Returns:
        - section: Tips section data with safety and insider tips
        - tips: List of tips for DB storage
        - source: "reddit_api" or "gemini_fallback"
        """
        if not attraction_name or not city_name:
            logger.warning(f"Missing attraction_name or city_name for attraction {attraction_id}")
            return None

        # Use injected client (for testing) or create new one
        if self._injected_reddit_client:
            # Testing path: use pre-created client
            result = await self._try_reddit_fetch(
                reddit_client=self._injected_reddit_client,
                attraction_name=attraction_name,
                city_name=city_name
            )
            if result:
                return result
        else:
            # Production path: create RedditClient INSIDE async context
            async with RedditClient() as reddit_client:
                result = await self._try_reddit_fetch(
                    reddit_client=reddit_client,
                    attraction_name=attraction_name,
                    city_name=city_name
                )
                if result:
                    return result
                # Reddit session automatically closed by __aexit__

        # Fall back to Gemini
        logger.info(f"Using Gemini fallback for tips: {attraction_name}")
        return await self._try_gemini_fallback(attraction_name, city_name)

    async def _try_reddit_fetch(
        self,
        reddit_client: RedditClient,
        attraction_name: str,
        city_name: str
    ) -> Optional[Dict[str, Any]]:
        """Try fetching from Reddit API.

        Returns tips dict if successful, None if failed or insufficient data.
        """
        if not reddit_client.reddit:
            logger.info("Reddit client not available (no credentials or init failed)")
            return None

        try:
            logger.info(f"Fetching attraction-specific tips from Reddit for {attraction_name}")

            reddit_posts = await reddit_client.get_attraction_specific_posts(
                attraction_name=attraction_name,
                limit=50,  # Increased limit for better filtering
                city=city_name,
                min_score=1,  # Lowered threshold to get more posts
                max_age_days=1095,  # 3 years for more historical data
            )

            # Try with even 1 post if available - better to use real Reddit data than generate
            if reddit_posts and len(reddit_posts) >= 1:
                result = await self._process_reddit_posts(
                    attraction_name=attraction_name,
                    city_name=city_name,
                    posts=reddit_posts,
                    scope="attraction"
                )
                if result:
                    logger.info(f"✓ Successfully fetched attraction-specific tips from Reddit for {attraction_name}")
                    return result
                else:
                    logger.warning(f"Reddit posts found but processing failed for {attraction_name}")
            else:
                logger.warning(f"Insufficient attraction-specific Reddit posts found for {attraction_name} (found: {len(reddit_posts) if reddit_posts else 0})")
                
                # Fallback: Try city-wide Reddit posts if attraction-specific search failed
                # NOTE: Even though we're using city-wide posts, the tips are still about the specific attraction
                # so scope should remain "attraction" - the scope indicates what the tip is about, not the data source
                if city_name:
                    logger.info(f"Trying city-wide Reddit posts for {city_name} as fallback")
                    try:
                        city_posts = await reddit_client.get_city_specific_posts(
                            city_name=city_name,
                            limit=50,
                            min_score=1,
                            max_age_days=1095
                        )
                        
                        if city_posts and len(city_posts) >= 1:
                            result = await self._process_reddit_posts(
                                attraction_name=attraction_name,
                                city_name=city_name,
                                posts=city_posts,
                                scope="attraction"
                            )
                            if result:
                                logger.info(f"✓ Successfully fetched city-wide tips from Reddit for {attraction_name}")
                                return result
                    except Exception as e:
                        logger.warning(f"Error fetching city-wide Reddit posts: {e}")

        except Exception as e:
            logger.error(f"Error fetching from Reddit: {e}")

        return None

    async def _try_gemini_fallback(
        self,
        attraction_name: str,
        city_name: str
    ) -> Optional[Dict[str, Any]]:
        """Try Gemini fallback for tips generation."""
        try:
            gemini_result = await self.fallback.generate_tips(
                attraction_name=attraction_name,
                city_name=city_name
            )
            if gemini_result:
                # Ensure scope is set to 'attraction'
                if 'tips' in gemini_result:
                    for tip in gemini_result['tips']:
                        tip['scope'] = 'attraction'
                logger.info(f"✓ Generated tips using Gemini for {attraction_name}")
            return gemini_result
        except Exception as e:
            logger.error(f"Failed to generate tips with Gemini: {e}")
            return None

    async def _process_reddit_posts(
        self,
        attraction_name: str,
        city_name: str,
        posts: List[Dict[str, Any]],
        scope: str = "attraction"
    ) -> Optional[Dict[str, Any]]:
        """Process Reddit posts into structured tips using Gemini.

        Args:
            attraction_name: Name of the attraction
            city_name: Name of the city
            posts: Reddit posts to process
            scope: Either "attraction" or "city" to indicate tip specificity
        """
        # Prefer higher score posts if not already sorted
        posts = sorted(
            posts,
            key=lambda p: (int(p.get("score") or 0)),
            reverse=True,
        )
        
        # Extract relevant content from posts
        content_snippets = []
        
        for post in posts[:20]:  # Use top 20 posts
            # Add post title and text
            if post.get('title'):
                content_snippets.append(f"Post: {post['title']}")
            if post.get('selftext') and len(post['selftext']) > 20:
                content_snippets.append(f"Content: {post['selftext'][:500]}")
            
            # Add top comments
            for comment in post.get('comments', [])[:3]:  # Top 3 comments per post
                if comment.get('body') and len(comment['body']) > 20:
                    content_snippets.append(f"Comment: {comment['body'][:300]}")
        
        if not content_snippets:
            logger.warning("No useful content extracted from Reddit posts")
            return None
        
        # Use Gemini to synthesize tips from Reddit content
        source_label = attraction_name if scope == "attraction" else city_name
        prompt = f"""Based on these Reddit posts and comments about {attraction_name} in {city_name}, create practical tips.

IMPORTANT: These tips must be SPECIFIC to "{attraction_name}" in {city_name}. Do not generate generic travel tips.

Reddit Content:
{chr(10).join(content_snippets[:50])}

Generate 6 SAFETY tips and 7 INSIDER tips based on this real user content. All tips must be directly relevant to {attraction_name} specifically.

SAFETY TIPS:
- 1 tip with position 0: Short and critical (1-2 lines, most important safety concern specific to {attraction_name})
- 5 tips with position 1: Detailed safety advice (2-3 lines each, specific to {attraction_name})

INSIDER TIPS:
- 2 tips with position 0: Quick insider secrets (1-2 lines each, best kept secrets about {attraction_name})
- 5 tips with position 1: Detailed insider advice (2-3 lines each, specific to {attraction_name})

Return ONLY a JSON object with this structure:

{{
  "safety": [
    {{
      "text": "<1-2 line critical safety tip specific to {attraction_name}>",
      "position": 0,
      "source": "Reddit ({source_label})",
      "scope": "{scope}"
    }},
    {{
      "text": "<2-3 line detailed safety tip specific to {attraction_name}>",
      "position": 1,
      "source": "Reddit ({source_label})",
      "scope": "{scope}"
    }},
    ... (5 more position 1 safety tips)
  ],
  "insider": [
    {{
      "text": "<1-2 line insider secret about {attraction_name}>",
      "position": 0,
      "source": "Reddit ({source_label})",
      "scope": "{scope}"
    }},
    {{
      "text": "<1-2 line insider secret about {attraction_name}>",
      "position": 0,
      "source": "Reddit ({source_label})",
      "scope": "{scope}"
    }},
    {{
      "text": "<2-3 line detailed insider tip about {attraction_name}>",
      "position": 1,
      "source": "Reddit ({source_label})",
      "scope": "{scope}"
    }},
    ... (4 more position 1 insider tips)
  ]
}}

Guidelines:
- TRAVEL & TOURISM FOCUS: Extract practical advice for tourists visiting {attraction_name} specifically
- Extract and synthesize actual advice from the Reddit content
- Keep the authentic voice and specific details from Reddit users
- Focus on tourist-relevant information: timing, tickets, crowds, photos, safety, money-saving
- ALL tips must be about {attraction_name} specifically, not generic travel advice
- Source format: "Reddit ({source_label})" to indicate source specificity
- Scope: "{scope}" indicates if tips are attraction-specific or city-wide
- Position 0 tips are SHORT (1-2 lines), position 1 tips are DETAILED (2-3 lines)

Return ONLY the JSON, no other text."""

        result = await self.gemini_client.generate_json(prompt)
        
        if not result:
            logger.error("Failed to process Reddit content with Gemini")
            return None
        
        # Extract tips
        safety_tips = result.get("safety", [])
        insider_tips = result.get("insider", [])

        # Process tips for DB storage
        all_tips = []
        source_label = f"Reddit - {scope.capitalize()}"

        for tip in safety_tips:
            all_tips.append({
                "tip_type": "SAFETY",
                "text": tip.get("text", ""),
                "source": tip.get("source", source_label),
                "scope": scope
            })

        for tip in insider_tips:
            all_tips.append({
                "tip_type": "INSIDER",
                "text": tip.get("text", ""),
                "source": tip.get("source", source_label),
                "scope": scope
            })

        # Skip validation for Reddit-processed tips - Gemini already extracted relevant tips from Reddit content
        # Validation was causing issues: rejecting good tips, extra API calls, mislabeling, etc.
        logger.info(f"Processed {len(safety_tips)} safety and {len(insider_tips)} insider tips from Reddit for {attraction_name}")

        return {
            "section": {
                "safety": [
                    {
                        "text": tip.get("text", ""),
                        "source": tip.get("source", source_label),
                        "scope": scope
                    }
                    for tip in safety_tips
                ],
                "insider": [
                    {
                        "text": tip.get("text", ""),
                        "source": tip.get("source", source_label),
                        "scope": scope
                    }
                    for tip in insider_tips
                ]
            },
            "tips": all_tips,
            "source": "reddit_api",
            "scope": scope
        }

    async def _process_city_posts(
        self,
        attraction_name: str,
        city_name: str,
        posts: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Process city-wide Reddit posts into attraction-relevant tips using Gemini.

        NOTE: This method is kept for potential future use but is not currently called.
        We prefer fewer high-quality attraction-specific tips over padding with city-wide tips.

        Args:
            attraction_name: Name of the attraction (for contextualization)
            city_name: Name of the city (source of tips)
            posts: Reddit posts about the city

        Returns:
            Structured tips with scope="city"
        """
        # Use the same processing logic but with city scope
        return await self._process_reddit_posts(
            attraction_name=attraction_name,
            city_name=city_name,
            posts=posts,
            scope="city"
        )

    async def _validate_tips_relevance(
        self,
        attraction_name: str,
        city_name: str,
        safety_tips: List[Dict[str, Any]],
        insider_tips: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Validate tips relevance using Gemini AI."""
        
        # Prepare tips for validation
        all_tips_for_validation = []
        
        for tip in safety_tips:
            all_tips_for_validation.append({
                "type": "SAFETY",
                "text": tip.get("text", ""),
                "position": tip.get("position", 1)
            })
        
        for tip in insider_tips:
            all_tips_for_validation.append({
                "type": "INSIDER", 
                "text": tip.get("text", ""),
                "position": tip.get("position", 1)
            })
        
        if not all_tips_for_validation:
            return None
        
        validation_prompt = f"""You are an expert validator for travel tips. Your task is to evaluate how relevant each tip is to the specific attraction: "{attraction_name}" in {city_name}.

Rate each tip on a scale of 1-10 based on how directly related it is to {attraction_name} specifically:
- 10: Extremely relevant - directly mentions {attraction_name} or its specific features/operations
- 9: Very relevant - specific advice about visiting {attraction_name} or its immediate surroundings
- 8: Relevant - practical advice that directly applies to {attraction_name} experience
- 7: Somewhat relevant - general travel advice that applies to {attraction_name}
- 6: Minimally relevant - loosely connected or mostly about {city_name} in general
- 1-5: Not relevant - generic travel advice or about something else entirely

Tips to validate:
{chr(10).join([f"{i+1}. [{tip['type']}] {tip['text']}" for i, tip in enumerate(all_tips_for_validation)])}

Return ONLY a JSON object with this structure:
{{
  "validated_tips": [
    {{
      "original_index": 0,
      "type": "SAFETY",
      "text": "<original text>",
      "relevance_score": 9,
      "is_relevant": true,
      "reason": "Directly mentions {attraction_name} and specific safety concern"
    }}
  ],
  "validation_summary": {{
    "total_tips": 13,
    "relevant_tips": 11,
    "avg_relevance_score": 8.5
  }}
}}

RULES:
- Only include tips with relevance_score >= 8/10
- If less than 2 safety tips or 2 insider tips pass validation, return empty validated_tips array (relaxed threshold)
- Preserve original tip text exactly as provided
- Be strict about attraction-specificity - tips must mention {attraction_name} or its specific features
- Reject generic city travel advice that doesn't apply specifically to {attraction_name}
- Prioritize tips that mention specific details about {attraction_name} (hours, location, features, etc.)

Return ONLY the JSON, no other text."""

        try:
            validation_result = await self.gemini_client.generate_json(validation_prompt)
            
            if not validation_result:
                logger.error("Failed to validate tips relevance with Gemini")
                return None
            
            validated_tips = validation_result.get("validated_tips", [])
            
            # Separate validated tips by type
            validated_safety = []
            validated_insider = []
            
            for tip_data in validated_tips:
                if tip_data.get("is_relevant", False):
                    original_index = tip_data.get("original_index")
                    if 0 <= original_index < len(all_tips_for_validation):
                        original_tip = all_tips_for_validation[original_index]
                        # Find original tip source and scope if available
                        original_source = None
                        original_scope = "attraction"
                        if original_tip["type"] == "SAFETY":
                            for st in safety_tips:
                                if st.get("text") == original_tip["text"]:
                                    original_source = st.get("source", "Reddit - Attraction")
                                    original_scope = st.get("scope", "attraction")
                                    break
                        else:
                            for it in insider_tips:
                                if it.get("text") == original_tip["text"]:
                                    original_source = it.get("source", "Reddit - Attraction")
                                    original_scope = it.get("scope", "attraction")
                                    break

                        validated_tip = {
                            "text": original_tip["text"],
                            "source": original_source or "Reddit - Attraction",
                            "scope": original_scope,
                            "relevance_score": tip_data.get("relevance_score", 0)
                        }
                        
                        if original_tip["type"] == "SAFETY":
                            validated_safety.append(validated_tip)
                        else:
                            validated_insider.append(validated_tip)
            
            logger.info(f"Validation results for {attraction_name}: "
                       f"Safety: {len(validated_safety)}, Insider: {len(validated_insider)}")
            
            # Only return if we have minimum viable tips (at least 2 total, with at least 1 of each type)
            total_tips = len(validated_safety) + len(validated_insider)
            if total_tips >= 2 and len(validated_safety) >= 1 and len(validated_insider) >= 1:
                return {
                    "safety": validated_safety,
                    "insider": validated_insider
                }
            else:
                logger.warning(f"Insufficient relevant tips after validation for {attraction_name}: "
                             f"Safety: {len(validated_safety)}, Insider: {len(validated_insider)} (need at least 1 of each, 2 total)")
                return None
                
        except Exception as e:
            logger.error(f"Error validating tips relevance: {e}")
            return None
