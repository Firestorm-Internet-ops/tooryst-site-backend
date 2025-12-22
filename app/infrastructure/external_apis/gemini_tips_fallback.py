"""Gemini-based generator for Safety and Insider Tips."""
import logging
from typing import Optional, Dict, Any, List
from .gemini_client import GeminiClient

logger = logging.getLogger(__name__)


class GeminiTipsFallback:
    """Generate safety and insider tips using Gemini AI."""
    
    def __init__(self, client: Optional[GeminiClient] = None):
        self.client = client or GeminiClient()
    
    async def generate_tips(
        self,
        attraction_name: str,
        city_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Generate safety and insider tips using Gemini AI.
        
        Args:
            attraction_name: Name of the attraction
            city_name: Optional city name for context
        
        Returns:
            Dict with section and tips data
        """
        logger.info(f"Generating tips for {attraction_name} using Gemini")
        
        location_context = f"{attraction_name}" + (f" in {city_name}" if city_name else "")
        prompt = f"""Generate practical tips for visiting {location_context}.

Create 6 SAFETY tips and 7 INSIDER tips.

SAFETY TIPS:
- 1 tip with position 0: Short and critical (1-2 lines, most important safety concern)
- 5 tips with position 1: Detailed safety advice (2-3 lines each)

INSIDER TIPS:
- 2 tips with position 0: Quick insider secrets (1-2 lines each, best kept secrets)
- 5 tips with position 1: Detailed insider advice (2-3 lines each)

Return ONLY a JSON object with this structure:

{{
  "safety": [
    {{
      "text": "<1-2 line critical safety tip specific to {attraction_name}>",
      "position": 0,
      "source": "Gemini ({attraction_name})",
      "scope": "attraction"
    }},
    {{
      "text": "<2-3 line detailed safety tip specific to {attraction_name}>",
      "position": 1,
      "source": "Gemini ({attraction_name})",
      "scope": "attraction"
    }},
    ... (5 more position 1 safety tips)
  ],
  "insider": [
    {{
      "text": "<1-2 line insider secret about {attraction_name}>",
      "position": 0,
      "source": "Gemini ({attraction_name})",
      "scope": "attraction"
    }},
    {{
      "text": "<1-2 line insider secret about {attraction_name}>",
      "position": 0,
      "source": "Gemini ({attraction_name})",
      "scope": "attraction"
    }},
    {{
      "text": "<2-3 line detailed insider tip about {attraction_name}>",
      "position": 1,
      "source": "Gemini ({attraction_name})",
      "scope": "attraction"
    }},
    ... (4 more position 1 insider tips)
  ]
}}

Guidelines:
- TRAVEL & TOURISM FOCUS: All tips must be practical for tourists visiting {attraction_name} specifically
- Safety tips: Pickpockets, scams, crowds, weather, health, accessibility, tourist-specific safety at {attraction_name}
- Insider tips: Best times to visit {attraction_name}, hidden spots, photo locations, local secrets, money-saving tips, skip-the-line tricks
- ALL tips must be about {attraction_name} specifically, not generic travel advice
- Source format: "Gemini ({attraction_name})" to indicate attraction-specific tips
- Scope: Always "attraction" for these tips (they are about the specific attraction)
- Position 0 tips are SHORT (1-2 lines), position 1 tips are DETAILED (2-3 lines)
- Focus on practical, actionable advice for visitors to {attraction_name}

Return ONLY the JSON, no other text."""

        result = await self.client.generate_json(prompt)
        
        if not result:
            logger.error("Failed to generate tips with Gemini")
            return None
        
        # Extract tips
        safety_tips = result.get("safety", [])
        insider_tips = result.get("insider", [])
        
        # Validate counts
        if len(safety_tips) != 6:
            logger.warning(f"Expected 6 safety tips, got {len(safety_tips)}")
        if len(insider_tips) != 7:
            logger.warning(f"Expected 7 insider tips, got {len(insider_tips)}")
        
        # Process tips for DB storage
        all_tips = []
        default_source = f"Gemini ({attraction_name})"

        # Process safety tips
        for idx, tip in enumerate(safety_tips):
            all_tips.append({
                "tip_type": "SAFETY",
                "text": tip.get("text", ""),
                "source": tip.get("source", default_source),
                "scope": tip.get("scope", "attraction"),
                "position": tip.get("position", 1)
            })

        # Process insider tips
        for idx, tip in enumerate(insider_tips):
            all_tips.append({
                "tip_type": "INSIDER",
                "text": tip.get("text", ""),
                "source": tip.get("source", default_source),
                "scope": tip.get("scope", "attraction"),
                "position": tip.get("position", 1)
            })

        # Skip validation for Gemini-generated tips - Gemini already generated attraction-specific tips
        # Validation was causing issues: mislabeling insider tips as safety, extra API calls, etc.
        logger.info(f"Generated {len(safety_tips)} safety and {len(insider_tips)} insider tips for {attraction_name}")

        return {
            "section": {
                "safety": [
                    {
                        "text": tip.get("text", ""),
                        "source": tip.get("source", default_source),
                        "scope": tip.get("scope", "attraction")
                    }
                    for tip in safety_tips
                ],
                "insider": [
                    {
                        "text": tip.get("text", ""),
                        "source": tip.get("source", default_source),
                        "scope": tip.get("scope", "attraction")
                    }
                    for tip in insider_tips
                ]
            },
            "tips": all_tips,  # For DB storage
            "source": "gemini_fallback",
            "scope": "attraction"
        }
    
    async def _validate_tips_relevance(
        self,
        attraction_name: str,
        city_name: Optional[str] = None,
        safety_tips: List[Dict[str, Any]] = None,
        insider_tips: List[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Validate tips relevance using Gemini AI."""
        
        # Prepare tips for validation
        all_tips_for_validation = []
        
        for tip in safety_tips or []:
            all_tips_for_validation.append({
                "type": "SAFETY",
                "text": tip.get("text", ""),
                "position": tip.get("position", 1)
            })
        
        for tip in insider_tips or []:
            all_tips_for_validation.append({
                "type": "INSIDER", 
                "text": tip.get("text", ""),
                "position": tip.get("position", 1)
            })
        
        if not all_tips_for_validation:
            return None
        
        location_context = f"{attraction_name}" + (f" in {city_name}" if city_name else "")
        validation_prompt = f"""You are an expert validator for travel tips. Your task is to evaluate how relevant each tip is to the specific attraction: "{attraction_name}"{f" in {city_name}" if city_name else ""}.

Rate each tip on a scale of 1-10 based on how directly related it is to {attraction_name} specifically:
- 10: Extremely relevant - directly about {attraction_name} specifically
- 8-9: Very relevant - specific to {attraction_name} or its immediate area{f" in {city_name}" if city_name else ""}
- 6-7: Somewhat relevant - general travel advice that applies to {attraction_name}
- 4-5: Minimally relevant - loosely connected or generic{f" {city_name}" if city_name else ""} advice
- 1-3: Not relevant - generic travel advice or about something else entirely

Tips to validate:
{chr(10).join([f"{i+1}. [{tip['type']}] {tip['text']} (Position: {tip['position']})" for i, tip in enumerate(all_tips_for_validation)])}

Return ONLY a JSON object with this structure:
{{
  "validated_tips": [
    {{
      "original_index": 0,
      "type": "SAFETY",
      "text": "<original text>",
      "position": 1,
      "relevance_score": 9,
      "is_relevant": true
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
- Position values should remain unchanged
- Be strict - only tips that are specifically about {attraction_name} (not just{f" {city_name}" if city_name else ""} in general) should pass
- Reject generic{f" {city_name}" if city_name else ""} travel advice that doesn't mention {attraction_name} specifically

Return ONLY the JSON, no other text."""

        try:
            validation_result = await self.client.generate_json(validation_prompt)
            
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
                            for st in safety_tips or []:
                                if st.get("text") == original_tip["text"]:
                                    original_source = st.get("source", f"Gemini ({attraction_name})")
                                    original_scope = st.get("scope", "attraction")
                                    break
                        else:
                            for it in insider_tips or []:
                                if it.get("text") == original_tip["text"]:
                                    original_source = it.get("source", f"Gemini ({attraction_name})")
                                    original_scope = it.get("scope", "attraction")
                                    break

                        validated_tip = {
                            "text": original_tip["text"],
                            "position": original_tip["position"],
                            "source": original_source or f"Gemini ({attraction_name})",
                            "scope": original_scope,
                            "relevance_score": tip_data.get("relevance_score", 0)
                        }
                        
                        if original_tip["type"] == "SAFETY":
                            validated_safety.append(validated_tip)
                        else:
                            validated_insider.append(validated_tip)
            
            logger.info(f"Validation results for {attraction_name}: "
                       f"Safety: {len(validated_safety)}, Insider: {len(validated_insider)}")

            # Only return if we have minimum viable tips (relaxed threshold: 2 of each to match Reddit validation)
            if len(validated_safety) >= 2 and len(validated_insider) >= 2:
                return {
                    "safety": validated_safety,
                    "insider": validated_insider
                }
            else:
                logger.warning(f"Insufficient relevant tips after validation for {attraction_name}: "
                             f"Safety: {len(validated_safety)}, Insider: {len(validated_insider)} (need at least 2 of each)")
                return None
                
        except Exception as e:
            logger.error(f"Error validating tips relevance: {e}")
            return None
