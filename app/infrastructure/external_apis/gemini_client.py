"""Gemini API client for AI-powered content generation."""
import os
import httpx
from typing import Optional, Dict, Any
import logging
import json

from app.config import settings

logger = logging.getLogger(__name__)


class GeminiClient:
    """Client for Google Gemini API."""
    
    BASE_URL = "https://generativelanguage.googleapis.com/v1"
    
    def __init__(
        self, 
        api_key: Optional[str] = None,
        model: Optional[str] = None
    ):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.model = model or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
        if not self.api_key:
            logger.warning("GEMINI_API_KEY not set")
    
    async def generate_json(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Generate JSON content using Gemini API.
        
        Args:
            prompt: The prompt to send to Gemini (should request JSON output)
        
        Returns:
            Parsed JSON dict or None if error
        """
        if not self.api_key:
            logger.error("Cannot call Gemini: API key missing")
            return None
        
        url = f"{self.BASE_URL}/models/{self.model}:generateContent"
        
        params = {
            "key": self.api_key
        }
        
        # Simple payload - rely on prompt to request JSON format
        payload: Dict[str, Any] = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ]
        }
        
        try:
            timeout = float(settings.GEMINI_API_TIMEOUT_SECONDS)
            async with httpx.AsyncClient(timeout=timeout) as client:
                logger.info(f"Calling Gemini model {self.model} for JSON generation")
                response = await client.post(url, params=params, json=payload)
                response.raise_for_status()
                data = response.json()
                
                # Extract text from response
                candidates = data.get("candidates", [])
                if not candidates:
                    logger.error("Gemini response has no candidates")
                    return None
                
                # Collect all text parts
                text = ""
                parts = candidates[0].get("content", {}).get("parts", [])
                for part in parts:
                    if "text" in part:
                        text += part["text"]
                
                if not text:
                    logger.error("Gemini candidate had no text content")
                    preview_length = settings.RESPONSE_TEXT_PREVIEW_LENGTH
                    logger.error(f"Full response: {json.dumps(data)[:preview_length]}")
                    return None
                
                preview_length = settings.RESPONSE_TEXT_PREVIEW_LENGTH
                logger.info(f"Gemini response text (first {preview_length} chars): {text[:preview_length]}")
                
                # Clean up text - remove markdown code blocks if present
                text = text.strip()
                if text.startswith("```json"):
                    text = text[7:]
                elif text.startswith("```"):
                    text = text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()
                
                # Parse JSON from the model's text
                try:
                    return json.loads(text)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON from Gemini response: {e}")
                    logger.error(f"Response text: {text[:500]}")
                    return None
        
        except httpx.HTTPStatusError as e:
            # Log JSON error body if present
            truncation_length = settings.ERROR_MESSAGE_TRUNCATION_LENGTH
            try:
                err = e.response.json()
                logger.error(f"HTTP error with Gemini: {e.response.status_code} - {json.dumps(err)[:truncation_length]}")
            except ValueError:
                logger.error(f"HTTP error with Gemini: {e.response.status_code} - {e.response.text[:truncation_length]}")
            return None
        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            return None
    
    async def generate_text(self, prompt: str) -> Optional[str]:
        """Generate plain text content using Gemini API.
        
        Args:
            prompt: The prompt to send to Gemini
        
        Returns:
            Generated text or None if error
        """
        if not self.api_key:
            logger.error("Cannot call Gemini: API key missing")
            return None
        
        url = f"{self.BASE_URL}/models/{self.model}:generateContent"
        
        params = {
            "key": self.api_key
        }
        
        payload: Dict[str, Any] = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ]
        }
        
        try:
            timeout = float(settings.GEMINI_API_TIMEOUT_SECONDS)
            async with httpx.AsyncClient(timeout=timeout) as client:
                logger.info(f"Calling Gemini model {self.model} for text generation")
                response = await client.post(url, params=params, json=payload)
                response.raise_for_status()
                data = response.json()
                
                # Extract text from response
                candidates = data.get("candidates", [])
                if not candidates:
                    logger.error("Gemini response has no candidates")
                    return None
                
                # Collect all text parts
                text = ""
                parts = candidates[0].get("content", {}).get("parts", [])
                for part in parts:
                    if "text" in part:
                        text += part["text"]
                
                if not text:
                    logger.error("Gemini candidate had no text content")
                    return None
                
                return text.strip()
        
        except httpx.HTTPStatusError as e:
            truncation_length = settings.ERROR_MESSAGE_TRUNCATION_LENGTH
            try:
                err = e.response.json()
                logger.error(f"HTTP error with Gemini: {e.response.status_code} - {json.dumps(err)[:truncation_length]}")
            except ValueError:
                logger.error(f"HTTP error with Gemini: {e.response.status_code} - {e.response.text[:truncation_length]}")
            return None
        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            return None
