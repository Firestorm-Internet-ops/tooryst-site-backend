"""Shared HTTP client with connection pooling for better performance."""
import httpx
import logging
from typing import Optional
from app.core.settings import settings

logger = logging.getLogger(__name__)

# Global shared client for connection pooling
_shared_client: Optional[httpx.AsyncClient] = None


def get_shared_client() -> httpx.AsyncClient:
    """Get or create shared HTTP client with connection pooling.
    
    Benefits:
    - Reuses TCP connections (much faster)
    - Reduces connection overhead
    - Better for high-volume API calls
    
    Connection pool settings from environment:
    - Max connections: HTTP_MAX_CONNECTIONS (default: 100)
    - Max keepalive: HTTP_MAX_KEEPALIVE (default: 50)
    - HTTP/2: HTTP_ENABLE_HTTP2 (default: true)
    - Timeout: 30 seconds
    """
    global _shared_client
    
    if _shared_client is None:
        limits = httpx.Limits(
            max_connections=settings.HTTP_MAX_CONNECTIONS,
            max_keepalive_connections=settings.HTTP_MAX_KEEPALIVE
        )
        
        _shared_client = httpx.AsyncClient(
            timeout=30.0,
            limits=limits,
            http2=settings.HTTP_ENABLE_HTTP2
        )
        
        logger.info(
            f"HTTP client initialized: max_conn={settings.HTTP_MAX_CONNECTIONS}, "
            f"keepalive={settings.HTTP_MAX_KEEPALIVE}, http2={settings.HTTP_ENABLE_HTTP2}"
        )
    
    return _shared_client


async def close_shared_client():
    """Close the shared HTTP client. Call this when shutting down."""
    global _shared_client
    
    if _shared_client is not None:
        await _shared_client.aclose()
        _shared_client = None
        logger.info("HTTP client closed")
