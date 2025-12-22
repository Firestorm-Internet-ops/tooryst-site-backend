"""Shared dependencies for API endpoints."""
import os
from fastapi import Header, HTTPException


async def verify_admin_key(x_admin_key: str = Header(..., alias="X-Admin-Key")):
    """Verify admin key for protected endpoints.

    Args:
        x_admin_key: Admin key from X-Admin-Key header

    Raises:
        HTTPException: 403 if admin key is invalid

    Returns:
        bool: True if key is valid
    """
    expected_key = os.getenv("ADMIN_KEY")

    if not expected_key:
        raise HTTPException(
            status_code=500,
            detail="Admin key not configured on server"
        )

    if x_admin_key != expected_key:
        raise HTTPException(
            status_code=403,
            detail="Invalid admin key"
        )

    return True
