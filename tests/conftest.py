"""
Pytest configuration and shared fixtures for backend tests.

This module provides test fixtures for:
- Database sessions (in-memory SQLite for fast tests)
- FastAPI test client
- Mock external APIs
- Test data factories
"""

import asyncio
import os
from typing import AsyncGenerator, Generator
from unittest.mock import MagicMock, AsyncMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

# Import app and models
from app.main import app
from app.infrastructure.persistence.models import Base
from app.infrastructure.persistence.db import get_db


# ==============================================================================
# DATABASE FIXTURES
# ==============================================================================

@pytest.fixture(scope="function")
def test_db_engine():
    """Create an in-memory SQLite database for testing."""
    from sqlalchemy import Integer

    # Use in-memory SQLite for fast tests
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # Enable foreign key support for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # Monkey-patch BigInteger columns to use Integer for SQLite autoincrement
    for table in Base.metadata.tables.values():
        for column in table.columns:
            if str(column.type) == 'BIGINT' and column.primary_key:
                column.type = Integer()

    # Create all tables
    Base.metadata.create_all(bind=engine)

    yield engine

    # Drop all tables after test
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def test_db_session(test_db_engine) -> Generator[Session, None, None]:
    """Create a database session for testing."""
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=test_db_engine
    )

    session = TestingSessionLocal()

    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture(scope="function")
def client(test_db_session) -> Generator[TestClient, None, None]:
    """Create a FastAPI test client with test database."""

    def override_get_db():
        try:
            yield test_db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


# ==============================================================================
# ASYNC TEST SUPPORT
# ==============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


# ==============================================================================
# MOCK EXTERNAL API FIXTURES
# ==============================================================================

@pytest.fixture
def mock_redis():
    """Mock Redis client for cache tests."""
    redis_mock = MagicMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.setex = AsyncMock(return_value=True)
    redis_mock.delete = AsyncMock(return_value=1)
    redis_mock.ping = AsyncMock(return_value=True)
    return redis_mock


@pytest.fixture
def mock_google_places_api():
    """Mock Google Places API responses."""
    return {
        "status": "OK",
        "result": {
            "name": "Test Attraction",
            "formatted_address": "123 Test St, Test City",
            "geometry": {
                "location": {"lat": 40.7128, "lng": -74.0060}
            },
            "rating": 4.5,
            "user_ratings_total": 1000,
            "photos": [
                {"photo_reference": "test_photo_ref_1"},
                {"photo_reference": "test_photo_ref_2"}
            ],
            "reviews": [
                {
                    "author_name": "Test User",
                    "rating": 5,
                    "text": "Great place!",
                    "time": 1640000000
                }
            ]
        }
    }


@pytest.fixture
def mock_youtube_api():
    """Mock YouTube API responses."""
    return {
        "items": [
            {
                "id": {"videoId": "test_video_1"},
                "snippet": {
                    "title": "Test Video 1",
                    "thumbnails": {
                        "high": {"url": "https://example.com/thumb1.jpg"}
                    },
                    "channelTitle": "Test Channel"
                },
                "statistics": {
                    "viewCount": "10000"
                },
                "contentDetails": {
                    "duration": "PT5M30S"
                }
            }
        ]
    }


@pytest.fixture
def mock_openweather_api():
    """Mock OpenWeatherMap API responses."""
    return {
        "cod": "200",
        "list": [
            {
                "dt": 1640000000,
                "main": {
                    "temp": 20.5,
                    "feels_like": 19.0,
                    "temp_min": 18.0,
                    "temp_max": 23.0,
                    "humidity": 65
                },
                "weather": [
                    {
                        "id": 800,
                        "main": "Clear",
                        "description": "clear sky",
                        "icon": "01d"
                    }
                ],
                "wind": {"speed": 3.5},
                "pop": 0.1
            }
        ]
    }


@pytest.fixture
def mock_gemini_api():
    """Mock Gemini AI API responses."""
    class MockGeminiResponse:
        text = "This is a test AI-generated response."

    return MockGeminiResponse()


# ==============================================================================
# TEST DATA FACTORIES
# ==============================================================================

@pytest.fixture
def sample_city_data():
    """Sample city data for testing."""
    return {
        "id": 1,
        "name": "Test City",
        "slug": "test-city",
        "country": "Test Country",
        "lat": 40.7128,
        "lng": -74.0060,
        "attraction_count": 10
    }


@pytest.fixture
def sample_attraction_data():
    """Sample attraction data for testing."""
    return {
        "id": 1,
        "city_id": 1,
        "name": "Test Attraction",
        "slug": "test-attraction",
        "description": "A great test attraction",
        "lat": 40.7128,
        "lng": -74.0060,
        "rating": 4.5,
        "review_count": 100,
        "is_active": True
    }


@pytest.fixture
def sample_hero_image_data():
    """Sample hero image data for testing."""
    return {
        "id": 1,
        "attraction_id": 1,
        "photo_reference": "test_photo_ref",
        "width": 1600,
        "height": 1200,
        "photo_url": "https://example.com/photo.jpg",
        "source": "google_places"
    }


@pytest.fixture
def sample_review_data():
    """Sample review data for testing."""
    return {
        "id": 1,
        "attraction_id": 1,
        "author_name": "Test User",
        "author_photo_url": "https://example.com/user.jpg",
        "rating": 5,
        "text": "Excellent attraction!",
        "time": "2024-01-01T12:00:00",
        "source": "google"
    }


# ==============================================================================
# AUTHENTICATION FIXTURES
# ==============================================================================

@pytest.fixture
def admin_headers():
    """Headers with admin API key for authenticated requests."""
    return {
        "X-Admin-Key": os.getenv("ADMIN_API_KEY", "test_admin_key")
    }


@pytest.fixture
def invalid_admin_headers():
    """Headers with invalid admin API key for testing auth failures."""
    return {
        "X-Admin-Key": "invalid_key_12345"
    }


# ==============================================================================
# UTILITY FIXTURES
# ==============================================================================

@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file for testing file operations."""
    test_file = tmp_path / "test_file.txt"
    test_file.write_text("Test content")
    return test_file


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables after each test."""
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


# ==============================================================================
# MARKERS
# ==============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "unit: Mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: Mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: Mark test as slow (may take >1 second)"
    )
    config.addinivalue_line(
        "markers", "external_api: Mark test as requiring external API calls"
    )
