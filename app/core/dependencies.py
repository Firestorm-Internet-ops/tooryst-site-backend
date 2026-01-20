"""Dependency injection for FastAPI routes.
Follows Dependency Inversion Principle - routes depend on abstractions."""
import os
from functools import lru_cache
from app.domain.repositories.attraction_repository import AttractionRepository
from app.domain.repositories.city_repository import CityRepository
from app.infrastructure.persistence.repositories.in_memory_attraction_repository import (
    InMemoryAttractionRepository,
)
from app.infrastructure.persistence.repositories.in_memory_city_repository import (
    InMemoryCityRepository,
)
from app.application.use_cases.get_attraction_page import GetAttractionPageUseCase
from app.application.use_cases.get_attraction_sections import GetAttractionSectionsUseCase
from app.application.services.attraction_data_service import AttractionDataService
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence.repositories.sqlalchemy_attraction_repository import (
    SQLAlchemyAttractionRepository,
)
from app.infrastructure.persistence.repositories.sqlalchemy_city_repository import (
    SQLAlchemyCityRepository,
)


# If using DB repos, keep a shared session for repo initialization
from app.config import settings
_db_session = SessionLocal() if settings.USE_SQL_REPOSITORIES else None


def get_attraction_repository() -> AttractionRepository:
    """Get attraction repository instance.
    
    Default to SQL repositories. Uses in-memory only if explicitly disabled.
    """
    if settings.USE_SQL_REPOSITORIES:
        return SQLAlchemyAttractionRepository(_db_session)
    return InMemoryAttractionRepository()


def get_city_repository() -> CityRepository:
    """Get city repository instance."""
    if settings.USE_SQL_REPOSITORIES:
        return SQLAlchemyCityRepository(_db_session)
    return InMemoryCityRepository()


def get_sqlalchemy_repositories():
    """Factory to get SQLAlchemy repositories (manual use)."""
    session = SessionLocal()
    return {
        "session": session,
        "attraction_repo": SQLAlchemyAttractionRepository(session),
        "city_repo": SQLAlchemyCityRepository(session),
    }


# Use case instances - Fresh instances each time to ensure consistency
def get_attraction_page_use_case() -> GetAttractionPageUseCase:
    """Get attraction page use case."""
    return GetAttractionPageUseCase(
        attraction_repository=get_attraction_repository(),
        city_repository=get_city_repository(),
        data_service=AttractionDataService(),
    )


def get_attraction_sections_use_case() -> GetAttractionSectionsUseCase:
    """Get attraction sections use case."""
    return GetAttractionSectionsUseCase(
        attraction_repository=get_attraction_repository(),
        city_repository=get_city_repository(),
        data_service=AttractionDataService(),
    )

