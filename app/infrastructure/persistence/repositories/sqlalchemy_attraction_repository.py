"""SQLAlchemy implementation of AttractionRepository."""
from typing import Optional, List
from sqlalchemy.orm import Session
from app.domain.entities.attraction import Attraction as AttractionEntity
from app.domain.repositories.attraction_repository import AttractionRepository
from app.domain.value_objects.coordinates import Coordinates
from app.infrastructure.persistence import models


def _to_entity(row: models.Attraction) -> AttractionEntity:
    """Map ORM model to domain entity."""
    coords = Coordinates(
        latitude=float(row.latitude) if row.latitude is not None else 0.0,
        longitude=float(row.longitude) if row.longitude is not None else 0.0,
    )
    return AttractionEntity(
        id=row.id,
        city_id=row.city_id,
        name=row.name,
        slug=row.slug,
        coordinates=coords,
        place_id=row.place_id,
        rating=float(row.rating) if row.rating is not None else None,
        review_count=row.review_count,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class SQLAlchemyAttractionRepository(AttractionRepository):
    """Attraction repository using SQLAlchemy."""

    def __init__(self, session: Session):
        self.session = session

    async def get_by_id(self, attraction_id: int) -> Optional[AttractionEntity]:
        row = self.session.get(models.Attraction, attraction_id)
        return _to_entity(row) if row else None

    async def get_by_slug(self, slug: str) -> Optional[AttractionEntity]:
        row = (
            self.session.query(models.Attraction)
            .filter(models.Attraction.slug == slug)
            .first()
        )
        return _to_entity(row) if row else None

    async def create(self, attraction: AttractionEntity) -> AttractionEntity:
        row = models.Attraction(
            id=attraction.id,
            city_id=attraction.city_id,
            slug=attraction.slug,
            name=attraction.name,
            place_id=attraction.place_id,
            rating=attraction.rating,
            review_count=attraction.review_count,
            latitude=attraction.coordinates.latitude,
            longitude=attraction.coordinates.longitude,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return _to_entity(row)

    async def update(self, attraction: AttractionEntity) -> AttractionEntity:
        row = self.session.get(models.Attraction, attraction.id)
        if not row:
            raise ValueError("Attraction not found")
        row.city_id = attraction.city_id
        row.slug = attraction.slug
        row.name = attraction.name
        row.place_id = attraction.place_id
        row.rating = attraction.rating
        row.review_count = attraction.review_count
        row.latitude = attraction.coordinates.latitude
        row.longitude = attraction.coordinates.longitude
        self.session.commit()
        self.session.refresh(row)
        return _to_entity(row)

    async def list_active(self, skip: int = 0, limit: int = 100) -> List[AttractionEntity]:
        rows = (
            self.session.query(models.Attraction)
            .offset(skip)
            .limit(limit)
            .all()
        )
        return [_to_entity(r) for r in rows]

    async def count_active(self) -> int:
        return (
            self.session.query(models.Attraction)
            .count()
        )


