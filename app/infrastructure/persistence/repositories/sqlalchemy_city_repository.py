"""SQLAlchemy implementation of CityRepository."""
from typing import Optional, List
from sqlalchemy.orm import Session
from app.domain.entities.city import City as CityEntity
from app.domain.repositories.city_repository import CityRepository
from app.domain.value_objects.coordinates import Coordinates
from app.infrastructure.persistence import models


def _to_entity(row: models.City) -> CityEntity:
    coords = None
    if row.latitude is not None and row.longitude is not None:
        coords = Coordinates(latitude=float(row.latitude), longitude=float(row.longitude))
    return CityEntity(
        id=row.id,
        slug=row.slug,
        name=row.name,
        country=row.country,
        coordinates=coords,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class SQLAlchemyCityRepository(CityRepository):
    """City repository using SQLAlchemy."""

    def __init__(self, session: Session):
        self.session = session

    async def get_by_id(self, city_id: int) -> Optional[CityEntity]:
        row = self.session.get(models.City, city_id)
        return _to_entity(row) if row else None

    async def get_by_slug(self, slug: str) -> Optional[CityEntity]:
        row = self.session.query(models.City).filter(models.City.slug == slug).first()
        return _to_entity(row) if row else None

    async def create(self, city: CityEntity) -> CityEntity:
        row = models.City(
            id=city.id,
            slug=city.slug,
            name=city.name,
            country=city.country,
            latitude=city.coordinates.latitude if city.coordinates else None,
            longitude=city.coordinates.longitude if city.coordinates else None,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return _to_entity(row)

    async def list_all(self, skip: int = 0, limit: int = 100) -> List[CityEntity]:
        rows = self.session.query(models.City).offset(skip).limit(limit).all()
        return [_to_entity(r) for r in rows]

    async def count_all(self) -> int:
        return self.session.query(models.City).count()


