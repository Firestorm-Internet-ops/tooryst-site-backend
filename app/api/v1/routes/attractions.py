"""Attraction API routes - thin layer delegating to use cases.
Follows Single Responsibility Principle - only handles HTTP concerns."""
from fastapi import APIRouter, HTTPException, Depends
from app.core.dependencies import (
    get_attraction_page_use_case,
    get_attraction_sections_use_case,
)
from app.application.use_cases.get_attraction_page import GetAttractionPageUseCase
from app.application.use_cases.get_attraction_sections import GetAttractionSectionsUseCase
from app.api.v1.schemas.attraction_schemas import AttractionPageResponseSchema
from app.api.v1.schemas.section_schemas import AttractionSectionsResponseSchema

router = APIRouter(tags=["attractions"])


@router.get("/attractions/{slug}/page", response_model=AttractionPageResponseSchema)
async def get_attraction_page(
    slug: str,
    use_case: GetAttractionPageUseCase = Depends(get_attraction_page_use_case),
):
    """
    Get complete attraction page data.
    
    Returns all card data needed for the storyboard page.
    """
    page_dto = await use_case.execute(slug)
    if not page_dto:
        raise HTTPException(status_code=404, detail=f"Attraction '{slug}' not found")
    
    # Convert DTO to Pydantic schema
    return AttractionPageResponseSchema(
        attraction_id=page_dto.attraction_id,
        slug=page_dto.slug,
        name=page_dto.name,
        city=page_dto.city,
        country=page_dto.country,
        timezone=page_dto.timezone,
        cards=page_dto.cards,
    )


@router.get("/attractions/{slug}/sections", response_model=AttractionSectionsResponseSchema)
async def get_attraction_sections(
    slug: str,
    use_case: GetAttractionSectionsUseCase = Depends(get_attraction_sections_use_case),
):
    """
    Get attraction sections data.
    
    Returns all sections organized by content type.
    """
    sections_dto = await use_case.execute(slug)
    if not sections_dto:
        raise HTTPException(status_code=404, detail=f"Attraction '{slug}' not found")
    
    # Convert DTO to Pydantic schema
    return AttractionSectionsResponseSchema(
        attraction_id=sections_dto.attraction_id,
        slug=sections_dto.slug,
        name=sections_dto.name,
        city=sections_dto.city,
        country=sections_dto.country,
        sections=sections_dto.sections,
    )
