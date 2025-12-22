"""Sitemap generation for SEO using XSLT stylesheets."""
import logging
from fastapi import APIRouter, Depends
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.infrastructure.persistence.db import get_db
from app.services.sitemap_generator import SitemapGenerator
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(tags=["sitemap"])




@router.get("/sitemap_index.xml")
async def generate_sitemap_index():
    """Generate sitemap index with XSLT styling."""
    generator = SitemapGenerator()

    try:
        sitemaps = generator.get_sitemap_list()
        sitemap_index_xml = generator.generate_sitemap_index_xml(sitemaps)

        logger.info(f"Generated sitemap index with {len(sitemaps)} sitemaps")

        return Response(
            content=sitemap_index_xml,
            media_type="application/xml",
            headers={"Cache-Control": f"public, max-age={settings.SITEMAP_INDEX_CACHE_TTL}"}
        )

    except Exception as e:
        logger.error(f"Error generating sitemap index: {e}")

        # Return minimal fallback index
        fallback_sitemaps = [{
            "loc": f"{settings.SITE_URL}/api/v1/sitemap-static.xml",
            "lastmod": generator.format_date(None)
        }]
        fallback_index = generator.generate_sitemap_index_xml(fallback_sitemaps)

        return Response(
            content=fallback_index,
            media_type="application/xml",
            headers={"Cache-Control": "public, max-age=300"}
        )


@router.get("/sitemap-static.xml")
async def generate_static_sitemap():
    """Generate sitemap for static pages with XSLT styling."""
    generator = SitemapGenerator()

    try:
        static_urls = generator.generate_static_urls()
        sitemap_xml = generator.generate_sitemap_xml(static_urls, include_images=True)

        logger.info(f"Generated static sitemap with {len(static_urls)} pages")

        return Response(
            content=sitemap_xml,
            media_type="application/xml",
            headers={"Cache-Control": f"public, max-age={settings.SITEMAP_CACHE_TTL}"}
        )

    except Exception as e:
        logger.error(f"Error generating static sitemap: {e}")

        # Return minimal fallback sitemap
        fallback_urls = [generator.create_url_entry(generator.static_pages[0])]
        fallback_xml = generator.generate_sitemap_xml(fallback_urls, include_images=False)

        return Response(
            content=fallback_xml,
            media_type="application/xml",
            headers={"Cache-Control": "public, max-age=300"}
        )


@router.get("/sitemap-cities.xml")
async def generate_cities_sitemap(db: Session = Depends(get_db)):
    """Generate sitemap for city pages with XSLT styling."""
    generator = SitemapGenerator()

    try:
        city_urls = generator.generate_city_urls(db)
        sitemap_xml = generator.generate_sitemap_xml(city_urls, include_images=False)

        logger.info(f"Generated cities sitemap with {len(city_urls)} cities")

        return Response(
            content=sitemap_xml,
            media_type="application/xml",
            headers={"Cache-Control": f"public, max-age={settings.SITEMAP_CACHE_TTL}"}
        )

    except Exception as e:
        logger.error(f"Error generating cities sitemap: {e}")

        # Return minimal fallback
        fallback_xml = generator.generate_sitemap_xml([], include_images=False)
        return Response(
            content=fallback_xml,
            media_type="application/xml",
            headers={"Cache-Control": "public, max-age=300"},
            status_code=200
        )


@router.get("/sitemap-attractions.xml")
async def generate_attractions_sitemap(db: Session = Depends(get_db)):
    """Generate sitemap for attraction pages with XSLT styling."""
    generator = SitemapGenerator()

    try:
        attraction_urls = generator.generate_attraction_urls(db)
        sitemap_xml = generator.generate_sitemap_xml(attraction_urls, include_images=True)

        logger.info(f"Generated attractions sitemap with {len(attraction_urls)} attractions")

        return Response(
            content=sitemap_xml,
            media_type="application/xml",
            headers={"Cache-Control": f"public, max-age={settings.SITEMAP_CACHE_TTL}"}
        )

    except Exception as e:
        logger.error(f"Error generating attractions sitemap: {e}")

        # Return minimal fallback
        fallback_xml = generator.generate_sitemap_xml([], include_images=False)
        return Response(
            content=fallback_xml,
            media_type="application/xml",
            headers={"Cache-Control": "public, max-age=300"},
            status_code=200
        )


@router.get("/sitemap-combined.xml")
async def generate_combined_sitemap(db: Session = Depends(get_db)):
    """Generate combined sitemap with all URL types (static + cities + attractions)."""
    generator = SitemapGenerator()

    try:
        # Generate all URL types
        static_urls = generator.generate_static_urls()
        city_urls = generator.generate_city_urls(db)
        attraction_urls = generator.generate_attraction_urls(db)

        # Combine all URLs
        all_urls = static_urls + city_urls + attraction_urls

        # Generate comprehensive sitemap XML
        sitemap_xml = generator.generate_sitemap_xml(all_urls, include_images=True)

        logger.info(f"Generated combined sitemap with {len(all_urls)} URLs "
                   f"(static: {len(static_urls)}, cities: {len(city_urls)}, "
                   f"attractions: {len(attraction_urls)})")

        return Response(
            content=sitemap_xml,
            media_type="application/xml",
            headers={"Cache-Control": f"public, max-age={settings.SITEMAP_CACHE_TTL}"}
        )

    except Exception as e:
        logger.error(f"Error generating combined sitemap: {e}")

        # Return minimal fallback with just static pages
        try:
            fallback_urls = generator.generate_static_urls()
            fallback_xml = generator.generate_sitemap_xml(fallback_urls, include_images=True)
        except:
            fallback_xml = generator.generate_sitemap_xml([], include_images=False)

        return Response(
            content=fallback_xml,
            media_type="application/xml",
            headers={"Cache-Control": "public, max-age=300"},
            status_code=200
        )