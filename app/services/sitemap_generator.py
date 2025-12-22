"""Sitemap generation service following best practices."""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from sqlalchemy.orm import Session
from urllib.parse import urljoin
import xml.sax.saxutils as saxutils

from app.config import get_settings
from app.infrastructure.persistence import models

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class SitemapUrl:
    """Represents a single URL in a sitemap."""
    loc: str
    lastmod: Optional[str] = None
    changefreq: Optional[str] = None
    priority: Optional[str] = None
    images: Optional[List[str]] = None


@dataclass
class StaticPage:
    """Represents a static page configuration."""
    path: str
    priority: str
    changefreq: str
    title: Optional[str] = None
    description: Optional[str] = None
    image: Optional[str] = None


class SitemapGenerator:
    """Unified sitemap generator following reference architecture."""

    def __init__(self):
        self.site_url = settings.SITE_URL.rstrip('/')
        self.static_pages = self._define_static_pages()

    def _define_static_pages(self) -> List[StaticPage]:
        """Define static pages with SEO metadata."""
        return [
            StaticPage(
                path="/",
                priority="1.0",
                changefreq="daily",
                title="Storyboard - Discover Amazing Attractions",
                description="Beat the crowds, maximize your fun with crowd-tracking for attractions worldwide",
                image="/logo-navbar.png"
            ),
            StaticPage(
                path="/attractions",
                priority="0.9",
                changefreq="daily",
                title="Browse All Attractions",
                description="Discover attractions with real-time crowd data and visitor insights"
            ),
            StaticPage(
                path="/cities",
                priority="0.8",
                changefreq="weekly",
                title="Browse Cities",
                description="Explore attractions in cities around the world"
            ),
            StaticPage(
                path="/search",
                priority="0.7",
                changefreq="weekly",
                title="Search Attractions",
                description="Find the perfect attraction for your next visit"
            ),
            StaticPage(
                path="/about",
                priority="0.6",
                changefreq="monthly",
                title="About Storyboard",
                description="Learn more about our mission to help you avoid crowds and find the best times to visit"
            ),
            StaticPage(
                path="/contact",
                priority="0.5",
                changefreq="monthly",
                title="Contact Us",
                description="Get in touch with the Storyboard team"
            ),
            StaticPage(
                path="/faq",
                priority="0.5",
                changefreq="monthly",
                title="Frequently Asked Questions",
                description="Common questions about crowd tracking and attraction data"
            ),
            StaticPage(
                path="/privacy-policy",
                priority="0.3",
                changefreq="yearly",
                title="Privacy Policy",
                description="How we protect and handle your data"
            ),
            StaticPage(
                path="/cookie-policy",
                priority="0.3",
                changefreq="yearly",
                title="Cookie Policy",
                description="Information about our use of cookies"
            ),
        ]

    def format_date(self, date_obj: Optional[datetime]) -> str:
        """Format date for sitemap lastmod field."""
        if date_obj:
            return date_obj.strftime('%Y-%m-%d')
        return datetime.now().strftime('%Y-%m-%d')

    def escape_xml(self, text: str) -> str:
        """Escape XML special characters for security."""
        return saxutils.escape(text)

    def create_url_entry(self, page: StaticPage) -> SitemapUrl:
        """Create a sitemap URL entry from a static page."""
        return SitemapUrl(
            loc=urljoin(self.site_url, page.path),
            lastmod=self.format_date(datetime.now()),
            changefreq=page.changefreq,
            priority=page.priority,
            images=[urljoin(self.site_url, page.image)] if page.image else None
        )

    def generate_static_urls(self) -> List[SitemapUrl]:
        """Generate URLs for static pages."""
        return [self.create_url_entry(page) for page in self.static_pages]

    def generate_city_urls(self, db: Session) -> List[SitemapUrl]:
        """Generate URLs for city pages."""
        try:
            cities = db.query(models.City).order_by(models.City.name).all()
            urls = []

            for city in cities:
                url = SitemapUrl(
                    loc=urljoin(self.site_url, f"/cities/{city.slug}"),
                    lastmod=self.format_date(city.updated_at),
                    changefreq="weekly",
                    priority="0.8"
                )
                urls.append(url)

            logger.info(f"Generated {len(urls)} city URLs for sitemap")
            return urls

        except Exception as e:
            logger.error(f"Error generating city URLs: {e}")
            return []

    def generate_attraction_urls(self, db: Session) -> List[SitemapUrl]:
        """Generate URLs for attraction pages."""
        try:
            attractions = db.query(models.Attraction).order_by(models.Attraction.name).all()
            urls = []

            for attraction in attractions:
                # Get attraction images for image sitemap support
                images = []
                if hasattr(attraction, 'hero_image') and attraction.hero_image:
                    images.append(urljoin(self.site_url, attraction.hero_image))

                url = SitemapUrl(
                    loc=urljoin(self.site_url, f"/attractions/{attraction.slug}"),
                    lastmod=self.format_date(attraction.updated_at),
                    changefreq="daily",
                    priority="0.7",
                    images=images if images else None
                )
                urls.append(url)

            logger.info(f"Generated {len(urls)} attraction URLs for sitemap")
            return urls

        except Exception as e:
            logger.error(f"Error generating attraction URLs: {e}")
            return []

    def generate_sitemap_xml(self, urls: List[SitemapUrl], include_images: bool = True) -> str:
        """Generate XML sitemap from URL list."""
        xml_content = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_content.append('<?xml-stylesheet type="text/xsl" href="/sitemap.xsl"?>')

        # Add namespaces
        if include_images and any(url.images for url in urls):
            xml_content.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">')
        else:
            xml_content.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        for url in urls:
            xml_content.append('  <url>')
            xml_content.append(f'    <loc>{self.escape_xml(url.loc)}</loc>')

            if url.lastmod:
                xml_content.append(f'    <lastmod>{url.lastmod}</lastmod>')
            if url.changefreq:
                xml_content.append(f'    <changefreq>{url.changefreq}</changefreq>')
            if url.priority:
                xml_content.append(f'    <priority>{url.priority}</priority>')

            # Add image entries if present
            if include_images and url.images:
                for image_url in url.images:
                    xml_content.append('    <image:image>')
                    xml_content.append(f'      <image:loc>{self.escape_xml(image_url)}</image:loc>')
                    xml_content.append('    </image:image>')

            xml_content.append('  </url>')

        xml_content.append('</urlset>')
        return '\n'.join(xml_content)

    def generate_sitemap_index_xml(self, sitemaps: List[Dict[str, str]]) -> str:
        """Generate sitemap index XML."""
        current_date = self.format_date(datetime.now())

        xml_content = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_content.append('<?xml-stylesheet type="text/xsl" href="/sitemap-index.xsl"?>')
        xml_content.append('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        for sitemap in sitemaps:
            xml_content.append('  <sitemap>')
            xml_content.append(f'    <loc>{self.escape_xml(sitemap["loc"])}</loc>')
            xml_content.append(f'    <lastmod>{sitemap.get("lastmod", current_date)}</lastmod>')
            xml_content.append('  </sitemap>')

        xml_content.append('</sitemapindex>')
        return '\n'.join(xml_content)

    def get_sitemap_list(self) -> List[Dict[str, str]]:
        """Get list of available sitemaps for the index."""
        base_url = urljoin(self.site_url, "/api/v1/")
        current_date = self.format_date(datetime.now())

        return [
            {
                "loc": urljoin(base_url, "sitemap-static.xml"),
                "lastmod": current_date,
                "type": "static"
            },
            {
                "loc": urljoin(base_url, "sitemap-cities.xml"),
                "lastmod": current_date,
                "type": "cities"
            },
            {
                "loc": urljoin(base_url, "sitemap-attractions.xml"),
                "lastmod": current_date,
                "type": "attractions"
            }
        ]