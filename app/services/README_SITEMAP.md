# Storyboard Sitemap System

## Overview

The Storyboard sitemap system provides comprehensive XML sitemaps for SEO optimization, following Google's sitemap standards and best practices. The system generates dynamic sitemaps for static pages, cities, and attractions with proper caching, error handling, and human-readable XSLT styling.

## Architecture

### Core Components

1. **SitemapGenerator** (`app/services/sitemap_generator.py`)
   - Unified sitemap generation following reference architecture
   - Environment-configurable base URLs
   - Static page definitions with SEO metadata
   - Dynamic URL generation for database entities
   - XML generation with proper escaping and validation
   - Image sitemap support for enhanced SEO

2. **Sitemap Routes** (`app/api/v1/routes/sitemap.py`)
   - FastAPI endpoints with proper async database session management
   - Consistent error handling and fallback responses
   - Configurable caching headers
   - Dependency injection for database sessions

3. **Frontend Proxy Routes** (`client/src/app/sitemap*.xml/`)
   - Next.js API routes for serving sitemaps
   - Proper caching strategies
   - Fallback responses for error conditions

## Available Endpoints

### Backend (API)
- `/api/v1/sitemap_index.xml` - Sitemap index listing all available sitemaps
- `/api/v1/sitemap-static.xml` - Static pages (homepage, about, contact, etc.)
- `/api/v1/sitemap-cities.xml` - Dynamic city pages
- `/api/v1/sitemap-attractions.xml` - Dynamic attraction pages with images

### Frontend (Public)
- `/sitemap_index.xml` - Sitemap index (proxied from backend)
- `/sitemap.xml` - Main sitemap with static pages (proxied from backend)

## Configuration

### Environment Variables

```env
# Sitemap Configuration
SITE_URL=https://storyboard.com          # Base URL for all sitemap URLs
API_BASE_URL=http://localhost:8000       # Backend API base URL
SITEMAP_CACHE_TTL=3600                   # Cache TTL for sitemaps (seconds)
SITEMAP_INDEX_CACHE_TTL=7200            # Cache TTL for sitemap index (seconds)
```

### Static Pages Configuration

Static pages are defined in `SitemapGenerator._define_static_pages()`:

```python
StaticPage(
    path="/",
    priority="1.0",           # SEO priority (0.0-1.0)
    changefreq="daily",       # Update frequency
    title="Page Title",       # Optional: page title
    description="Page desc",  # Optional: page description
    image="/logo.png"         # Optional: page image
)
```

## Features

### SEO Optimization
- **Priority Levels**: Pages ranked by importance (1.0 = highest)
- **Change Frequencies**: Appropriate update frequencies for different content types
- **Last Modified**: Accurate timestamps for content freshness
- **Image Sitemaps**: Google's image sitemap schema for better image discovery

### Caching Strategy
- **Static Pages**: 1 hour cache (configurable)
- **Dynamic Pages**: 1 hour cache (configurable)
- **Sitemap Index**: 2 hours cache (configurable)
- **Error Responses**: 5 minutes cache

### Error Handling
- **Graceful Degradation**: Always returns valid XML even on database errors
- **Fallback Responses**: Minimal sitemaps with essential pages
- **Consistent Status Codes**: Returns 200 OK even for partial failures
- **Comprehensive Logging**: Detailed error logging for debugging

### Human-Readable Display
- **XSLT Stylesheets**: Beautiful web interface when viewed in browsers
- **Statistics Dashboard**: URL counts and last update information
- **Interactive Tables**: Sortable and filterable sitemap contents
- **Responsive Design**: Works on all device sizes

## Usage

### Adding New Static Pages

1. Update `SitemapGenerator._define_static_pages()`:
```python
StaticPage(
    path="/new-page",
    priority="0.6",
    changefreq="monthly",
    title="New Page Title",
    description="Page description for SEO"
)
```

### Adding Dynamic Content Types

1. Create URL generation method in `SitemapGenerator`:
```python
def generate_new_type_urls(self, db: Session) -> List[SitemapUrl]:
    # Query database and generate URLs
    pass
```

2. Add new endpoint in `sitemap.py`:
```python
@router.get("/sitemap-newtype.xml")
async def generate_newtype_sitemap(db: Session = Depends(get_db)):
    # Implementation
    pass
```

3. Update sitemap index in `SitemapGenerator.get_sitemap_list()`.

## Testing

### Manual Testing
```bash
# Test sitemap generation
curl http://localhost:8000/api/v1/sitemap_index.xml
curl http://localhost:8000/api/v1/sitemap-static.xml
curl http://localhost:8000/api/v1/sitemap-cities.xml
curl http://localhost:8000/api/v1/sitemap-attractions.xml

# Test frontend proxy
curl http://localhost:3000/sitemap_index.xml
curl http://localhost:3000/sitemap.xml
```

### Validation
- Use Google Search Console for sitemap validation
- Validate XML syntax with online XML validators
- Test XSLT display in multiple browsers

## Best Practices

1. **URL Limits**: Keep individual sitemaps under 50,000 URLs
2. **File Size**: Keep sitemap files under 50MB uncompressed
3. **Update Frequency**: Match `changefreq` with actual content update patterns
4. **Priority Values**: Use priority values strategically, not uniformly
5. **Image Optimization**: Include only high-quality, relevant images
6. **Cache Headers**: Use appropriate cache durations for different content types

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed
2. **Database Connection**: Check database configuration and connectivity
3. **URL Generation**: Verify SITE_URL environment variable is set correctly
4. **XSLT Display**: Ensure stylesheet files are accessible at correct paths
5. **Cache Issues**: Clear CDN and browser caches when testing changes

### Monitoring

- Monitor sitemap generation performance
- Track error rates in logs
- Verify search engine crawling behavior
- Check for broken URLs in sitemaps

## Performance Considerations

- Database queries are optimized with proper ordering
- Caching reduces server load and improves response times
- Fallback responses prevent service disruptions
- Async database sessions prevent connection leaks