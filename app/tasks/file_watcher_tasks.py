"""Celery tasks for file watching and triggering pipeline on Excel updates."""
import os
import logging
import redis
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo, available_timezones

from app.celery_app import celery_app
from app.core.notifications import notification_manager, AlertType, AlertSeverity
from app.infrastructure.external_apis.nearby_attractions_fetcher import NearbyAttractionsFetcherImpl
from app.infrastructure.persistence.storage_functions import store_nearby_attractions

# Module-level logger for file watcher
logger = logging.getLogger(__name__)


def setup_import_logging() -> logging.Logger:
    """Setup individual logging for Excel import operations."""
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    # Generate unique log file name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"import_{timestamp}.log"

    # Create logger
    logger = logging.getLogger(f'import_{timestamp}')
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler only
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Prevent propagation - logs ONLY go to import file, not Celery worker
    logger.propagate = False

    logger.info(f"Import logging initialized. Log file: {log_file}")

    # Log to Celery worker just once to show where to find detailed logs
    celery_logger = logging.getLogger(__name__)
    celery_logger.info(f"📋 Import logs: {log_file}")

    return logger


def clean_value(value):
    """Convert pandas nan/NaN to None for SQL compatibility."""
    import pandas as pd
    if pd.isna(value):
        return None
    return value


def slugify(value: str) -> str:
    import re
    return re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')


def get_timezone_from_offset(utc_offset_minutes: int) -> str:
    """Determine timezone from UTC offset in minutes.
    
    Uses zoneinfo to find a valid timezone that matches the offset.
    Falls back to UTC if no match found.
    
    Args:
        utc_offset_minutes: UTC offset in minutes (e.g., 60 for UTC+1)
    
    Returns:
        Timezone string (e.g., 'Europe/Paris')
    """
    from datetime import datetime, timezone, timedelta
    
    if utc_offset_minutes is None:
        return 'UTC'
    
    # Create a target offset
    target_offset = timedelta(minutes=utc_offset_minutes)
    target_tz = timezone(target_offset)
    
    # Try to find a matching timezone from available timezones
    now = datetime.now()
    for tz_name in available_timezones():
        try:
            tz = ZoneInfo(tz_name)
            # Get the offset for this timezone at the current time
            dt = now.replace(tzinfo=tz)
            if dt.utcoffset() == target_offset:
                return tz_name
        except Exception:
            continue
    
    # Fallback: return UTC if no match found
    logger.warning(f"No timezone found for UTC offset {utc_offset_minutes} minutes, using UTC")
    return 'UTC'


@celery_app.task(name="app.tasks.file_watcher_tasks.process_excel_update")
def process_excel_update(file_path: str):
    """Process Excel file update and trigger pipeline for new attractions.

    This task:
    1. Reads the Excel file
    2. Identifies new attractions not in database
    3. Imports new attractions
    4. Triggers full pipeline for new attractions only
    """
    # Setup separate logging
    logger = setup_import_logging()
    
    # Acquire global lock to prevent concurrent imports
    # This prevents duplicate processing if file watcher triggers multiple times
    redis_client = None
    lock_acquired = False
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=4,
            decode_responses=True
        )
        
        lock_key = "excel_import:lock"
        lock_acquired = redis_client.set(lock_key, "1", ex=300, nx=True)  # 5 minute lock
        
        if not lock_acquired:
            logger.warning("⏭️  Excel import already in progress, skipping duplicate")
            return {
                "status": "skipped",
                "reason": "import_already_in_progress"
            }
    except Exception as e:
        logger.warning(f"Could not acquire Redis lock: {e}, proceeding anyway")

    try:
        logger.info("="*80)
        logger.info("EXCEL IMPORT STARTED")
        logger.info("="*80)
        logger.info(f"📖 Reading Excel file: {file_path}")
        
        import pandas as pd
        import asyncio
        from datetime import datetime
        from app.infrastructure.persistence.db import SessionLocal
        from app.infrastructure.persistence import models
        from app.infrastructure.external_apis.google_places_client import GooglePlacesClient
        
        # Read Excel file
        df = pd.read_excel(file_path)
        logger.info(f"✓ Read {len(df)} rows from Excel file")

        # Capture first-seen city coordinates (lat/lng) per city slug
        city_coords = {}
        for idx, row in df.iterrows():
            city_name = row.get('city_name') or row.get('city')
            if not city_name:
                continue
            city_slug = slugify(city_name)
            if city_slug not in city_coords:
                city_coords[city_slug] = {
                    "lat": clean_value(row.get('lat')),
                    "lng": clean_value(row.get('lng'))
                }
        
        # Get existing attraction slugs from database
        logger.info("🔍 Checking database for existing attractions...")
        session = SessionLocal()
        try:
            existing_slugs = set(
                slug for (slug,) in session.query(models.Attraction.slug).all()
            )
            logger.info(f"✓ Found {len(existing_slugs)} existing attractions in database")
        finally:
            session.close()
        
        # Find new attractions
        logger.info("🔍 Comparing Excel with database...")
        logger.info(f"Excel columns: {df.columns.tolist()}")
        
        new_attractions = []
        for idx, row in df.iterrows():
            # Handle different column names
            attraction_name = row.get('attraction_name') or row.get('name')
            city_name = row.get('city_name') or row.get('city')
            
            if not attraction_name:
                logger.warning(f"Row {idx} has no attraction name, skipping")
                continue
            
            # Generate slug from attraction name
            slug = slugify(attraction_name)
            
            # Debug first row
            if idx == 0:
                logger.info(f"First row: name={attraction_name}, city={city_name}, slug={slug}")
            
            if slug and slug not in existing_slugs:
                new_attractions.append({
                    'slug': slug,
                    'name': attraction_name,
                    'city': city_name,
                    'country': row.get('country', 'Unknown')
                })
                logger.info(f"  New: {attraction_name} -> {slug}")
        
        if not new_attractions:
            logger.info("✓ No new attractions found - database is up to date")
            logger.info("="*80)
            return {
                "status": "success",
                "new_attractions": 0,
                "message": "No new attractions to process"
            }
        
        logger.info(f"✓ Found {len(new_attractions)} new attractions:")
        for attr in new_attractions:
            logger.info(f"  • {attr['name']} ({attr['city']}, {attr['country']})")
        
        # Import new attractions directly
        logger.info("➕ Importing new attractions to database...")
        logger.info("🔍 Fetching Google Place IDs...")
        
        # Initialize Google Places client
        places_client = GooglePlacesClient()
        
        # Fetch place IDs and timezone for all new attractions
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        for attr in new_attractions:
            row = df[df['attraction_name'] == attr['name']].iloc[0]
            lat = row.get('lat')
            lng = row.get('lng')
            
            # Use resolved_name if available, otherwise use attraction name
            search_name = clean_value(row.get('resolved_name')) or attr['name']
            
            # Build search query
            query = f"{search_name} {attr['city']}"
            
            try:
                result = loop.run_until_complete(
                    places_client.find_place(
                        query=query,
                        latitude=lat if lat else None,
                        longitude=lng if lng else None
                    )
                )
                
                if result and result.get('place_id'):
                    place_id = result['place_id']
                    attr['place_id'] = place_id
                    if search_name != attr['name']:
                        logger.info(f"  ✓ Found Place ID for {attr['name']} (using resolved_name: {search_name}): {place_id}")
                    else:
                        logger.info(f"  ✓ Found Place ID for {attr['name']}: {place_id}")
                    
                    # Fetch place details to get timezone
                    try:
                        details = loop.run_until_complete(
                            places_client.get_place_details(place_id)
                        )
                        
                        if details:
                            # Try to get timezone from API response
                            timezone_data = details.get('timeZone')
                            timezone_str = None
                            
                            # Handle both string and dict formats from API
                            if isinstance(timezone_data, dict):
                                # API returns {'id': 'Europe/Amsterdam'}
                                timezone_str = timezone_data.get('id')
                            elif isinstance(timezone_data, str):
                                # API returns 'Europe/Amsterdam'
                                timezone_str = timezone_data
                            
                            if timezone_str:
                                # Validate timezone using zoneinfo
                                try:
                                    ZoneInfo(timezone_str)
                                    attr['timezone'] = timezone_str
                                    if search_name != attr['name']:
                                        logger.info(f"  ✓ Found timezone for {attr['name']} (using resolved_name: {search_name}): {timezone_str}")
                                    else:
                                        logger.info(f"  ✓ Found timezone for {attr['name']}: {timezone_str}")
                                except Exception as tz_err:
                                    logger.warning(f"  ⚠ Invalid timezone '{timezone_str}' for {attr['name']}: {tz_err}")
                                    attr['timezone'] = 'UTC'
                            else:
                                # Fallback: try to determine from UTC offset if available
                                utc_offset = details.get('utcOffsetMinutes')
                                if utc_offset is not None:
                                    attr['timezone'] = get_timezone_from_offset(utc_offset)
                                    logger.info(f"  ✓ Determined timezone for {attr['name']}: {attr['timezone']} (from UTC offset {utc_offset})")
                                else:
                                    attr['timezone'] = 'UTC'
                                    logger.warning(f"  ⚠ No timezone info for {attr['name']}, using UTC")
                        else:
                            attr['timezone'] = 'UTC'
                            logger.warning(f"  ⚠ Could not fetch details for {attr['name']}, using UTC")
                    except Exception as detail_err:
                        attr['timezone'] = 'UTC'
                        logger.warning(f"  ⚠ Error fetching timezone for {attr['name']}: {detail_err}")
                else:
                    attr['place_id'] = None
                    attr['timezone'] = 'UTC'
                    logger.warning(f"  ⚠ No Place ID found for {attr['name']}")
            except Exception as e:
                attr['place_id'] = None
                attr['timezone'] = 'UTC'
                logger.warning(f"  ⚠ Error fetching Place ID for {attr['name']}: {e}")
        
        loop.close()
        
        # Now import to database
        session = SessionLocal()
        affected_city_ids = set()
        imported_count = 0
        updated_count = 0
        skipped_count = 0
        
        try:
            for attr in new_attractions:
                # Generate city slug
                city_slug = slugify(attr['city'])
                
                # Find the row in dataframe for this attraction
                row = df[df['attraction_name'] == attr['name']].iloc[0]
                
                # Get or create city (with proper exception handling for concurrent inserts)
                city = session.query(models.City).filter_by(
                    slug=city_slug
                ).first()
                
                if not city:
                    now = datetime.utcnow()
                    # Use first-seen city coords from Excel if available
                    city_lat = city_coords.get(city_slug, {}).get("lat")
                    city_lng = city_coords.get(city_slug, {}).get("lng")
                    
                    try:
                        city = models.City(
                            slug=city_slug,
                            name=attr['city'],
                            country=attr['country'],
                            latitude=city_lat,
                            longitude=city_lng,
                            timezone=attr.get('timezone', 'UTC'),
                            created_at=now,
                            updated_at=now
                        )
                        session.add(city)
                        session.flush()
                        logger.info(f"  ✓ Created city: {attr['city']} (lat={city_lat}, lng={city_lng}, tz={attr.get('timezone', 'UTC')})")
                    except Exception as e:
                        # Another worker may have created the city concurrently
                        # Rollback and re-fetch
                        session.rollback()
                        city = session.query(models.City).filter_by(
                            slug=city_slug
                        ).first()
                        if city:
                            logger.info(f"  ✓ City {attr['city']} was created by another worker, using existing")
                        else:
                            # Check if it's a duplicate entry error (IntegrityError)
                            from sqlalchemy.exc import IntegrityError
                            error_str = str(e)
                            if isinstance(e, IntegrityError) or "Duplicate entry" in error_str:
                                # Try one more time to fetch after a brief moment
                                import time
                                time.sleep(0.1)  # Small delay to let other transaction commit
                                city = session.query(models.City).filter_by(
                                    slug=city_slug
                                ).first()
                                if city:
                                    logger.info(f"  ✓ City {attr['city']} was created by another worker (retry), using existing")
                                else:
                                    logger.error(f"  ✗ Failed to create/fetch city {attr['city']}: {e}")
                                    raise
                            else:
                                # Not a duplicate entry error, re-raise
                                raise
                
                # Update timezone if it's currently UTC (default) and we have a better one
                if city and (city.timezone == 'UTC' or city.timezone is None) and attr.get('timezone') and attr.get('timezone') != 'UTC':
                    city.timezone = attr.get('timezone')
                    city.updated_at = datetime.utcnow()
                    logger.info(f"  ✓ Updated timezone for city {attr['city']}: {attr.get('timezone')}")
                
                # Check if attraction already exists (double-check to prevent race conditions)
                existing_attraction = session.query(models.Attraction).filter_by(
                    slug=attr['slug']
                ).first()
                
                if existing_attraction:
                    # Update existing attraction instead of creating new one
                    logger.info(f"  ⚠ Attraction {attr['name']} already exists, updating...")
                    existing_attraction.name = attr['name']
                    existing_attraction.city_id = city.id
                    if attr.get('place_id'):
                        existing_attraction.place_id = attr.get('place_id')
                    if row.get('lat'):
                        existing_attraction.latitude = row.get('lat')
                    if row.get('lng'):
                        existing_attraction.longitude = row.get('lng')
                    existing_attraction.updated_at = datetime.utcnow()
                    updated_count += 1
                    place_id_status = f"(Place ID: {attr.get('place_id')})" if attr.get('place_id') else "(No Place ID)"
                    logger.info(f"  ✓ Updated: {attr['name']} {place_id_status}")
                    affected_city_ids.add(city.id)
                    continue
                
                # Create new attraction with place_id
                now = datetime.utcnow()
                resolved_name = clean_value(row.get('resolved_name'))
                address = clean_value(row.get('address'))
                
                attraction = models.Attraction(
                    slug=attr['slug'],
                    name=attr['name'],
                    resolved_name=resolved_name,
                    address=address,
                    city_id=city.id,
                    place_id=attr.get('place_id'),
                    latitude=row.get('lat'),
                    longitude=row.get('lng'),
                    created_at=now,
                    updated_at=now
                )
                session.add(attraction)
                session.flush()
                affected_city_ids.add(city.id)
                
                # Store widgets if present (clean nan values)
                widget_primary = clean_value(row.get('widget_primary'))
                widget_secondary = clean_value(row.get('widget_secondary'))
                
                if widget_primary is not None or widget_secondary is not None:
                    widget = models.WidgetConfig(
                        attraction_id=attraction.id,
                        widget_primary=widget_primary,
                        widget_secondary=widget_secondary,
                        created_at=datetime.utcnow()
                    )
                    session.add(widget)
                
                imported_count += 1
                place_id_status = f"(Place ID: {attr.get('place_id')})" if attr.get('place_id') else "(No Place ID)"
                logger.info(f"  ✓ Imported: {attr['name']} {place_id_status}")
            
            session.commit()
            logger.info(f"✓ Successfully processed {len(new_attractions)} attractions:")
            logger.info(f"  • Imported: {imported_count}")
            logger.info(f"  • Updated: {updated_count}")
            if skipped_count > 0:
                logger.info(f"  • Skipped: {skipped_count}")

            # Trigger pipeline for new attractions BEFORE refreshing nearby
            # This ensures pipeline starts even if nearby refresh is slow
            logger.info("🚀 Triggering parallel pipeline for new attractions...")
            from app.tasks.parallel_pipeline_tasks import orchestrate_pipeline

            new_slugs = [a['slug'] for a in new_attractions]
            pipeline_result = orchestrate_pipeline.delay(new_slugs)

            logger.info(f"✓ Parallel pipeline triggered with task ID: {pipeline_result.id}")
            logger.info(f"⚡ Processing {len(new_slugs)} attractions with staged parallelism")
            logger.info("="*80)
            logger.info("PIPELINE INITIALIZATION COMPLETE")
            logger.info("="*80)

            # Refresh nearby attractions for all attractions in affected cities
            # This is done AFTER pipeline trigger to avoid blocking pipeline start
            if affected_city_ids:
                logger.info(f"🔄 Refreshing nearby attractions for cities: {affected_city_ids}")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                fetcher = NearbyAttractionsFetcherImpl()
                refreshed = 0
                try:
                    city_attractions = session.query(models.Attraction).filter(
                        models.Attraction.city_id.in_(affected_city_ids)
                    ).all()
                    for attraction in city_attractions:
                        if attraction.latitude is None or attraction.longitude is None:
                            logger.warning(f"Skipping nearby refresh for {attraction.name} (missing coords)")
                            continue
                        city_name = None
                        if hasattr(attraction, "city") and attraction.city:
                            city_name = attraction.city.name
                        if not city_name:
                            city_obj = session.query(models.City).filter_by(id=attraction.city_id).first()
                            city_name = city_obj.name if city_obj else "Unknown City"
                        try:
                            result = loop.run_until_complete(
                                fetcher.fetch(
                                    attraction_id=attraction.id,
                                    attraction_name=attraction.name,
                                    city_name=city_name,
                                    latitude=float(attraction.latitude),
                                    longitude=float(attraction.longitude),
                                    place_id=attraction.place_id
                                )
                            )
                            if result and result.get("nearby"):
                                store_nearby_attractions(attraction.id, result["nearby"])
                                refreshed += 1
                        except Exception as e:
                            logger.warning(f"Nearby refresh failed for {attraction.name}: {e}")
                finally:
                    loop.close()
                logger.info(f"✓ Nearby refresh completed for {refreshed} attractions across {len(affected_city_ids)} cities")
            else:
                logger.info("No affected cities for nearby refresh")
            
        except Exception as e:
            session.rollback()
            logger.error(f"✗ Failed to import attractions: {e}")
            import traceback
            stack_trace = traceback.format_exc()
            logger.error(stack_trace)
            
            # Send notification for import failure
            notification_manager.send_alert(
                alert_type=AlertType.PIPELINE_FAILED,
                severity=AlertSeverity.ERROR,
                title="Excel Import Failed",
                message=f"Failed to import attractions from Excel file.\n\nFile: {file_path}\n\nError: {str(e)}\n\nStack trace:\n{stack_trace}",
                metadata={
                    "file_path": file_path,
                    "error_type": type(e).__name__,
                    "new_attractions_count": len(new_attractions)
                }
            )
            
            logger.info("="*80)
            return {
                "status": "error",
                "error": f"Failed to import new attractions: {str(e)}"
            }
        finally:
            session.close()
        
        return {
            "status": "success",
            "new_attractions": len(new_attractions),
            "attraction_names": [a['name'] for a in new_attractions],
            "pipeline_task_id": pipeline_result.id
        }
        
    except Exception as e:
        logger.error("="*80)
        logger.error(f"✗ PIPELINE ERROR: {e}")
        logger.error("="*80)
        import traceback
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        
        # Send notification for pipeline initialization failure
        notification_manager.send_alert(
            alert_type=AlertType.PIPELINE_FAILED,
            severity=AlertSeverity.CRITICAL,
            title="Pipeline Initialization Failed",
            message=f"Failed to initialize pipeline from Excel file.\n\nFile: {file_path}\n\nError: {str(e)}\n\nStack trace:\n{stack_trace}",
            metadata={
                "file_path": file_path,
                "error_type": type(e).__name__
            }
        )
        
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        # Release the lock
        if redis_client and lock_acquired:
            try:
                redis_client.delete("excel_import:lock")
            except Exception as e:
                logger.warning(f"Could not release Redis lock: {e}")


def start_file_watcher():
    """Start watching the Excel file for changes.
    
    This should be run as a separate process alongside Celery worker.
    Uses Redis for global debounce (survives process restarts).
    """
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    from pathlib import Path
    import time
    import hashlib
    
    # Get absolute path to data directory
    project_root = Path(__file__).parent.parent.parent
    watch_dir = str(project_root / "data")
    file_pattern = os.getenv("INPUT_FILE_PATTERN", "attractions.xlsx")
    debounce_seconds = float(os.getenv("MONITOR_DEBOUNCE_SECONDS", "3.0"))
    
    # Redis connection for global debounce
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=4,  # Use separate DB for file watcher debounce
            decode_responses=True
        )
        redis_client.ping()
        logger.info("✓ File watcher connected to Redis for global debounce")
    except Exception as e:
        logger.warning(f"⚠ Redis not available for file watcher debounce: {e}")
        redis_client = None
    
    class ExcelFileHandler(FileSystemEventHandler):
        def __init__(self, redis_client_ref):
            self.last_modified = {}  # Fallback in-memory debounce
            self.redis_client = redis_client_ref
        
        def on_modified(self, event):
            if event.is_directory:
                return
            
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            
            # Check if it matches our pattern
            if file_name != file_pattern:
                return
            
            # Try Redis debounce first (global)
            if self.redis_client:
                try:
                    debounce_key = f"file_watcher:debounce:{file_path}"
                    
                    # Use SET NX (set if not exists) with TTL for atomic operation
                    # This prevents race conditions
                    result = self.redis_client.set(
                        debounce_key, 
                        "1", 
                        ex=int(debounce_seconds),
                        nx=True  # Only set if key doesn't exist
                    )
                    
                    if not result:
                        # Key already exists, we're within debounce window
                        logger.debug(f"Debounced (Redis): {file_name} - within {debounce_seconds}s window")
                        return
                    
                except Exception as e:
                    logger.warning(f"Redis debounce failed, falling back to in-memory: {e}")
                    self.redis_client = None
            
            # Fallback: in-memory debounce
            if not self.redis_client:
                now = time.time()
                last_mod = self.last_modified.get(file_path, 0)
                
                if now - last_mod < debounce_seconds:
                    logger.debug(f"Debounced (in-memory): {file_name} - within {debounce_seconds}s window")
                    return
                
                self.last_modified[file_path] = now
            
            logger.info(f"✓ Detected change in {file_name}, triggering import...")
            
            # Trigger Celery task
            process_excel_update.delay(file_path)
    
    # Create observer
    event_handler = ExcelFileHandler(redis_client)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=False)
    
    logger.info(f"Starting file watcher on {watch_dir} for {file_pattern}")
    logger.info(f"Debounce window: {debounce_seconds} seconds")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("File watcher stopped")
    
    observer.join()


if __name__ == "__main__":
    # Run file watcher
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_file_watcher()
