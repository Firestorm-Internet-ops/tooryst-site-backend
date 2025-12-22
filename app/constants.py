"""Application constants that never change across environments.

These are true constants representing physical facts, mathematical formulas,
or fixed business logic that should never vary between dev/staging/prod.
"""

# ===== Geographic Constants =====
EARTH_RADIUS_KM = 6371  # Earth's radius in kilometers (for Haversine formula)

# ===== Time Constants =====
SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
DAYS_PER_WEEK = 7

# ===== Enum Values =====
TIP_TYPE_SAFETY = "SAFETY"
TIP_TYPE_INSIDER = "INSIDER"
TIP_TYPE_GENERAL = "GENERAL"

# ===== Data Source Identifiers =====
SOURCE_GOOGLE_PLACES = "google_places_api"
SOURCE_OPENWEATHER = "openweathermap_api"
SOURCE_BESTTIME = "besttime_api"
SOURCE_YOUTUBE = "youtube_api"
SOURCE_REDDIT = "reddit_api"
SOURCE_GEMINI_FALLBACK = "gemini_fallback"

# ===== Alert Severity Levels =====
SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_WARNING = "WARNING"
SEVERITY_INFO = "INFO"

# ===== Database Constraints =====
MAX_DISTANCE_DECIMAL_PLACES = 3  # DECIMAL(6,3) supports 3 decimal places
MAX_VARCHAR_LENGTH_SHORT = 255
MAX_VARCHAR_LENGTH_MEDIUM = 512
MAX_VARCHAR_LENGTH_LONG = 1024

# ===== HTTP Status Codes (commonly used) =====
HTTP_OK = 200
HTTP_CREATED = 201
HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404
HTTP_RATE_LIMIT = 429
HTTP_SERVER_ERROR = 500

# ===== Position/Ordering =====
FIRST_POSITION = 1
DEFAULT_POSITION = 0
