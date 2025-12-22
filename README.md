# Toorysts Backend

Complete backend system with automatic data pipeline for Toorysts travel intelligence platform.

## 🚀 Quick Start

```bash
# 1. Setup
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your API keys and database credentials

# 3. Generate Admin Key
python scripts/generate_admin_key.py
# Add to .env: ADMIN_API_KEY=your_generated_key

# 4. Initialize Database
mysql -u root -p
CREATE DATABASE toorysts;
exit
mysql -u root -p toorysts < sql/create_schema.sql

# 5. Import Attractions
python scripts/02_import_attractions.py

# 6. Start Everything
./start_all.sh
```

**API Docs:** http://localhost:8000/docs

---

## ✨ What It Does

### Automatic Pipeline System

1. **👀 File Watcher** - Monitors Excel file for changes
2. **➕ Auto Import** - Adds new attractions to database
3. **🚀 Data Fetching** - Fetches 9 sections from 7 APIs:
   - Hero Images (Google Places)
   - Best Time (BestTime API)
   - Weather (OpenWeatherMap)
   - Map (Google Maps)
   - Visitor Info (Google Places + Gemini)
   - Reviews (Google Places)
   - Tips (Reddit + Gemini)
   - Audience Profiles (Gemini)
   - Social Videos (YouTube)
   - Nearby Attractions (Database + Google Places)
4. **💾 Smart Storage** - Upsert pattern avoids duplicates
5. **🔄 Auto Refresh** - Scheduled updates via Celery Beat

**Just update the Excel file and the pipeline does the rest!**

---

## 📚 Documentation

| File | Description |
|------|-------------|
| **[QUICKSTART.md](QUICKSTART.md)** | Quick start guide |
| **[SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md)** | Complete system architecture |
| **[CLOUD_DEPLOYMENT.md](CLOUD_DEPLOYMENT.md)** | Cloud deployment guide |
| **[PERFORMANCE_OPTIMIZATION.md](PERFORMANCE_OPTIMIZATION.md)** | Performance details |

---

## 🎯 Key Features

### 1. Automatic Data Pipeline
- Watches Excel file for changes
- Imports new attractions automatically
- Fetches all data from external APIs
- Stores in database with smart upsert

### 2. Scheduled Refreshes
- **Best Time**: Every 5 days at 2 AM
- **Weather**: Every 3 days at 3 AM
- **Visitor Info**: Weekly on Monday at 3 AM
- **Full Pipeline**: Monthly on 1st at 4 AM

### 3. Smart Refresh Logic
- Only refreshes data when needed
- Avoids unnecessary API calls
- Maintains data freshness

### 4. Complete REST API
- Homepage data
- Cities list and details
- Attractions with filters
- Complete attraction data (all 9 sections)
- Admin endpoints for pipeline management

---

## 🏃 Running the System

### Start All Services
```bash
./start_all.sh
```

This starts:
- Redis
- Backend API (port 8000)
- Celery Worker
- Celery Beat
- File Watcher (if enabled)

### Stop All Services
```bash
./stop_all.sh
```

### View Logs
```bash
tail -f logs/backend.log
tail -f logs/celery_worker.log
tail -f logs/celery_beat.log
tail -f logs/file_watcher.log
```

---

## 📊 API Endpoints

### Public Endpoints
```
GET  /health                              # Health check
GET  /api/v1/homepage                     # Homepage data
GET  /api/v1/cities                       # All cities
GET  /api/v1/cities/{slug}                # City details
GET  /api/v1/cities/{slug}/attractions    # City attractions (paginated)
GET  /api/v1/attractions                  # All attractions (with filters)
GET  /api/v1/attractions/{slug}           # Complete attraction data
GET  /api/v1/attractions/{slug}/page      # Attraction page data
GET  /api/v1/attractions/{slug}/sections  # Attraction sections
```

### Admin Endpoints (Require X-Admin-Key)
```
POST /api/v1/pipeline/start               # Start pipeline
GET  /api/v1/pipeline/status/{task_id}    # Check task status
```

---

## 🗂️ Architecture

```
backend/
├── app/
│   ├── api/v1/routes/       # API endpoints
│   │   ├── frontend.py      # Public endpoints
│   │   ├── pipeline.py      # Admin endpoints
│   │   └── attractions.py   # Legacy endpoints
│   ├── application/         # Business logic
│   ├── domain/              # Domain models
│   ├── infrastructure/      # External APIs & DB
│   │   ├── external_apis/   # API clients & fetchers
│   │   └── persistence/     # Database models
│   ├── tasks/               # Celery tasks
│   │   ├── refresh_tasks.py      # Scheduled refreshes
│   │   ├── pipeline_tasks.py     # Full pipeline
│   │   └── file_watcher_tasks.py # Excel monitoring
│   ├── celery_app.py        # Celery configuration
│   └── main.py              # FastAPI app
├── scripts/                 # Utility scripts
├── sql/                     # Database schema
├── logs/                    # Log files
└── data/                    # Excel files
```

---

## 🔧 Configuration

### Required Environment Variables

```bash
# Database
DATABASE_HOST=localhost
DATABASE_PORT=3306
DATABASE_USER=root
DATABASE_PASSWORD=your_password
DATABASE_NAME=toorysts

# Admin
ADMIN_API_KEY=your_generated_key

# API Keys
GOOGLE_PLACES_API_KEY=your_key
GOOGLE_MAPS_API_KEY=your_key
YOUTUBE_API_KEY=your_key
REDDIT_CLIENT_ID=your_id
REDDIT_CLIENT_SECRET=your_secret
OPENWEATHERMAP_API_KEY=your_key
GEMINI_API_KEY=your_key
BESTTIME_API_PRIVATE_KEY=your_key

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/1

# File Watcher
FILE_MONITOR_ENABLED=true
WATCH_DIRECTORY=data
INPUT_FILE_PATTERN=input_attractions.xlsx
```

---

## 🧪 Testing

### Test Individual Fetchers
```bash
python scripts/test_metadata.py
python scripts/test_audience.py
python scripts/test_social_videos.py
python scripts/test_nearby.py
python scripts/test_tips.py
```

### Test Full Pipeline
```bash
python scripts/00_fetch_all_data.py
```

### Check API
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/v1/homepage
```

---

## 📈 Monitoring

### Check Services
```bash
# Redis
redis-cli ping

# Celery Worker
celery -A app.celery_app inspect active

# Celery Beat
celery -A app.celery_app inspect scheduled
```

### View Task Status
```bash
curl http://localhost:8000/api/v1/pipeline/status/TASK_ID \
  -H "X-Admin-Key: YOUR_KEY"
```

---

## 🎉 Complete Workflow

### Adding New Attractions

1. **Update Excel File**
   - Add new attractions to `data/attractions.xlsx`
   - Required: slug, name, city, country, place_id, lat, lng

2. **Save File**
   - File watcher detects change automatically
   - Or trigger manually: `POST /api/v1/pipeline/start`

3. **Pipeline Runs**
   - Imports new attractions
   - Fetches all 9 sections of data
   - Stores in database

4. **Verify**
   - Check API: `GET /api/v1/attractions/{slug}`
   - View logs: `tail -f logs/celery_worker.log`

**Done!** ✅ New attractions are live with complete data.

---

## 🆘 Troubleshooting

### Port 8000 in use
```bash
kill -9 $(lsof -ti:8000)
./start_all.sh
```

### Redis not running
```bash
redis-server --daemonize yes
```

### Celery tasks not running
```bash
# Check worker
celery -A app.celery_app inspect active

# Check logs
tail -f logs/celery_worker.log
```

### File watcher not detecting
```bash
# Check if enabled
grep FILE_MONITOR_ENABLED .env

# Check logs
tail -f logs/file_watcher.log
```

---

## 📝 License

MIT

---

## 🤝 Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review documentation files
3. Test with individual scripts
4. Verify API keys and quotas
