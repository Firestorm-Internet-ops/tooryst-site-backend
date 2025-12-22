#!/bin/bash
# Cloud Deployment Script for Storyboard Backend
# This script sets up and starts all services for production

set -e  # Exit on error

echo "=========================================="
echo "Storyboard Backend - Cloud Deployment"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running in cloud environment
if [ -z "$ENVIRONMENT" ]; then
    export ENVIRONMENT="production"
fi

echo -e "${GREEN}Environment: $ENVIRONMENT${NC}"

# 1. Check prerequisites
echo ""
echo "Step 1: Checking prerequisites..."

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python 3 found${NC}"

# Check MySQL
if ! command -v mysql &> /dev/null; then
    echo -e "${YELLOW}⚠ MySQL client not found (optional)${NC}"
else
    echo -e "${GREEN}✓ MySQL found${NC}"
fi

# Check Redis
if ! command -v redis-cli &> /dev/null; then
    echo -e "${RED}✗ Redis not found${NC}"
    echo "Please install Redis:"
    echo "  Ubuntu: sudo apt-get install redis-server"
    echo "  macOS: brew install redis"
    exit 1
fi
echo -e "${GREEN}✓ Redis found${NC}"

# 2. Setup virtual environment
echo ""
echo "Step 2: Setting up Python environment..."

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# 3. Install dependencies
echo ""
echo "Step 3: Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"

# 4. Check .env file
echo ""
echo "Step 4: Checking configuration..."

if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠ .env file not found, copying from .env.example${NC}"
    cp .env.example .env
    echo -e "${RED}⚠ IMPORTANT: Please update .env with your API keys and database credentials${NC}"
    echo "Press Enter to continue after updating .env..."
    read
fi
echo -e "${GREEN}✓ Configuration file found${NC}"

# 5. Start Redis
echo ""
echo "Step 5: Starting Redis..."

if pgrep -x "redis-server" > /dev/null; then
    echo -e "${GREEN}✓ Redis already running${NC}"
else
    redis-server --daemonize yes
    sleep 2
    if pgrep -x "redis-server" > /dev/null; then
        echo -e "${GREEN}✓ Redis started${NC}"
    else
        echo -e "${RED}✗ Failed to start Redis${NC}"
        exit 1
    fi
fi

# Test Redis connection
if redis-cli ping > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Redis connection successful${NC}"
else
    echo -e "${RED}✗ Cannot connect to Redis${NC}"
    exit 1
fi

# 6. Setup database
echo ""
echo "Step 6: Setting up database..."

# Check if database exists
DB_HOST=$(grep DATABASE_HOST .env | cut -d '=' -f2)
DB_PORT=$(grep DATABASE_PORT .env | cut -d '=' -f2)
DB_USER=$(grep DATABASE_USER .env | cut -d '=' -f2)
DB_PASSWORD=$(grep DATABASE_PASSWORD .env | cut -d '=' -f2)
DB_NAME=$(grep DATABASE_NAME .env | cut -d '=' -f2)

echo "Database: $DB_NAME on $DB_HOST:$DB_PORT"

# Run database setup script
if [ -f "scripts/01_create_database.py" ]; then
    echo "Running database setup..."
    python scripts/01_create_database.py
    echo -e "${GREEN}✓ Database setup complete${NC}"
else
    echo -e "${YELLOW}⚠ Database setup script not found, skipping${NC}"
fi

# 7. Create necessary directories
echo ""
echo "Step 7: Creating directories..."

mkdir -p data
mkdir -p logs
mkdir -p .pids

echo -e "${GREEN}✓ Directories created${NC}"

# 8. Start all services
echo ""
echo "Step 8: Starting services..."

# Make start scripts executable
chmod +x start_all.sh stop_all.sh

# Stop any existing services
echo "Stopping any existing services..."
./stop_all.sh 2>/dev/null || true

# Start all services
echo "Starting all services..."
./start_all.sh

# 9. Verify services
echo ""
echo "Step 9: Verifying services..."

sleep 5

# Check FastAPI
if pgrep -f "uvicorn.*main:app" > /dev/null; then
    echo -e "${GREEN}✓ FastAPI server running${NC}"
else
    echo -e "${RED}✗ FastAPI server not running${NC}"
fi

# Check Celery worker
if pgrep -f "celery.*worker" > /dev/null; then
    echo -e "${GREEN}✓ Celery worker running${NC}"
else
    echo -e "${RED}✗ Celery worker not running${NC}"
fi

# Check Celery beat
if pgrep -f "celery.*beat" > /dev/null; then
    echo -e "${GREEN}✓ Celery beat running${NC}"
else
    echo -e "${RED}✗ Celery beat not running${NC}"
fi

# Check file watcher
if pgrep -f "start_file_watcher" > /dev/null; then
    echo -e "${GREEN}✓ File watcher running${NC}"
else
    echo -e "${RED}✗ File watcher not running${NC}"
fi

# 10. Display status
echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Services Status:"
echo "  • FastAPI:      http://localhost:8000"
echo "  • API Docs:     http://localhost:8000/docs"
echo "  • Redis:        localhost:6379"
echo "  • File Watcher: Monitoring data/attractions.xlsx"
echo ""
echo "Logs:"
echo "  • FastAPI:      logs/fastapi.log"
echo "  • Celery:       logs/celery_worker.log"
echo "  • Beat:         logs/celery_beat.log"
echo "  • File Watcher: logs/file_watcher.log"
echo ""
echo "Next Steps:"
echo "  1. Update data/attractions.xlsx with your attractions"
echo "  2. Pipeline will automatically run when file is saved"
echo "  3. Monitor logs: tail -f logs/*.log"
echo "  4. Check API: curl http://localhost:8000/api/v1/homepage"
echo ""
echo "To stop all services:"
echo "  ./stop_all.sh"
echo ""
echo "=========================================="
