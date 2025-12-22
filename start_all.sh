#!/bin/bash

# Start All Services for Development
# This script starts Redis, Backend, Celery Worker, and Celery Beat in the background

echo "=========================================="
echo "Starting All Tooryst Backend Services"
echo "=========================================="

# Check if Redis is already running
if redis-cli ping > /dev/null 2>&1; then
    echo "✓ Redis is already running"
else
    echo "Starting Redis..."
    redis-server --daemonize yes
    sleep 2
    if redis-cli ping > /dev/null 2>&1; then
        echo "✓ Redis started"
    else
        echo "✗ Failed to start Redis"
        echo "  Try starting manually: redis-server"
        exit 1
    fi
fi

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Load environment variables
if [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if port 8000 is in use
if lsof -ti:8000 > /dev/null 2>&1; then
    echo "Port 8000 is in use. Stopping existing process..."
    kill -9 $(lsof -ti:8000) 2>/dev/null
    sleep 1
fi

# Start Backend API
echo "Starting Backend API..."
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload > logs/backend.log 2>&1 &
BACKEND_PID=$!
echo "✓ Backend API started (PID: $BACKEND_PID)"

# Wait for backend to be ready
sleep 2

# Stop any existing Celery processes
if pgrep -f "celery.*worker" > /dev/null 2>&1; then
    echo "Stopping existing Celery Worker..."
    pkill -f "celery.*worker"
    sleep 1
fi

if pgrep -f "celery.*beat" > /dev/null 2>&1; then
    echo "Stopping existing Celery Beat..."
    pkill -f "celery.*beat"
    sleep 1
fi

# Start Celery Workers for pipeline stages
echo "Starting Celery Workers..."

# Stage 1 Worker (Metadata) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_1 -c 1 -n stage1@%h --loglevel=INFO > logs/celery_stage1.log 2>&1 &
STAGE1_PID=$!
echo "✓ Celery Stage 1 Worker started (PID: $STAGE1_PID)"

# Stage 2 Worker (Hero Images) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_2 -c 1 -n stage2@%h --loglevel=INFO > logs/celery_stage2.log 2>&1 &
STAGE2_PID=$!
echo "✓ Celery Stage 2 Worker started (PID: $STAGE2_PID)"

# Stage 3 Worker (Best Time) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_3 -c 1 -n stage3@%h --loglevel=INFO > logs/celery_stage3.log 2>&1 &
STAGE3_PID=$!
echo "✓ Celery Stage 3 Worker started (PID: $STAGE3_PID)"

# Stage 4 Worker (Weather) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_4 -c 1 -n stage4@%h --loglevel=INFO > logs/celery_stage4.log 2>&1 &
STAGE4_PID=$!
echo "✓ Celery Stage 4 Worker started (PID: $STAGE4_PID)"

# Stage 5 Worker (Tips) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_5 -c 1 -n stage5@%h --loglevel=INFO > logs/celery_stage5.log 2>&1 &
STAGE5_PID=$!
echo "✓ Celery Stage 5 Worker started (PID: $STAGE5_PID)"

# Stage 6 Worker (Map) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_6 -c 1 -n stage6@%h --loglevel=INFO > logs/celery_stage6.log 2>&1 &
STAGE6_PID=$!
echo "✓ Celery Stage 6 Worker started (PID: $STAGE6_PID)"

# Stage 7 Worker (Reviews) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_7 -c 1 -n stage7@%h --loglevel=INFO > logs/celery_stage7.log 2>&1 &
STAGE7_PID=$!
echo "✓ Celery Stage 7 Worker started (PID: $STAGE7_PID)"

# Stage 8 Worker (Social Videos) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_8 -c 1 -n stage8@%h --loglevel=INFO > logs/celery_stage8.log 2>&1 &
STAGE8_PID=$!
echo "✓ Celery Stage 8 Worker started (PID: $STAGE8_PID)"

# Stage 9 Worker (Nearby Attractions) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_9 -c 1 -n stage9@%h --loglevel=INFO > logs/celery_stage9.log 2>&1 &
STAGE9_PID=$!
echo "✓ Celery Stage 9 Worker started (PID: $STAGE9_PID)"

# Stage 10 Worker (Audiences) - 1 concurrent for sequential pipeline flow
nohup celery -A app.celery_app worker -Q pipeline_stage_10 -c 1 -n stage10@%h --loglevel=INFO > logs/celery_stage10.log 2>&1 &
STAGE10_PID=$!
echo "✓ Celery Stage 10 Worker started (PID: $STAGE10_PID)"

# Main Pipeline Worker
nohup celery -A app.celery_app worker -Q pipeline,celery -c 4 -n main@%h --loglevel=INFO > logs/celery_worker.log 2>&1 &
WORKER_PID=$!
echo "✓ Celery Main Worker started (PID: $WORKER_PID)"

# Start Celery Beat
echo "Starting Celery Beat..."
nohup celery -A app.celery_app beat --loglevel=INFO > logs/celery_beat.log 2>&1 &
BEAT_PID=$!
echo "✓ Celery Beat started (PID: $BEAT_PID)"

# Start File Watcher (if enabled)
if [ "${FILE_MONITOR_ENABLED}" = "true" ]; then
    echo "Starting File Watcher..."
    nohup python -m app.tasks.file_watcher_tasks > logs/file_watcher.log 2>&1 &
    WATCHER_PID=$!
    echo "✓ File Watcher started (PID: $WATCHER_PID)"
    echo "$WATCHER_PID" > .pids/watcher.pid
else
    echo "⊘ File Watcher disabled (set FILE_MONITOR_ENABLED=true in .env to enable)"
fi

# Save PIDs to file for easy stopping
mkdir -p .pids
echo "$BACKEND_PID" > .pids/backend.pid
echo "$STAGE1_PID" > .pids/stage1.pid
echo "$STAGE2_PID" > .pids/stage2.pid
echo "$STAGE3_PID" > .pids/stage3.pid
echo "$STAGE4_PID" > .pids/stage4.pid
echo "$STAGE5_PID" > .pids/stage5.pid
echo "$STAGE6_PID" > .pids/stage6.pid
echo "$STAGE7_PID" > .pids/stage7.pid
echo "$STAGE8_PID" > .pids/stage8.pid
echo "$STAGE9_PID" > .pids/stage9.pid
echo "$STAGE10_PID" > .pids/stage10.pid
echo "$WORKER_PID" > .pids/worker.pid
echo "$BEAT_PID" > .pids/beat.pid

echo ""
echo "=========================================="
echo "All Services Started Successfully!"
echo "=========================================="
echo ""
echo "Services:"
echo "  • Redis:              Running"
echo "  • Backend API:        http://localhost:8000"
echo "  • API Docs:           http://localhost:8000/docs"
echo "  • Celery Stage 1:     Running (Metadata)"
echo "  • Celery Stage 2:     Running (Hero Images)"
echo "  • Celery Stage 3:     Running (Best Time)"
echo "  • Celery Stage 4:     Running (Weather)"
echo "  • Celery Stage 5:     Running (Tips)"
echo "  • Celery Stage 6:     Running (Map)"
echo "  • Celery Stage 7:     Running (Reviews)"
echo "  • Celery Stage 8:     Running (Social Videos)"
echo "  • Celery Stage 9:     Running (Nearby Attractions)"
echo "  • Celery Stage 10:    Running (Audiences)"
echo "  • Celery Main:        Running (Pipeline + General)"
echo "  • Celery Beat:        Running"
if [ "${FILE_MONITOR_ENABLED}" = "true" ]; then
    echo "  • File Watcher:       Running (monitoring ${WATCH_DIRECTORY}/${INPUT_FILE_PATTERN})"
fi
echo ""
echo "Logs:"
echo "  • Backend:            tail -f logs/backend.log"
echo "  • Celery Stage 1:     tail -f logs/celery_stage1.log"
echo "  • Celery Stage 2:     tail -f logs/celery_stage2.log"
echo "  • Celery Stage 3:     tail -f logs/celery_stage3.log"
echo "  • Celery Stage 4:     tail -f logs/celery_stage4.log"
echo "  • Celery Stage 5:     tail -f logs/celery_stage5.log"
echo "  • Celery Stage 6:     tail -f logs/celery_stage6.log"
echo "  • Celery Stage 7:     tail -f logs/celery_stage7.log"
echo "  • Celery Stage 8:     tail -f logs/celery_stage8.log"
echo "  • Celery Stage 9:     tail -f logs/celery_stage9.log"
echo "  • Celery Stage 10:    tail -f logs/celery_stage10.log"
echo "  • Celery Main:        tail -f logs/celery_worker.log"
echo "  • Celery Beat:        tail -f logs/celery_beat.log"
if [ "${FILE_MONITOR_ENABLED}" = "true" ]; then
    echo "  • File Watcher:       tail -f logs/file_watcher.log"
fi
echo ""
echo "To stop all services: ./stop_all.sh"
echo "=========================================="
