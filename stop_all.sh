#!/bin/bash

# Stop All Services

echo "=========================================="
echo "Stopping All Tooryst Backend Services"
echo "=========================================="

# Create .pids directory if it doesn't exist
mkdir -p .pids

# Stop Backend API
if [ -f ".pids/backend.pid" ]; then
    BACKEND_PID=$(cat .pids/backend.pid)
    if kill -0 $BACKEND_PID 2>/dev/null; then
        kill $BACKEND_PID
        echo "✓ Backend API stopped"
    else
        echo "✓ Backend API not running"
    fi
    rm .pids/backend.pid
else
    # Try to find and kill by port
    BACKEND_PID=$(lsof -ti:8000)
    if [ ! -z "$BACKEND_PID" ]; then
        kill $BACKEND_PID
        echo "✓ Backend API stopped"
    else
        echo "✓ Backend API not running"
    fi
fi

# Stop Celery Workers (gracefully)
echo "Stopping Celery Workers..."
if command -v celery >/dev/null 2>&1; then
    # Try graceful shutdown first
    celery -A app.celery_app control shutdown 2>/dev/null || true
    sleep 1
fi

# Stop all stage workers (stage1-stage10)
for i in {1..10}; do
    if [ -f ".pids/stage${i}.pid" ]; then
        STAGE_PID=$(cat .pids/stage${i}.pid)
        if kill -0 $STAGE_PID 2>/dev/null; then
            kill -TERM $STAGE_PID 2>/dev/null || true
            sleep 0.5
            kill -9 $STAGE_PID 2>/dev/null || true
        fi
        rm -f .pids/stage${i}.pid
    fi
done

# Stop main worker
if [ -f ".pids/worker.pid" ]; then
    WORKER_PID=$(cat .pids/worker.pid)
    if kill -0 $WORKER_PID 2>/dev/null; then
        kill -TERM $WORKER_PID 2>/dev/null || true
        sleep 1
        kill -9 $WORKER_PID 2>/dev/null || true
        echo "✓ Celery Workers stopped"
    else
        echo "✓ Celery Workers not running"
    fi
    rm -f .pids/worker.pid
else
    # Try to find and kill all celery workers
    pkill -TERM -f "celery.*worker" 2>/dev/null || true
    sleep 1
    pkill -9 -f "celery.*worker" 2>/dev/null || true
    echo "✓ Celery Workers stopped"
fi

# Stop Celery Beat
if [ -f ".pids/beat.pid" ]; then
    BEAT_PID=$(cat .pids/beat.pid)
    if kill -0 $BEAT_PID 2>/dev/null; then
        kill $BEAT_PID
        echo "✓ Celery Beat stopped"
    else
        echo "✓ Celery Beat not running"
    fi
    rm .pids/beat.pid
else
    # Try to find and kill celery beat
    pkill -f "celery.*beat"
    echo "✓ Celery Beat stopped"
fi

# Stop File Watcher
if [ -f ".pids/watcher.pid" ]; then
    WATCHER_PID=$(cat .pids/watcher.pid)
    if kill -0 $WATCHER_PID 2>/dev/null; then
        kill $WATCHER_PID
        echo "✓ File Watcher stopped"
    else
        echo "✓ File Watcher not running"
    fi
    rm .pids/watcher.pid
else
    # Try to find and kill file watcher
    pkill -f "file_watcher_tasks"
    echo "✓ File Watcher stopped"
fi

# Clear Redis queues/state to completely stop any running pipeline
# This is critical for stopping in-flight tasks and stage manager locks
# DB0: Celery broker queues, DB1: Celery results, DB2: Redis cache, DB4: stage manager semaphores/queues
echo "Clearing Redis state..."
if command -v redis-cli >/dev/null 2>&1; then
    # Flush all databases to ensure complete pipeline stop
    redis-cli FLUSHALL 2>/dev/null && echo "✓ Redis FLUSHALL - all databases cleared (pipeline completely stopped)"
    
    # Verify each DB is cleared
    redis-cli -n 0 DBSIZE 2>/dev/null | grep -q "0" && echo "  ✓ DB0 (Celery broker) cleared"
    redis-cli -n 1 DBSIZE 2>/dev/null | grep -q "0" && echo "  ✓ DB1 (Celery results) cleared"
    redis-cli -n 2 DBSIZE 2>/dev/null | grep -q "0" && echo "  ✓ DB2 (Redis cache) cleared"
    redis-cli -n 4 DBSIZE 2>/dev/null | grep -q "0" && echo "  ✓ DB4 (stage manager) cleared"
else
    echo "⚠ redis-cli not found; Redis queues/state not cleared"
    echo "  To manually clear: redis-cli FLUSHALL (clears all databases)"
fi

# Optionally stop Redis (commented out by default)
# echo "Stopping Redis..."
# redis-cli shutdown
# echo "✓ Redis stopped"

echo ""
echo "=========================================="
echo "All Services Stopped"
echo "=========================================="
