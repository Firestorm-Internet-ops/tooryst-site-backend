#!/bin/bash

# Start only the FastAPI backend without Celery
echo "Starting FastAPI backend only..."

# Kill any existing uvicorn processes
pkill -f uvicorn

# Start the backend
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --log-level info