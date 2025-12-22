#!/bin/bash

# Database Initialization Script

echo "==================================="
echo "Storyboard Database Initialization"
echo "==================================="

# Load environment variables
if [ -f "../.env" ]; then
    export $(cat ../.env | grep -v '^#' | xargs)
elif [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

DB_HOST=${DATABASE_HOST:-localhost}
DB_PORT=${DATABASE_PORT:-3306}
DB_USER=${DATABASE_USER:-root}
DB_NAME=${DATABASE_NAME:-storyboard}

echo ""
echo "Database Configuration:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  User: $DB_USER"
echo "  Database: $DB_NAME"
echo ""

# Check if MySQL is accessible
echo "Checking MySQL connection..."
mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DATABASE_PASSWORD" -e "SELECT 1;" > /dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "❌ Cannot connect to MySQL. Please check your credentials."
    exit 1
fi

echo "✓ MySQL connection successful"
echo ""

# Create database if it doesn't exist
echo "Creating database if not exists..."
mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DATABASE_PASSWORD" -e "CREATE DATABASE IF NOT EXISTS $DB_NAME;" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ Database created/verified"
else
    echo "❌ Failed to create database"
    exit 1
fi

echo ""

# Run schema creation
echo "Creating database schema..."
SCHEMA_FILE="../sql/create_schema.sql"

if [ ! -f "$SCHEMA_FILE" ]; then
    SCHEMA_FILE="sql/create_schema.sql"
fi

if [ ! -f "$SCHEMA_FILE" ]; then
    echo "❌ Schema file not found: $SCHEMA_FILE"
    exit 1
fi

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DATABASE_PASSWORD" "$DB_NAME" < "$SCHEMA_FILE"

if [ $? -eq 0 ]; then
    echo "✓ Schema created successfully"
else
    echo "❌ Failed to create schema"
    exit 1
fi

echo ""
echo "==================================="
echo "Database initialization complete!"
echo "==================================="
echo ""
echo "Next steps:"
echo "1. Import attractions: python scripts/02_import_attractions.py"
echo "2. Start backend: ./start.sh"
echo "3. Start Celery worker: ./start_celery_worker.sh"
echo "4. Start Celery beat: ./start_celery_beat.sh"
