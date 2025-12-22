"""Health check service for monitoring system components."""
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum

import pymysql
import redis
from celery import Celery

from app.config import settings

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health status values."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


class HealthCheckService:
    """Service for checking health of system components."""
    
    def __init__(self):
        """Initialize health check service."""
        self.db_config = {
            'host': os.getenv('DATABASE_HOST', 'localhost'),
            'port': int(os.getenv('DATABASE_PORT', 3306)),
            'user': os.getenv('DATABASE_USER', 'root'),
            'password': os.getenv('DATABASE_PASSWORD', ''),
            'database': os.getenv('DATABASE_NAME', 'storyboard'),
            'charset': 'utf8mb4'
        }
        
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        
        self.celery_broker_url = os.getenv('CELERY_BROKER_URL', f'redis://{self.redis_host}:{self.redis_port}/0')
    
    def check_database(self) -> Dict[str, Any]:
        """Check database connectivity and health.
        
        Returns:
            Dictionary with status and details
        """
        try:
            conn = pymysql.connect(**self.db_config)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                
                return {
                    "status": HealthStatus.HEALTHY,
                    "message": "Database connection successful",
                    "details": {
                        "host": self.db_config['host'],
                        "database": self.db_config['database']
                    }
                }
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY,
                "message": f"Database connection failed: {str(e)}",
                "details": {
                    "host": self.db_config['host'],
                    "database": self.db_config['database'],
                    "error": str(e)
                }
            }
    
    def check_redis(self) -> Dict[str, Any]:
        """Check Redis connectivity and health.
        
        Returns:
            Dictionary with status and details
        """
        try:
            socket_timeout = settings.REDIS_SOCKET_TIMEOUT_SECONDS
            client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                socket_connect_timeout=socket_timeout
            )
            
            # Test connection with ping
            client.ping()
            
            # Get some stats
            info = client.info('stats')
            
            return {
                "status": HealthStatus.HEALTHY,
                "message": "Redis connection successful",
                "details": {
                    "host": self.redis_host,
                    "port": self.redis_port,
                    "total_connections_received": info.get('total_connections_received', 0),
                    "total_commands_processed": info.get('total_commands_processed', 0)
                }
            }
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY,
                "message": f"Redis connection failed: {str(e)}",
                "details": {
                    "host": self.redis_host,
                    "port": self.redis_port,
                    "error": str(e)
                }
            }
    
    def check_celery_worker(self) -> Dict[str, Any]:
        """Check Celery worker health.
        
        Returns:
            Dictionary with status and details
        """
        try:
            from app.celery_app import celery_app
            
            # Get active workers
            celery_timeout = settings.CELERY_INSPECT_TIMEOUT_SECONDS
            inspect = celery_app.control.inspect(timeout=celery_timeout)
            active_workers = inspect.active()
            
            if active_workers:
                worker_count = len(active_workers)
                return {
                    "status": HealthStatus.HEALTHY,
                    "message": f"{worker_count} Celery worker(s) active",
                    "details": {
                        "worker_count": worker_count,
                        "workers": list(active_workers.keys())
                    }
                }
            else:
                return {
                    "status": HealthStatus.UNHEALTHY,
                    "message": "No active Celery workers found",
                    "details": {
                        "worker_count": 0
                    }
                }
        except Exception as e:
            logger.error(f"Celery health check failed: {e}")
            return {
                "status": HealthStatus.UNHEALTHY,
                "message": f"Celery check failed: {str(e)}",
                "details": {
                    "error": str(e)
                }
            }
    
    def get_last_pipeline_run(self) -> Optional[Dict[str, Any]]:
        """Get timestamp and details of last pipeline run.
        
        Returns:
            Dictionary with last run details or None
        """
        try:
            conn = pymysql.connect(**self.db_config)
            try:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            id,
                            started_at,
                            completed_at,
                            status,
                            attractions_processed,
                            attractions_succeeded,
                            attractions_failed,
                            error_message
                        FROM pipeline_runs
                        ORDER BY started_at DESC
                        LIMIT 1
                    """)
                    result = cursor.fetchone()
                    
                    if result:
                        return {
                            "id": result['id'],
                            "started_at": result['started_at'].isoformat() if result['started_at'] else None,
                            "completed_at": result['completed_at'].isoformat() if result['completed_at'] else None,
                            "status": result['status'],
                            "attractions_processed": result['attractions_processed'],
                            "attractions_succeeded": result['attractions_succeeded'],
                            "attractions_failed": result['attractions_failed'],
                            "error_message": result['error_message']
                        }
                    return None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get last pipeline run: {e}")
            return None
    
    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall system health status.
        
        Returns:
            Dictionary with overall health and component statuses
        """
        # Check all components
        db_health = self.check_database()
        redis_health = self.check_redis()
        celery_health = self.check_celery_worker()
        last_pipeline_run = self.get_last_pipeline_run()
        
        # Determine overall status
        component_statuses = [
            db_health['status'],
            redis_health['status'],
            celery_health['status']
        ]
        
        if all(status == HealthStatus.HEALTHY for status in component_statuses):
            overall_status = HealthStatus.HEALTHY
        elif any(status == HealthStatus.UNHEALTHY for status in component_statuses):
            overall_status = HealthStatus.UNHEALTHY
        else:
            overall_status = HealthStatus.DEGRADED
        
        return {
            "status": overall_status,
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                "database": db_health,
                "redis": redis_health,
                "celery": celery_health
            },
            "last_pipeline_run": last_pipeline_run
        }


# Global health check service instance
health_service = HealthCheckService()
