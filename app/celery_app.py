"""Celery application configuration."""
import os
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

load_dotenv()

# Create Celery app
celery_app = Celery(
    "storyboard",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
    include=[
        "app.tasks.refresh_tasks",
        "app.tasks.pipeline_tasks",
        "app.tasks.parallel_pipeline_tasks",
        "app.tasks.pipeline_cleanup",
        "app.tasks.pipeline_resume",
        "app.tasks.data_reporting",
        "app.tasks.file_watcher_tasks",
        "app.tasks.youtube_retry_tasks",
        "app.tasks.reddit_tip_fetcher_task",
        "app.tasks.nearby_attractions_tasks"
    ]
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone=os.getenv("CELERY_TIMEZONE", "UTC"),
    enable_utc=True,
    # Keep retrying broker connection on startup (Celery 6+ change)
    broker_connection_retry_on_startup=bool(
        int(os.getenv("CELERY_BROKER_RETRY_ON_STARTUP", "1"))
    ),
    task_track_started=True,
    task_time_limit=int(os.getenv("CELERY_TASK_TIME_LIMIT_SECONDS", "1800")),  # 30 minutes
    task_soft_time_limit=int(os.getenv("CELERY_TASK_TIME_LIMIT_SECONDS", "1800")) - 60,
    worker_prefetch_multiplier=4,  # Increased from 1 for async I/O work (pipeline stages)
    worker_max_tasks_per_child=50,
)

# Task routing for pipeline stages
celery_app.conf.task_routes = {
    'app.tasks.parallel_pipeline_tasks.process_stage_metadata': {
        'queue': 'pipeline_stage_1',
        'routing_key': 'stage1'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_hero_images': {
        'queue': 'pipeline_stage_2',
        'routing_key': 'stage2'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_best_time': {
        'queue': 'pipeline_stage_3',
        'routing_key': 'stage3'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_weather': {
        'queue': 'pipeline_stage_4',
        'routing_key': 'stage4'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_tips': {
        'queue': 'pipeline_stage_5',
        'routing_key': 'stage5'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_map': {
        'queue': 'pipeline_stage_6',
        'routing_key': 'stage6'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_reviews': {
        'queue': 'pipeline_stage_7',
        'routing_key': 'stage7'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_social_videos': {
        'queue': 'pipeline_stage_8',
        'routing_key': 'stage8'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_nearby': {
        'queue': 'pipeline_stage_9',
        'routing_key': 'stage9'
    },
    'app.tasks.parallel_pipeline_tasks.process_stage_audiences': {
        'queue': 'pipeline_stage_10',
        'routing_key': 'stage10'
    },
    'app.tasks.parallel_pipeline_tasks.orchestrate_pipeline': {
        'queue': 'pipeline',
        'routing_key': 'pipeline'
    }
}

# Celery Beat schedule for periodic tasks
celery_app.conf.beat_schedule = {
    # Weather - Refresh every 3 days (we have 5-7 days forecast, refresh when 2-4 days left)
    "refresh-weather-periodic": {
        "task": "app.tasks.refresh_tasks.refresh_weather_data",
        "schedule": crontab(
            hour=int(os.getenv("WEATHER_REFRESH_HOUR", "3")),
            minute=0,
            day_of_week="*/3"  # Every 3 days
        ),
    },
    
    # Visitor Info (Opening Hours) - Refresh weekly on Monday at 3 AM
    "refresh-visitor-info-weekly": {
        "task": "app.tasks.refresh_tasks.refresh_visitor_info",
        "schedule": crontab(
            day_of_week=os.getenv("VISITOR_INFO_REFRESH_DAY", "monday"),
            hour=int(os.getenv("VISITOR_INFO_REFRESH_HOUR", "3")),
            minute=0
        ),
    },
    
    # Full Pipeline - Refresh all data monthly on 1st at 4 AM
    "refresh-all-data-monthly": {
        "task": "app.tasks.pipeline_tasks.run_full_pipeline",
        "schedule": crontab(
            day_of_month="1",
            hour=4,
            minute=0
        ),
    },
    
    # YouTube Retry - Fetch videos for attractions without videos (Daily at 8 AM UTC = midnight PT)
    "youtube-retry-daily": {
        "task": "app.tasks.youtube_retry_tasks.fetch_missing_youtube_videos",
        "schedule": crontab(
            hour=8,
            minute=0
        ),
    },
    
    # Nearby Attractions - Refresh nearby attractions for attractions that need it (Daily at 2 AM)
    "refresh-nearby-attractions-daily": {
        "task": "app.tasks.nearby_attractions_tasks.refresh_all_nearby_attractions",
        "schedule": crontab(
            hour=2,
            minute=0
        ),
    },

}

if __name__ == "__main__":
    celery_app.start()
