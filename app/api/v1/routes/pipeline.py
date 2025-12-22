"""API endpoint for pipeline management."""
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import Dict, List, Optional
import os
from sqlalchemy import text

from app.tasks.file_watcher_tasks import process_excel_update
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from app.core.stage_manager import stage_manager

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


class PipelineResponse(BaseModel):
    """Response model for pipeline."""
    status: str
    message: str
    task_id: str


def verify_admin_key(x_admin_key: str = Header(...)):
    """Verify admin API key."""
    admin_key = os.getenv("ADMIN_API_KEY")
    if not admin_key or x_admin_key != admin_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    return True


@router.post("/start", response_model=PipelineResponse)
async def start_pipeline(_: bool = Depends(verify_admin_key)):
    """Start the pipeline to process Excel file and fetch data.
    
    This endpoint:
    1. Reads data/attractions.xlsx
    2. Identifies new attractions not in database
    3. Imports them to attractions table
    4. Triggers full pipeline to fetch all 9 sections of data
    
    Requires admin API key in X-Admin-Key header.
        
    Returns:
        Task ID to track progress
    """
    try:
        # Use absolute path from backend root
        from pathlib import Path
        # __file__ = backend/app/api/v1/routes/pipeline.py
        # Go up 5 levels: routes -> v1 -> api -> app -> backend
        backend_root = Path(__file__).parent.parent.parent.parent.parent
        file_path = str(backend_root / "data" / "attractions.xlsx")
        
        task = process_excel_update.delay(file_path)
        
        return PipelineResponse(
            status="success",
            message=f"Pipeline started",
            task_id=task.id
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {str(e)}")


@router.get("/status/{task_id}")
async def get_pipeline_status(task_id: str, _: bool = Depends(verify_admin_key)):
    """Get status of a pipeline task.
    
    Requires admin API key in X-Admin-Key header.
    
    Args:
        task_id: Celery task ID from /start endpoint
        
    Returns:
        Task status and result
    """
    try:
        from celery.result import AsyncResult
        from app.celery_app import celery_app
        
        task = AsyncResult(task_id, app=celery_app)
        
        return {
            "task_id": task_id,
            "status": task.status,
            "result": task.result if task.ready() else None,
            "info": task.info
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@router.get("/progress/{pipeline_run_id}")
async def get_pipeline_progress(pipeline_run_id: int, _: bool = Depends(verify_admin_key)):
    """Get detailed progress of a pipeline run.
    
    Returns attraction names with their current stage and completed stages.
    Tracks progress using Redis stage queues to avoid false positives from old data.
    
    Requires admin API key in X-Admin-Key header.
    
    Args:
        pipeline_run_id: ID of the pipeline run
        
    Returns:
        JSON with attraction names as keys and stage progress as values:
        {
            "Singapore Zoo": {
                "current_stage": 10,
                "current_stage_name": "audiences",
                "stages_completed": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "status": "complete"
            },
            "Skypark Observation Deck": {
                "current_stage": 4,
                "current_stage_name": "weather",
                "stages_completed": [1, 2, 3],
                "status": "in_progress"
            }
        }
    """
    try:
        session = SessionLocal()
        
        # Get pipeline run info
        pipeline_run = session.query(models.PipelineRun).filter_by(id=pipeline_run_id).first()
        if not pipeline_run:
            raise HTTPException(status_code=404, detail=f"Pipeline run {pipeline_run_id} not found")
        
        # Get all attractions for this pipeline run
        attractions = session.query(models.Attraction).all()
        
        # Stage definitions
        STAGES = {
            1: 'metadata',
            2: 'hero_images',
            3: 'best_time',
            4: 'weather',
            5: 'tips',
            6: 'map',
            7: 'reviews',
            8: 'social_videos',
            9: 'nearby',
            10: 'audiences'
        }
        
        # Stage order for tracking
        STAGE_ORDER = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        
        progress_data = {}
        
        for attraction in attractions:
            # Track which stages this attraction has been queued for in THIS pipeline run
            # by checking Redis stage queues
            stages_completed = []
            
            # Check each stage queue to see if this attraction has passed through
            for stage_num in STAGE_ORDER:
                stage_name = STAGES[stage_num]
                queue_key = f"stage_queue:{stage_name}"
                
                try:
                    # Get all items in this stage's queue
                    all_items = stage_manager.redis_client.zrange(queue_key, 0, -1)
                    
                    # Check if this attraction is in the queue for this pipeline run
                    member_pattern = f"{pipeline_run_id}:{attraction.id}"
                    is_in_queue = any(member_pattern in item for item in all_items)
                    
                    # If attraction is in queue, it means it has reached this stage
                    if is_in_queue:
                        stages_completed.append(stage_num)
                except Exception as e:
                    # If we can't check queue, fall back to checking database
                    pass
            
            # If no queue data, fall back to database check (for completed pipelines)
            if not stages_completed:
                # Check database for actual data
                stage_models = {
                    1: models.AttractionMetadata,
                    2: models.HeroImage,
                    3: models.BestTimeData,
                    4: models.WeatherForecast,
                    5: models.Tip,
                    6: models.MapSnapshot,
                    7: models.Review,
                    8: models.SocialVideo,
                    9: models.NearbyAttraction,
                    10: models.AudienceProfile
                }
                
                for stage_num, model_class in stage_models.items():
                    try:
                        has_data = session.query(model_class).filter_by(attraction_id=attraction.id).first()
                        if has_data:
                            stages_completed.append(stage_num)
                    except Exception:
                        pass
            
            # Determine current stage and status
            if not stages_completed:
                # No stages completed - attraction is queued
                current_stage = 1
                status = "queued"
            elif len(stages_completed) == 10:
                # All 10 stages completed
                current_stage = 10
                status = "complete"
            else:
                # Find the next stage after the last completed one
                stages_completed_sorted = sorted(stages_completed)
                last_completed = stages_completed_sorted[-1]
                
                # Find next stage (handle any gaps)
                current_stage = last_completed + 1
                while current_stage <= 10 and current_stage in stages_completed:
                    current_stage += 1
                
                # If we've gone past stage 10, mark as complete
                if current_stage > 10:
                    current_stage = 10
                    status = "complete"
                else:
                    status = "in_progress"
            
            # Get stage name
            stage_name = STAGES.get(current_stage, 'unknown')
            
            progress_data[attraction.name] = {
                "attraction_id": attraction.id,
                "current_stage": current_stage,
                "current_stage_name": stage_name,
                "stages_completed": sorted(stages_completed),
                "total_stages_completed": len(stages_completed),
                "status": status
            }
        
        session.close()
        
        # Calculate summary statistics
        complete_count = sum(1 for a in progress_data.values() if a["status"] == "complete")
        in_progress_count = sum(1 for a in progress_data.values() if a["status"] == "in_progress")
        queued_count = sum(1 for a in progress_data.values() if a["status"] == "queued")
        
        return {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_status": pipeline_run.status,
            "attractions": progress_data,
            "summary": {
                "total_attractions": len(attractions),
                "complete": complete_count,
                "in_progress": in_progress_count,
                "queued": queued_count,
                "total_stages": 10,
                "average_stages_completed": round(sum(a["total_stages_completed"] for a in progress_data.values()) / len(attractions), 2) if attractions else 0
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"Failed to get pipeline progress: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)
