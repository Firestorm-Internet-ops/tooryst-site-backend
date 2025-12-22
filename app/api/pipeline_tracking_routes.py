"""API routes for pipeline tracking and data monitoring."""
from fastapi import APIRouter, HTTPException, Query
from app.core.data_tracking_manager import data_tracking_manager
from app.infrastructure.persistence.db import SessionLocal
from sqlalchemy import text

router = APIRouter(prefix="/api/pipeline", tags=["pipeline-tracking"])


@router.get("/tracking/pipeline/{pipeline_run_id}")
async def get_pipeline_tracking_data(pipeline_run_id: int):
    """Get real-time tracking data for a pipeline run.
    
    Returns:
        - Total attractions
        - Data counts for each data type
        - Per-attraction breakdown
    """
    try:
        summary = data_tracking_manager.get_pipeline_detailed_summary(pipeline_run_id)
        
        if not summary:
            raise HTTPException(
                status_code=404,
                detail=f"No tracking data found for pipeline {pipeline_run_id}"
            )
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'total_attractions': summary['total_attractions'],
            'totals': summary['totals'],
            'attractions': summary['attractions']
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving tracking data: {str(e)}"
        )


@router.get("/tracking/pipeline/{pipeline_run_id}/summary")
async def get_pipeline_tracking_summary(pipeline_run_id: int):
    """Get summary statistics for a pipeline run.
    
    Returns:
        - Total attractions processed
        - Total data counts by type
        - Average data per attraction
    """
    try:
        summary = data_tracking_manager.get_pipeline_data_summary(pipeline_run_id)
        
        if not summary:
            raise HTTPException(
                status_code=404,
                detail=f"No tracking data found for pipeline {pipeline_run_id}"
            )
        
        # Calculate averages
        total_attractions = summary['total_attractions']
        averages = {
            'hero_images': round(summary['total_hero_images'] / total_attractions, 2) if total_attractions > 0 else 0,
            'reviews': round(summary['total_reviews'] / total_attractions, 2) if total_attractions > 0 else 0,
            'tips': round(summary['total_tips'] / total_attractions, 2) if total_attractions > 0 else 0,
            'social_videos': round(summary['total_social_videos'] / total_attractions, 2) if total_attractions > 0 else 0,
            'nearby_attractions': round(summary['total_nearby_attractions'] / total_attractions, 2) if total_attractions > 0 else 0,
            'audience_profiles': round(summary['total_audience_profiles'] / total_attractions, 2) if total_attractions > 0 else 0,
        }
        
        total_data_points = sum([
            summary['total_hero_images'],
            summary['total_reviews'],
            summary['total_tips'],
            summary['total_social_videos'],
            summary['total_nearby_attractions'],
            summary['total_audience_profiles']
        ])
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'total_attractions': total_attractions,
            'totals': summary,
            'averages': averages,
            'total_data_points': total_data_points
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving summary: {str(e)}"
        )


@router.get("/tracking/attraction/{pipeline_run_id}/{attraction_id}")
async def get_attraction_tracking_data(pipeline_run_id: int, attraction_id: int):
    """Get tracking data for a specific attraction.
    
    Returns:
        - All data counts for the attraction
    """
    try:
        data = data_tracking_manager.get_attraction_data_summary(pipeline_run_id, attraction_id)
        
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No tracking data found for attraction {attraction_id} in pipeline {pipeline_run_id}"
            )
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'attraction_id': attraction_id,
            'data': data
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving attraction data: {str(e)}"
        )


@router.get("/tracking/pipeline/{pipeline_run_id}/top-attractions")
async def get_top_attractions_by_data(
    pipeline_run_id: int,
    limit: int = Query(10, ge=1, le=100)
):
    """Get top attractions by total data collected.
    
    Args:
        pipeline_run_id: Pipeline run ID
        limit: Number of top attractions to return (1-100)
    
    Returns:
        - List of attractions sorted by total data count
    """
    session = SessionLocal()
    try:
        results = session.execute(text("""
            SELECT 
                a.id,
                a.name,
                adt.hero_images_count,
                adt.reviews_count,
                adt.tips_count,
                adt.social_videos_count,
                adt.nearby_attractions_count,
                adt.audience_profiles_count,
                (adt.hero_images_count + adt.reviews_count + adt.tips_count + 
                 adt.social_videos_count + adt.nearby_attractions_count + 
                 adt.audience_profiles_count) as total_data
            FROM attraction_data_tracking adt
            JOIN attractions a ON adt.attraction_id = a.id
            WHERE adt.pipeline_run_id = :pipeline_run_id
            ORDER BY total_data DESC
            LIMIT :limit
        """), {
            'pipeline_run_id': pipeline_run_id,
            'limit': limit
        }).fetchall()
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No tracking data found for pipeline {pipeline_run_id}"
            )
        
        attractions = []
        for row in results:
            attractions.append({
                'id': row[0],
                'name': row[1],
                'hero_images': row[2],
                'reviews': row[3],
                'tips': row[4],
                'social_videos': row[5],
                'nearby_attractions': row[6],
                'audience_profiles': row[7],
                'total_data': row[8]
            })
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'limit': limit,
            'count': len(attractions),
            'attractions': attractions
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving top attractions: {str(e)}"
        )


@router.get("/tracking/pipeline/{pipeline_run_id}/empty-attractions")
async def get_empty_attractions(pipeline_run_id: int):
    """Get attractions with no data collected.
    
    Returns:
        - List of attractions with zero data points
    """
    session = SessionLocal()
    try:
        results = session.execute(text("""
            SELECT 
                a.id,
                a.name
            FROM attraction_data_tracking adt
            JOIN attractions a ON adt.attraction_id = a.id
            WHERE adt.pipeline_run_id = :pipeline_run_id
              AND adt.hero_images_count = 0
              AND adt.reviews_count = 0
              AND adt.tips_count = 0
              AND adt.social_videos_count = 0
              AND adt.nearby_attractions_count = 0
              AND adt.audience_profiles_count = 0
            ORDER BY a.name
        """), {'pipeline_run_id': pipeline_run_id}).fetchall()
        
        attractions = [{'id': row[0], 'name': row[1]} for row in results]
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'count': len(attractions),
            'attractions': attractions
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving empty attractions: {str(e)}"
        )


@router.get("/tracking/pipeline/{pipeline_run_id}/stats")
async def get_pipeline_statistics(pipeline_run_id: int):
    """Get detailed statistics for a pipeline run.
    
    Returns:
        - Comprehensive statistics including min, max, average
    """
    session = SessionLocal()
    try:
        stats = session.execute(text("""
            SELECT 
                COUNT(DISTINCT attraction_id) as total_attractions,
                MIN(hero_images_count) as min_hero_images,
                MAX(hero_images_count) as max_hero_images,
                AVG(hero_images_count) as avg_hero_images,
                MIN(reviews_count) as min_reviews,
                MAX(reviews_count) as max_reviews,
                AVG(reviews_count) as avg_reviews,
                MIN(tips_count) as min_tips,
                MAX(tips_count) as max_tips,
                AVG(tips_count) as avg_tips,
                MIN(social_videos_count) as min_videos,
                MAX(social_videos_count) as max_videos,
                AVG(social_videos_count) as avg_videos,
                MIN(nearby_attractions_count) as min_nearby,
                MAX(nearby_attractions_count) as max_nearby,
                AVG(nearby_attractions_count) as avg_nearby,
                MIN(audience_profiles_count) as min_profiles,
                MAX(audience_profiles_count) as max_profiles,
                AVG(audience_profiles_count) as avg_profiles
            FROM attraction_data_tracking
            WHERE pipeline_run_id = :pipeline_run_id
        """), {'pipeline_run_id': pipeline_run_id}).fetchone()
        
        if not stats or stats[0] == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No tracking data found for pipeline {pipeline_run_id}"
            )
        
        return {
            'status': 'success',
            'pipeline_run_id': pipeline_run_id,
            'statistics': {
                'total_attractions': stats[0],
                'hero_images': {
                    'min': stats[1],
                    'max': stats[2],
                    'avg': round(stats[3], 2) if stats[3] else 0
                },
                'reviews': {
                    'min': stats[4],
                    'max': stats[5],
                    'avg': round(stats[6], 2) if stats[6] else 0
                },
                'tips': {
                    'min': stats[7],
                    'max': stats[8],
                    'avg': round(stats[9], 2) if stats[9] else 0
                },
                'social_videos': {
                    'min': stats[10],
                    'max': stats[11],
                    'avg': round(stats[12], 2) if stats[12] else 0
                },
                'nearby_attractions': {
                    'min': stats[13],
                    'max': stats[14],
                    'avg': round(stats[15], 2) if stats[15] else 0
                },
                'audience_profiles': {
                    'min': stats[16],
                    'max': stats[17],
                    'avg': round(stats[18], 2) if stats[18] else 0
                }
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving statistics: {str(e)}"
        )
