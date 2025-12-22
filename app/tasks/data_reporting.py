"""Data reporting tasks for pipeline analytics."""
import logging
from app.celery_app import celery_app
from app.core.data_tracking_manager import data_tracking_manager

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.data_reporting.get_pipeline_data_report")
def get_pipeline_data_report(pipeline_run_id: int):
    """Get a comprehensive data report for a pipeline run.
    
    Args:
        pipeline_run_id: ID of the pipeline run
        
    Returns:
        Dict with detailed data statistics
    """
    try:
        summary = data_tracking_manager.get_pipeline_detailed_summary(pipeline_run_id)
        
        if summary:
            logger.info(f"Pipeline {pipeline_run_id} data report:")
            logger.info(f"  Total attractions: {summary['total_attractions']}")
            logger.info(f"  Total hero images: {summary['totals']['hero_images']}")
            logger.info(f"  Total reviews: {summary['totals']['reviews']}")
            logger.info(f"  Total tips: {summary['totals']['tips']}")
            logger.info(f"  Total social videos: {summary['totals']['social_videos']}")
            logger.info(f"  Total nearby attractions: {summary['totals']['nearby_attractions']}")
            logger.info(f"  Total audience profiles: {summary['totals']['audience_profiles']}")
            
            return {
                'status': 'success',
                'pipeline_run_id': pipeline_run_id,
                'report': summary
            }
        else:
            return {
                'status': 'no_data',
                'pipeline_run_id': pipeline_run_id,
                'message': 'No data found for this pipeline'
            }
    except Exception as e:
        logger.error(f"Error generating data report: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@celery_app.task(name="app.tasks.data_reporting.get_attraction_data_report")
def get_attraction_data_report(pipeline_run_id: int, attraction_id: int):
    """Get data report for a specific attraction.
    
    Args:
        pipeline_run_id: ID of the pipeline run
        attraction_id: ID of the attraction
        
    Returns:
        Dict with attraction data statistics
    """
    try:
        summary = data_tracking_manager.get_attraction_data_summary(pipeline_run_id, attraction_id)
        
        if summary:
            return {
                'status': 'success',
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'data': summary
            }
        else:
            return {
                'status': 'no_data',
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'message': 'No data found for this attraction'
            }
    except Exception as e:
        logger.error(f"Error generating attraction report: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
