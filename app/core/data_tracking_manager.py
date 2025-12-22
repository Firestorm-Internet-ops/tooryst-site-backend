"""Attraction data tracking manager."""
import logging
from sqlalchemy import text
from app.infrastructure.persistence.db import SessionLocal

logger = logging.getLogger(__name__)


class DataTrackingManager:
    """Manages tracking of data added for each attraction."""

    @staticmethod
    def create_tracking_record(pipeline_run_id: int, attraction_id: int):
        """Create a new tracking record for an attraction.
        
        Args:
            pipeline_run_id: ID of the pipeline run
            attraction_id: ID of the attraction
        """
        session = SessionLocal()
        try:
            session.execute(text("""
                INSERT INTO attraction_data_tracking 
                (pipeline_run_id, attraction_id)
                VALUES (:pipeline_run_id, :attraction_id)
                ON DUPLICATE KEY UPDATE
                    updated_at = CURRENT_TIMESTAMP
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to create tracking record: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def update_hero_images_count(pipeline_run_id: int, attraction_id: int, count: int):
        """Update hero images count for an attraction.
        
        Args:
            pipeline_run_id: ID of the pipeline run
            attraction_id: ID of the attraction
            count: Number of hero images
        """
        session = SessionLocal()
        try:
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET hero_images_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'count': count
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to update hero images count: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def update_reviews_count(pipeline_run_id: int, attraction_id: int, count: int):
        """Update reviews count for an attraction."""
        session = SessionLocal()
        try:
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET reviews_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'count': count
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to update reviews count: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def update_tips_count(pipeline_run_id: int, attraction_id: int, count: int):
        """Update tips count for an attraction."""
        session = SessionLocal()
        try:
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET tips_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'count': count
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to update tips count: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def update_social_videos_count(pipeline_run_id: int, attraction_id: int, count: int):
        """Update social videos count for an attraction."""
        session = SessionLocal()
        try:
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET social_videos_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'count': count
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to update social videos count: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def update_nearby_attractions_count(pipeline_run_id: int, attraction_id: int, count: int):
        """Update nearby attractions count for an attraction."""
        session = SessionLocal()
        try:
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET nearby_attractions_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'count': count
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to update nearby attractions count: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def update_audience_profiles_count(pipeline_run_id: int, attraction_id: int, count: int):
        """Update audience profiles count for an attraction."""
        session = SessionLocal()
        try:
            session.execute(text("""
                UPDATE attraction_data_tracking
                SET audience_profiles_count = :count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id,
                'count': count
            })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to update audience profiles count: {e}")
            session.rollback()
        finally:
            session.close()

    @staticmethod
    def get_attraction_data_summary(pipeline_run_id: int, attraction_id: int):
        """Get data summary for an attraction.
        
        Returns:
            Dict with all data counts
        """
        session = SessionLocal()
        try:
            result = session.execute(text("""
                SELECT 
                    hero_images_count,
                    reviews_count,
                    tips_count,
                    social_videos_count,
                    nearby_attractions_count,
                    audience_profiles_count
                FROM attraction_data_tracking
                WHERE pipeline_run_id = :pipeline_run_id
                  AND attraction_id = :attraction_id
            """), {
                'pipeline_run_id': pipeline_run_id,
                'attraction_id': attraction_id
            }).fetchone()
            
            if result:
                return {
                    'hero_images': result[0],
                    'reviews': result[1],
                    'tips': result[2],
                    'social_videos': result[3],
                    'nearby_attractions': result[4],
                    'audience_profiles': result[5]
                }
            return None
        finally:
            session.close()

    @staticmethod
    def get_pipeline_data_summary(pipeline_run_id: int):
        """Get data summary for entire pipeline.
        
        Returns:
            Dict with total counts and per-attraction breakdown
        """
        session = SessionLocal()
        try:
            # Get totals
            totals = session.execute(text("""
                SELECT 
                    COUNT(DISTINCT attraction_id) as total_attractions,
                    SUM(hero_images_count) as total_hero_images,
                    SUM(reviews_count) as total_reviews,
                    SUM(tips_count) as total_tips,
                    SUM(social_videos_count) as total_social_videos,
                    SUM(nearby_attractions_count) as total_nearby_attractions,
                    SUM(audience_profiles_count) as total_audience_profiles
                FROM attraction_data_tracking
                WHERE pipeline_run_id = :pipeline_run_id
            """), {'pipeline_run_id': pipeline_run_id}).fetchone()
            
            if totals:
                return {
                    'total_attractions': totals[0],
                    'total_hero_images': totals[1] or 0,
                    'total_reviews': totals[2] or 0,
                    'total_tips': totals[3] or 0,
                    'total_social_videos': totals[4] or 0,
                    'total_nearby_attractions': totals[5] or 0,
                    'total_audience_profiles': totals[6] or 0
                }
            return None
        finally:
            session.close()

    @staticmethod
    def get_pipeline_detailed_summary(pipeline_run_id: int):
        """Get detailed summary for entire pipeline with per-attraction breakdown.
        
        Returns:
            Dict with totals and list of attractions with their data counts
        """
        session = SessionLocal()
        try:
            # Get per-attraction details
            details = session.execute(text("""
                SELECT 
                    a.id,
                    a.name,
                    adt.hero_images_count,
                    adt.reviews_count,
                    adt.tips_count,
                    adt.social_videos_count,
                    adt.nearby_attractions_count,
                    adt.audience_profiles_count
                FROM attraction_data_tracking adt
                JOIN attractions a ON adt.attraction_id = a.id
                WHERE adt.pipeline_run_id = :pipeline_run_id
                ORDER BY a.name
            """), {'pipeline_run_id': pipeline_run_id}).fetchall()
            
            if details:
                attractions = []
                totals = {
                    'hero_images': 0,
                    'reviews': 0,
                    'tips': 0,
                    'social_videos': 0,
                    'nearby_attractions': 0,
                    'audience_profiles': 0
                }
                
                for row in details:
                    attraction_data = {
                        'id': row[0],
                        'name': row[1],
                        'hero_images': row[2],
                        'reviews': row[3],
                        'tips': row[4],
                        'social_videos': row[5],
                        'nearby_attractions': row[6],
                        'audience_profiles': row[7]
                    }
                    attractions.append(attraction_data)
                    
                    # Add to totals
                    totals['hero_images'] += row[2]
                    totals['reviews'] += row[3]
                    totals['tips'] += row[4]
                    totals['social_videos'] += row[5]
                    totals['nearby_attractions'] += row[6]
                    totals['audience_profiles'] += row[7]
                
                return {
                    'total_attractions': len(attractions),
                    'totals': totals,
                    'attractions': attractions
                }
            return None
        finally:
            session.close()


data_tracking_manager = DataTrackingManager()
