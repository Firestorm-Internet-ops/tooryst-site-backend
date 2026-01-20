-- Migration: Add GCS storage columns to hero_images table
-- Date: 2026-01-20
-- Description: Support hybrid image storage with Google Cloud Storage

-- Add new columns for GCS integration
ALTER TABLE hero_images
ADD COLUMN google_photo_reference VARCHAR(512) NULL COMMENT 'Google Places photo.name (e.g., places/{place_id}/photos/{photo_id})',
ADD COLUMN gcs_url_card VARCHAR(512) NULL COMMENT 'GCS URL for 400px card version',
ADD COLUMN gcs_url_hero VARCHAR(512) NULL COMMENT 'GCS URL for 1600px hero version',
ADD COLUMN last_refreshed_at TIMESTAMP NULL DEFAULT NULL COMMENT 'When image was last downloaded from Google';

-- Add index for efficient querying of stale images
ALTER TABLE hero_images
ADD INDEX idx_hero_images_last_refreshed (last_refreshed_at);

-- Verify columns were added
-- SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_COMMENT
-- FROM INFORMATION_SCHEMA.COLUMNS
-- WHERE TABLE_NAME = 'hero_images'
-- AND COLUMN_NAME IN ('google_photo_reference', 'gcs_url_card', 'gcs_url_hero', 'last_refreshed_at');
