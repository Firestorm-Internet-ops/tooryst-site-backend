-- ============================================================================
-- PIPELINE SCHEMA - Complete Database Setup
-- ============================================================================
-- This file contains all SQL migrations needed for the pipeline system.
-- Run this file once to set up all tables and indexes.
-- ============================================================================

-- ============================================================================
-- 1. PIPELINE CHECKPOINTS TABLE
-- ============================================================================
-- Tracks which stages are completed for each attraction in a pipeline run
-- Used for resumable pipelines after disconnects
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    pipeline_run_id BIGINT UNSIGNED NOT NULL,
    attraction_id BIGINT UNSIGNED NOT NULL,
    stage_name VARCHAR(50) NOT NULL,
    status ENUM('completed', 'failed', 'skipped') NOT NULL DEFAULT 'completed',
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicate checkpoints
    UNIQUE KEY unique_checkpoint (pipeline_run_id, attraction_id, stage_name),
    
    -- Indexes for efficient queries
    INDEX idx_pipeline_run (pipeline_run_id),
    INDEX idx_attraction (attraction_id),
    INDEX idx_stage (stage_name),
    INDEX idx_status (status),
    INDEX idx_pipeline_attraction (pipeline_run_id, attraction_id),
    
    -- Foreign key to pipeline_runs
    CONSTRAINT fk_checkpoint_pipeline FOREIGN KEY (pipeline_run_id) 
        REFERENCES pipeline_runs(id) ON DELETE CASCADE
);

-- ============================================================================
-- 2. ATTRACTION DATA TRACKING TABLE
-- ============================================================================
-- Tracks the amount of data added for each attraction during pipeline execution
-- Includes counts for all data types: images, reviews, tips, videos, etc.
-- ============================================================================

CREATE TABLE IF NOT EXISTS attraction_data_tracking (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    attraction_id BIGINT UNSIGNED NOT NULL,
    pipeline_run_id BIGINT UNSIGNED NOT NULL,
    
    -- Data counts
    hero_images_count INT UNSIGNED DEFAULT 0,
    reviews_count INT UNSIGNED DEFAULT 0,
    tips_count INT UNSIGNED DEFAULT 0,
    social_videos_count INT UNSIGNED DEFAULT 0,
    nearby_attractions_count INT UNSIGNED DEFAULT 0,
    audience_profiles_count INT UNSIGNED DEFAULT 0,
    
    -- Metadata
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE KEY unique_tracking (pipeline_run_id, attraction_id),
    
    -- Indexes for efficient queries
    INDEX idx_pipeline_run (pipeline_run_id),
    INDEX idx_attraction (attraction_id),
    INDEX idx_created_at (created_at),
    
    -- Foreign keys
    CONSTRAINT fk_tracking_pipeline FOREIGN KEY (pipeline_run_id) 
        REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    CONSTRAINT fk_tracking_attraction FOREIGN KEY (attraction_id) 
        REFERENCES attractions(id) ON DELETE CASCADE
);

-- ============================================================================
-- 3. ADD COMPLETED_AT COLUMN TO PIPELINE_RUNS
-- ============================================================================
-- Tracks when a pipeline run was completed
-- ============================================================================

ALTER TABLE pipeline_runs 
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP NULL DEFAULT NULL;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
-- All tables have been created successfully!
-- The pipeline system is ready to use.
-- ============================================================================
