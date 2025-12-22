-- Create table to track data added for each attraction
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
