-- Create pipeline checkpoints table for resumable pipelines
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
