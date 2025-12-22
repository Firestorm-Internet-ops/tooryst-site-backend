-- Add completed_at column to pipeline_runs table
ALTER TABLE pipeline_runs 
ADD COLUMN completed_at TIMESTAMP NULL DEFAULT NULL;
