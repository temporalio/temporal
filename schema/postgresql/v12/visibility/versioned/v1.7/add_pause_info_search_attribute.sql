ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalPauseInfo JSONB
    GENERATED ALWAYS AS (search_attributes->'TemporalPauseInfo') STORED,
  ADD COLUMN IF NOT EXISTS TemporalWorkerDeploymentVersion VARCHAR(255)
    GENERATED ALWAYS AS (search_attributes->>'TemporalWorkerDeploymentVersion') STORED,
  ADD COLUMN IF NOT EXISTS TemporalWorkflowVersioningBehavior VARCHAR(255)
    GENERATED ALWAYS AS (search_attributes->>'TemporalWorkflowVersioningBehavior') STORED,
  ADD COLUMN IF NOT EXISTS TemporalWorkerDeployment VARCHAR(255)
    GENERATED ALWAYS AS (search_attributes->>'TemporalWorkerDeployment') STORED;
CREATE INDEX by_temporal_pause_info ON executions_visibility USING GIN (namespace_id, TemporalPauseInfo jsonb_path_ops);
