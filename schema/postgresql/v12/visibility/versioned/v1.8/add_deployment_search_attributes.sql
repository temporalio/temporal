ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeploymentVersion    JSONB GENERATED ALWAYS AS (search_attributes->'TemporalWorkerDeploymentVersion') STORED;
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkflowVersioningBehavior JSONB GENERATED ALWAYS AS (search_attributes->'TemporalWorkflowVersioningBehavior') STORED;
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeployment           JSONB GENERATED ALWAYS AS (search_attributes->'TemporalWorkerDeployment') STORED;

CREATE INDEX by_temporal_worker_deployment_version ON executions_visibility USING GIN (namespace_id, TemporalWorkerDeploymentVersion jsonb_path_ops);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility USING GIN (namespace_id, TemporalWorkflowVersioningBehavior jsonb_path_ops);
CREATE INDEX by_temporal_worker_deployment ON executions_visibility USING GIN (namespace_id, TemporalWorkerDeployment jsonb_path_ops);
