ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeploymentVersion    VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalWorkerDeploymentVersion') STORED;
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkflowVersioningBehavior VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalWorkflowVersioningBehavior') STORED;
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeployment           VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalWorkerDeployment') STORED;

CREATE INDEX by_temporal_worker_deployment_version ON executions_visibility (namespace_id, TemporalWorkerDeploymentVersion, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, TemporalWorkflowVersioningBehavior, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment ON executions_visibility (namespace_id, TemporalWorkerDeployment, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
