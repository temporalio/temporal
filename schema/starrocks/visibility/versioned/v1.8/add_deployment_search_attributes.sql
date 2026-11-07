ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeploymentVersion    VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.TemporalWorkerDeploymentVersion");
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkflowVersioningBehavior VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.TemporalWorkflowVersioningBehavior");
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeployment           VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.TemporalWorkerDeployment");

CREATE INDEX by_temporal_worker_deployment_version    ON executions_visibility (namespace_id, TemporalWorkerDeploymentVersion,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, TemporalWorkflowVersioningBehavior,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment            ON executions_visibility (namespace_id, TemporalWorkerDeployment,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
