ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeploymentVersion JSON GENERATED ALWAYS AS (search_attributes->'$.TemporalWorkerDeploymentVersion');
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkflowVersioningBehavior JSON GENERATED ALWAYS AS (search_attributes->'$.TemporalWorkflowVersioningBehavior');
ALTER TABLE executions_visibility ADD COLUMN TemporalWorkerDeployment JSON GENERATED ALWAYS AS (search_attributes->'$.TemporalWorkerDeployment');

CREATE INDEX by_temporal_worker_deployment_version    ON executions_visibility (namespace_id, (CAST(TemporalWorkerDeploymentVersion    AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, (CAST(TemporalWorkflowVersioningBehavior AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment            ON executions_visibility (namespace_id, (CAST(TemporalWorkerDeployment           AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
