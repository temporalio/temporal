ALTER TABLE executions_visibility
ADD COLUMN TemporalUsedWorkerDeploymentVersions JSONB
GENERATED ALWAYS AS (search_attributes->'TemporalUsedWorkerDeploymentVersions') STORED;

CREATE INDEX by_used_deployment_versions
ON executions_visibility
USING GIN (namespace_id, TemporalUsedWorkerDeploymentVersions jsonb_path_ops);
