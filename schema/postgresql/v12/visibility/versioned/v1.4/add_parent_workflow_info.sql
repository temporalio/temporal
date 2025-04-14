ALTER TABLE executions_visibility ADD COLUMN parent_workflow_id VARCHAR(255) NULL;
ALTER TABLE executions_visibility ADD COLUMN parent_run_id      VARCHAR(255) NULL;
CREATE INDEX by_parent_workflow_id  ON executions_visibility (namespace_id, parent_workflow_id, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_run_id       ON executions_visibility (namespace_id, parent_run_id,      (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
