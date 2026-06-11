ALTER TABLE executions_visibility ADD COLUMN parent_workflow_id VARCHAR(255) NULL;
ALTER TABLE executions_visibility ADD COLUMN parent_run_id      VARCHAR(255) NULL;
CREATE INDEX by_parent_workflow_id  ON executions_visibility (namespace_id, parent_workflow_id, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_run_id       ON executions_visibility (namespace_id, parent_run_id,      (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
