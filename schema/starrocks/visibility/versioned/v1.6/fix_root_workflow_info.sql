DROP INDEX by_root_workflow_id    ON executions_visibility;
DROP INDEX by_root_run_id         ON executions_visibility;
ALTER TABLE executions_visibility DROP COLUMN root_workflow_id;
ALTER TABLE executions_visibility DROP COLUMN root_run_id;

ALTER TABLE executions_visibility ADD COLUMN root_workflow_id VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE executions_visibility ADD COLUMN root_run_id      VARCHAR(255) NOT NULL DEFAULT '';
CREATE INDEX by_root_workflow_id  ON executions_visibility (namespace_id, root_workflow_id, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_root_run_id       ON executions_visibility (namespace_id, root_run_id,      (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
