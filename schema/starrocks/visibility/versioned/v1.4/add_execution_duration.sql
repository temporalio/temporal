ALTER TABLE executions_visibility ADD COLUMN execution_duration BIGINT NULL;
CREATE INDEX by_execution_duration ON executions_visibility (namespace_id, execution_duration, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
