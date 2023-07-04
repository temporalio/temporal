ALTER TABLE executions_visibility ADD COLUMN history_size_bytes BIGINT NULL;
CREATE INDEX by_history_size_bytes ON executions_visibility (namespace_id, history_size_bytes, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
