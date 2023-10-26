ALTER TABLE executions_visibility ADD COLUMN state_transition_count BIGINT NULL;
CREATE INDEX by_state_transition_count ON executions_visibility (namespace_id, state_transition_count, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
