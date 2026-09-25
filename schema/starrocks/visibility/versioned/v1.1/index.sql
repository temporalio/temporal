CREATE INDEX by_close_time_by_status ON executions_visibility (namespace_id, close_time DESC, run_id, status);
