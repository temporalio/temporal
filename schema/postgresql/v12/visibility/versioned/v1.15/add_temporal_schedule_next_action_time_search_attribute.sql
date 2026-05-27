ALTER TABLE executions_visibility
ADD COLUMN TemporalScheduleNextActionTime TIMESTAMP GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalScheduleNextActionTime')) STORED;

CREATE INDEX by_temporal_schedule_next_action_time ON executions_visibility (namespace_id, TemporalScheduleNextActionTime, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
