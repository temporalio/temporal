ALTER TABLE executions_visibility ADD COLUMN TemporalPauseInfo JSONB GENERATED ALWAYS AS (search_attributes->'TemporalPauseInfo') STORED;
CREATE INDEX by_temporal_pause_info ON executions_visibility USING GIN (namespace_id, TemporalPauseInfo jsonb_path_ops);
