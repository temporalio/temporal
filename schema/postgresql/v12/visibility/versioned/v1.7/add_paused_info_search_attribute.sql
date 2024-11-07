ALTER TABLE executions_visibility ADD COLUMN TemporalPausedInfo JSONB GENERATED ALWAYS AS (search_attributes->'TemporalPausedInfo') STORED;
CREATE INDEX by_temporal_paused_info ON executions_visibility USING GIN (namespace_id, TemporalPausedInfo jsonb_path_ops);

