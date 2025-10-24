ALTER TABLE executions_visibility ADD COLUMN TemporalPauseInfo JSON GENERATED ALWAYS AS (search_attributes->'$.TemporalPauseInfo');
CREATE INDEX by_temporal_pause_info ON executions_visibility (namespace_id, (CAST(TemporalPauseInfo AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
