ALTER TABLE executions_visibility ADD COLUMN PausedInfo JSON GENERATED ALWAYS AS (search_attributes->'$.PausedInfo');
CREATE INDEX by_paused_info ON executions_visibility (namespace_id, (CAST(PausedInfo AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
