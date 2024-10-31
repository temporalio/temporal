ALTER TABLE executions_visibility ADD COLUMN PauseActivityTypes JSON GENERATED ALWAYS AS (search_attributes->'$.PauseActivityTypes');
CREATE INDEX by_paused_activity_types ON executions_visibility (namespace_id, (CAST(PauseActivityTypes AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
