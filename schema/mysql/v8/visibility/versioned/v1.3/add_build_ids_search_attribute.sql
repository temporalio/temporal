ALTER TABLE executions_visibility ADD COLUMN BuildIDs JSON GENERATED ALWAYS AS (search_attributes->'BuildIDs');
CREATE INDEX by_build_ids ON executions_visibility (namespace_id, (CAST(BuildIDs AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
