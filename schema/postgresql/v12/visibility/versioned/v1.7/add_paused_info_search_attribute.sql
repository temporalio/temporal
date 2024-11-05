ALTER TABLE executions_visibility ADD COLUMN PausedInfo JSONB GENERATED ALWAYS AS (search_attributes->'PausedInfo') STORED;
CREATE INDEX by_paused_info ON executions_visibility USING GIN (namespace_id, PausedInfo jsonb_path_ops);

