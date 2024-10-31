ALTER TABLE executions_visibility ADD COLUMN PauseActivityTypes JSONB GENERATED ALWAYS AS (search_attributes->'PauseActivityTypes') STORED;
CREATE INDEX by_paused_activity_types ON executions_visibility USING GIN (namespace_id, PauseActivityTypes jsonb_path_ops);

