ALTER TABLE executions_visibility ADD COLUMN PausedEntities JSONB GENERATED ALWAYS AS (search_attributes->'PausedEntities') STORED;
CREATE INDEX by_paused_entities ON executions_visibility USING GIN (namespace_id, PausedEntities jsonb_path_ops);

