ALTER TABLE executions_visibility ADD COLUMN BuildIDs JSONB GENERATED ALWAYS AS (search_attributes->'BuildIDs') STORED;
CREATE INDEX by_build_ids ON executions_visibility USING GIN (namespace_id, BuildIDs jsonb_path_ops);
