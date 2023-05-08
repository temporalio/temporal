ALTER TABLE executions_visibility ADD COLUMN BuildIds JSONB GENERATED ALWAYS AS (search_attributes->'BuildIds') STORED;
CREATE INDEX by_build_ids ON executions_visibility USING GIN (namespace_id, BuildIds jsonb_path_ops);
