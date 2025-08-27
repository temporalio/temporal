ALTER TABLE executions_visibility ADD COLUMN TemporalReportedProblems JSONB GENERATED ALWAYS AS (search_attributes->'TemporalReportedProblems') STORED;
CREATE INDEX by_temporal_reported_problems ON executions_visibility USING GIN (namespace_id, TemporalReportedProblems jsonb_path_ops);
