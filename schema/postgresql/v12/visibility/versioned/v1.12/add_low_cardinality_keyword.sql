ALTER TABLE executions_visibility ADD COLUMN TemporalLowCardinalityKeyword01 VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalLowCardinalityKeyword01') STORED;
CREATE INDEX by_temporal_low_cardinality_keyword_01 ON executions_visibility (namespace_id, TemporalLowCardinalityKeyword01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);

