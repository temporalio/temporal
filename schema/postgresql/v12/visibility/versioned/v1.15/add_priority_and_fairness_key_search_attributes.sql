ALTER TABLE executions_visibility
ADD COLUMN TemporalPriorityKey BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalPriorityKey')::bigint) STORED;

ALTER TABLE executions_visibility
ADD COLUMN TemporalFairnessKey VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalFairnessKey') STORED;

-- TemporalFairnessKey is intentionally not indexed: fairness keys are high cardinality.
CREATE INDEX by_temporal_priority_key ON executions_visibility (namespace_id, TemporalPriorityKey, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
