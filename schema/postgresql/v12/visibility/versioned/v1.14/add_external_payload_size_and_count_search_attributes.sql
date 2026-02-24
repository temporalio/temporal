ALTER TABLE executions_visibility
ADD COLUMN TemporalExternalPayloadSizeBytes BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadSizeBytes')::bigint) STORED;

ALTER TABLE executions_visibility
ADD COLUMN TemporalExternalPayloadCount BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadCount')::bigint) STORED;

CREATE INDEX by_temporal_external_payload_size_bytes ON executions_visibility (namespace_id, TemporalExternalPayloadSizeBytes, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);

CREATE INDEX by_temporal_external_payload_count ON executions_visibility (namespace_id, TemporalExternalPayloadCount, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id); 
