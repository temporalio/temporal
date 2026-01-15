ALTER TABLE executions_visibility
ADD COLUMN TemporalExternalPayloadSizeBytes BIGINT 
GENERATED ALWAYS AS (search_attributes->"$.TemporalExternalPayloadSizeBytes");

ALTER TABLE executions_visibility
ADD COLUMN TemporalExternalPayloadCount BIGINT 
GENERATED ALWAYS AS (search_attributes->"$.TemporalExternalPayloadCount");

CREATE INDEX by_temporal_external_payload_size_bytes 
ON executions_visibility (
    namespace_id, 
    TemporalExternalPayloadSizeBytes, 
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, 
    start_time DESC, 
    run_id
);

CREATE INDEX by_temporal_external_payload_count 
ON executions_visibility (
    namespace_id, 
    TemporalExternalPayloadCount, 
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, 
    start_time DESC, 
    run_id
); 