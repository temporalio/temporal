ALTER TABLE executions_visibility
ADD COLUMN TemporalTimeSkippingEnabled BOOLEAN
GENERATED ALWAYS AS (search_attributes->"$.TemporalTimeSkippingEnabled");

ALTER TABLE executions_visibility
ADD COLUMN TemporalTimeSkippingOnceEnabled BOOLEAN
GENERATED ALWAYS AS (search_attributes->"$.TemporalTimeSkippingOnceEnabled");

CREATE INDEX by_temporal_time_skipping_enabled
ON executions_visibility (
    namespace_id,
    TemporalTimeSkippingEnabled,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);

CREATE INDEX by_temporal_time_skipping_once_enabled
ON executions_visibility (
    namespace_id,
    TemporalTimeSkippingOnceEnabled,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);
