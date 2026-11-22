ALTER TABLE executions_visibility
ADD COLUMN TemporalTimeSkippingEnabled BOOLEAN
GENERATED ALWAYS AS (search_attributes->"$.TemporalTimeSkippingEnabled");

ALTER TABLE executions_visibility
ADD COLUMN TemporalTimeSkippingInitialSkippedDuration BIGINT
GENERATED ALWAYS AS (search_attributes->"$.TemporalTimeSkippingInitialSkippedDuration");

ALTER TABLE executions_visibility
ADD COLUMN TemporalTimeSkippingAccumulatedSkippedDuration BIGINT
GENERATED ALWAYS AS (search_attributes->"$.TemporalTimeSkippingAccumulatedSkippedDuration");

CREATE INDEX by_temporal_time_skipping_enabled
ON executions_visibility (
    namespace_id,
    TemporalTimeSkippingEnabled,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);

CREATE INDEX by_temporal_time_skipping_initial_skipped_duration
ON executions_visibility (
    namespace_id,
    TemporalTimeSkippingInitialSkippedDuration,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);

CREATE INDEX by_temporal_time_skipping_accumulated_skipped_duration
ON executions_visibility (
    namespace_id,
    TemporalTimeSkippingAccumulatedSkippedDuration,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);
