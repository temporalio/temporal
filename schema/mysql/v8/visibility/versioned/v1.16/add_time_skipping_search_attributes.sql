ALTER TABLE executions_visibility
ADD COLUMN TemporalTimeSkippingState VARCHAR(255)
GENERATED ALWAYS AS (JSON_UNQUOTE(search_attributes->"$.TemporalTimeSkippingState"));

CREATE INDEX by_temporal_time_skipping_state
ON executions_visibility (
    namespace_id,
    TemporalTimeSkippingState,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);
