ALTER TABLE executions_visibility
ADD COLUMN TemporalScheduleNextActionTime DATETIME(6)
GENERATED ALWAYS AS (
  CONVERT_TZ(
    REGEXP_REPLACE(search_attributes->>"$.TemporalScheduleNextActionTime", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
    SUBSTR(REPLACE(search_attributes->>"$.TemporalScheduleNextActionTime", 'Z', '+00:00'), -6, 6),
    '+00:00'
  )
);

CREATE INDEX by_temporal_schedule_next_action_time
ON executions_visibility (
    namespace_id,
    TemporalScheduleNextActionTime,
    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC,
    start_time DESC,
    run_id
);
