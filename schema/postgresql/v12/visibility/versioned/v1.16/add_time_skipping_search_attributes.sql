-- Add new columns
ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalTimeSkippingEnabled                    BOOLEAN GENERATED ALWAYS AS ((search_attributes->'TemporalTimeSkippingEnabled')::boolean)                   STORED,
  ADD COLUMN IF NOT EXISTS TemporalTimeSkippingInitialSkippedDuration     BIGINT  GENERATED ALWAYS AS ((search_attributes->'TemporalTimeSkippingInitialSkippedDuration')::bigint)      STORED,
  ADD COLUMN IF NOT EXISTS TemporalTimeSkippingAccumulatedSkippedDuration BIGINT  GENERATED ALWAYS AS ((search_attributes->'TemporalTimeSkippingAccumulatedSkippedDuration')::bigint)  STORED;

-- Drop invalid indices
DO LANGUAGE 'plpgsql' $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT i.relname as indexname
    FROM
      pg_class i,
      pg_index ix
    WHERE
      i.oid = ix.indexrelid
      AND ix.indrelid = (SELECT oid FROM pg_class WHERE relname = 'executions_visibility')
      AND i.relname IN (
        'by_temporal_time_skipping_enabled',
        'by_temporal_time_skipping_initial_skipped_duration',
        'by_temporal_time_skipping_accumulated_skipped_duration'
      )
      AND NOT ix.indisvalid
  LOOP
    EXECUTE format('DROP INDEX %I', r.indexname);
    RAISE NOTICE 'Dropped invalid index %', r.indexname;
  END LOOP;
END $$;

-- Create new indices
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_time_skipping_enabled ON executions_visibility (namespace_id, TemporalTimeSkippingEnabled, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_time_skipping_initial_skipped_duration ON executions_visibility (namespace_id, TemporalTimeSkippingInitialSkippedDuration, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_time_skipping_accumulated_skipped_duration ON executions_visibility (namespace_id, TemporalTimeSkippingAccumulatedSkippedDuration, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
