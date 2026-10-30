-- Add new columns
ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalPriorityKey BIGINT       GENERATED ALWAYS AS ((search_attributes->'TemporalPriorityKey')::bigint) STORED,
  ADD COLUMN IF NOT EXISTS TemporalFairnessKey VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalFairnessKey')          STORED;

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
        'by_temporal_priority_key',
        'by_temporal_fairness_key'
      )
      AND NOT ix.indisvalid
  LOOP
    EXECUTE format('DROP INDEX %I', r.indexname);
    RAISE NOTICE 'Dropped invalid index %', r.indexname;
  END LOOP;
END $$;

-- Create new indices
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_priority_key ON executions_visibility (namespace_id, TemporalPriorityKey, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_fairness_key ON executions_visibility (namespace_id, TemporalFairnessKey, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
