-- Add new columns
ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalLastBranchTime             TIMESTAMP GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalLastBranchTime'))             STORED,
  ADD COLUMN IF NOT EXISTS TemporalLastSupersedeTime          TIMESTAMP GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalLastSupersedeTime'))          STORED,
  ADD COLUMN IF NOT EXISTS TemporalLastOrphanedOperationsTime TIMESTAMP GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalLastOrphanedOperationsTime')) STORED;

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
        'by_temporal_last_branch_time',
        'by_temporal_last_supersede_time',
        'by_temporal_last_orphaned_operations_time'
      )
      AND NOT ix.indisvalid
  LOOP
    EXECUTE format('DROP INDEX %I', r.indexname);
    RAISE NOTICE 'Dropped invalid index %', r.indexname;
  END LOOP;
END $$;

-- Create new indices
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_last_branch_time ON executions_visibility (namespace_id, TemporalLastBranchTime, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_last_supersede_time ON executions_visibility (namespace_id, TemporalLastSupersedeTime, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_last_orphaned_operations_time ON executions_visibility (namespace_id, TemporalLastOrphanedOperationsTime, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
