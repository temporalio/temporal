-- Add new column
ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalLastBranchTime TIMESTAMP GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalLastBranchTime')) STORED;

-- Drop invalid index
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
        'by_temporal_last_branch_time'
      )
      AND NOT ix.indisvalid
  LOOP
    EXECUTE format('DROP INDEX %I', r.indexname);
    RAISE NOTICE 'Dropped invalid index %', r.indexname;
  END LOOP;
END $$;

-- Create new index
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_last_branch_time ON executions_visibility (namespace_id, TemporalLastBranchTime, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
