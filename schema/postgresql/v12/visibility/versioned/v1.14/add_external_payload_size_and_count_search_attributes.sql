ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalExternalPayloadSizeBytes BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadSizeBytes')::bigint)  STORED,
  ADD COLUMN IF NOT EXISTS TemporalExternalPayloadCount     BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadCount')::bigint)      STORED;

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
        'by_temporal_external_payload_size_bytes',
        'by_temporal_external_payload_count'
      )
      AND NOT ix.indisvalid
  LOOP
    EXECUTE format('DROP INDEX %I', r.indexname);
    RAISE NOTICE 'Dropped invalid index %', r.indexname;
  END LOOP;
END $$;


CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_external_payload_size_bytes ON executions_visibility (namespace_id, TemporalExternalPayloadSizeBytes, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_external_payload_count      ON executions_visibility (namespace_id, TemporalExternalPayloadCount,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
