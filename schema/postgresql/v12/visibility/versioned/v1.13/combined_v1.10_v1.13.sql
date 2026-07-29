-- Add new columns
ALTER TABLE executions_visibility
  -- v1.10
  ADD COLUMN IF NOT EXISTS TemporalReportedProblems JSONB GENERATED ALWAYS AS (search_attributes->'TemporalReportedProblems') STORED,

  -- v1.11
  -- Pre-allocated CHASM search attributes
  ADD COLUMN IF NOT EXISTS TemporalBool01         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'TemporalBool01')::boolean)        STORED,
  ADD COLUMN IF NOT EXISTS TemporalBool02         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'TemporalBool02')::boolean)        STORED,
  ADD COLUMN IF NOT EXISTS TemporalDatetime01     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalDatetime01'))  STORED,
  ADD COLUMN IF NOT EXISTS TemporalDatetime02     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalDatetime02'))  STORED,
  ADD COLUMN IF NOT EXISTS TemporalDouble01       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'TemporalDouble01')::decimal)      STORED,
  ADD COLUMN IF NOT EXISTS TemporalDouble02       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'TemporalDouble02')::decimal)      STORED,
  ADD COLUMN IF NOT EXISTS TemporalInt01          BIGINT          GENERATED ALWAYS AS ((search_attributes->'TemporalInt01')::bigint)          STORED,
  ADD COLUMN IF NOT EXISTS TemporalInt02          BIGINT          GENERATED ALWAYS AS ((search_attributes->'TemporalInt02')::bigint)          STORED,
  ADD COLUMN IF NOT EXISTS TemporalKeyword01      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword01')               STORED,
  ADD COLUMN IF NOT EXISTS TemporalKeyword02      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword02')               STORED,
  ADD COLUMN IF NOT EXISTS TemporalKeyword03      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword03')               STORED,
  ADD COLUMN IF NOT EXISTS TemporalKeyword04      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword04')               STORED,
  ADD COLUMN IF NOT EXISTS TemporalKeywordList01  JSONB           GENERATED ALWAYS AS (search_attributes->'TemporalKeywordList01')            STORED,
  ADD COLUMN IF NOT EXISTS TemporalKeywordList02  JSONB           GENERATED ALWAYS AS (search_attributes->'TemporalKeywordList02')            STORED,

  -- v1.12
  ADD COLUMN IF NOT EXISTS TemporalLowCardinalityKeyword01 VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>'TemporalLowCardinalityKeyword01') STORED,

  -- v1.13
  ADD COLUMN IF NOT EXISTS TemporalUsedWorkerDeploymentVersions JSONB GENERATED ALWAYS AS (search_attributes->'TemporalUsedWorkerDeploymentVersions') STORED;

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
        'by_temporal_reported_problems',
        'by_temporal_bool_01',
        'by_temporal_bool_02',
        'by_temporal_datetime_01',
        'by_temporal_datetime_02',
        'by_temporal_double_01',
        'by_temporal_double_02',
        'by_temporal_int_01',
        'by_temporal_int_02',
        'by_temporal_keyword_01',
        'by_temporal_keyword_02',
        'by_temporal_keyword_03',
        'by_temporal_keyword_04',
        'by_temporal_keyword_list_01',
        'by_temporal_keyword_list_02',
        'by_temporal_low_cardinality_keyword_01',
        'by_used_deployment_versions'
      )
      AND NOT ix.indisvalid
  LOOP
    EXECUTE format('DROP INDEX %I', r.indexname);
    RAISE NOTICE 'Dropped invalid index %', r.indexname;
  END LOOP;
END $$;

-- Create new indices

-- v1.10
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_reported_problems ON executions_visibility USING GIN (namespace_id, TemporalReportedProblems jsonb_path_ops);

-- v1.11
-- Indexes for the pre-allocated CHASM search attributes
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_bool_01          ON executions_visibility (namespace_id, TemporalBool01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_bool_02          ON executions_visibility (namespace_id, TemporalBool02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_datetime_01      ON executions_visibility (namespace_id, TemporalDatetime01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_datetime_02      ON executions_visibility (namespace_id, TemporalDatetime02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_double_01        ON executions_visibility (namespace_id, TemporalDouble01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_double_02        ON executions_visibility (namespace_id, TemporalDouble02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_int_01           ON executions_visibility (namespace_id, TemporalInt01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_int_02           ON executions_visibility (namespace_id, TemporalInt02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_keyword_01       ON executions_visibility (namespace_id, TemporalKeyword01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_keyword_02       ON executions_visibility (namespace_id, TemporalKeyword02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_keyword_03       ON executions_visibility (namespace_id, TemporalKeyword03, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_keyword_04       ON executions_visibility (namespace_id, TemporalKeyword04, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_keyword_list_01  ON executions_visibility USING GIN (namespace_id, TemporalKeywordList01 jsonb_path_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_keyword_list_02  ON executions_visibility USING GIN (namespace_id, TemporalKeywordList02 jsonb_path_ops);

-- v1.12
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_temporal_low_cardinality_keyword_01 ON executions_visibility (namespace_id, TemporalLowCardinalityKeyword01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);

-- v1.13
CREATE INDEX CONCURRENTLY IF NOT EXISTS by_used_deployment_versions ON executions_visibility USING GIN (namespace_id, TemporalUsedWorkerDeploymentVersions jsonb_path_ops);
