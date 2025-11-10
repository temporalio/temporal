-- Pre-allocated CHASM search attributes
ALTER TABLE executions_visibility
  ADD COLUMN TemporalBool01         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'TemporalBool01')::boolean)        STORED,
  ADD COLUMN TemporalBool02         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'TemporalBool02')::boolean)        STORED,
  ADD COLUMN TemporalDatetime01     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalDatetime01'))  STORED,
  ADD COLUMN TemporalDatetime02     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalDatetime02'))  STORED,
  ADD COLUMN TemporalDouble01       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'TemporalDouble01')::decimal)      STORED,
  ADD COLUMN TemporalDouble02       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'TemporalDouble02')::decimal)      STORED,
  ADD COLUMN TemporalInt01          BIGINT          GENERATED ALWAYS AS ((search_attributes->'TemporalInt01')::bigint)          STORED,
  ADD COLUMN TemporalInt02          BIGINT          GENERATED ALWAYS AS ((search_attributes->'TemporalInt02')::bigint)          STORED,
  ADD COLUMN TemporalKeyword01      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword01')               STORED,
  ADD COLUMN TemporalKeyword02      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword02')               STORED,
  ADD COLUMN TemporalKeyword03      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword03')               STORED,
  ADD COLUMN TemporalKeyword04      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword04')               STORED,
  ADD COLUMN TemporalKeywordList01  JSONB           GENERATED ALWAYS AS (search_attributes->'TemporalKeywordList01')            STORED,
  ADD COLUMN TemporalKeywordList02  JSONB           GENERATED ALWAYS AS (search_attributes->'TemporalKeywordList02')            STORED;

-- Indexes for the pre-allocated CHASM search attributes
CREATE INDEX by_temporal_bool_01          ON executions_visibility (namespace_id, TemporalBool01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_bool_02          ON executions_visibility (namespace_id, TemporalBool02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_datetime_01      ON executions_visibility (namespace_id, TemporalDatetime01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_datetime_02      ON executions_visibility (namespace_id, TemporalDatetime02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_double_01        ON executions_visibility (namespace_id, TemporalDouble01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_double_02        ON executions_visibility (namespace_id, TemporalDouble02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_int_01           ON executions_visibility (namespace_id, TemporalInt01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_int_02           ON executions_visibility (namespace_id, TemporalInt02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_01       ON executions_visibility (namespace_id, TemporalKeyword01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_02       ON executions_visibility (namespace_id, TemporalKeyword02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_03       ON executions_visibility (namespace_id, TemporalKeyword03, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_04       ON executions_visibility (namespace_id, TemporalKeyword04, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_list_01  ON executions_visibility USING GIN (namespace_id, TemporalKeywordList01 jsonb_path_ops);
CREATE INDEX by_temporal_keyword_list_02  ON executions_visibility USING GIN (namespace_id, TemporalKeywordList02 jsonb_path_ops);
