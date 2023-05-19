CREATE EXTENSION IF NOT EXISTS btree_gin;

-- convert_ts converts a timestamp in RFC3339 to UTC timestamp without time zone.
CREATE FUNCTION convert_ts(s VARCHAR) RETURNS TIMESTAMP AS $$
BEGIN
  RETURN s::timestamptz at time zone 'UTC';
END
$$ LANGUAGE plpgsql IMMUTABLE RETURNS NULL ON NULL INPUT;

-- Add search attributes related columns
ALTER TABLE executions_visibility ADD COLUMN search_attributes JSONB NULL;

-- Pre-defined search attributes
ALTER TABLE executions_visibility
  ADD COLUMN TemporalChangeVersion      JSONB         GENERATED ALWAYS AS (search_attributes->'TemporalChangeVersion')                    STORED,
  ADD COLUMN BinaryChecksums            JSONB         GENERATED ALWAYS AS (search_attributes->'BinaryChecksums')                          STORED,
  ADD COLUMN BatcherUser                VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>'BatcherUser')                             STORED,
  ADD COLUMN TemporalScheduledStartTime TIMESTAMP     GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalScheduledStartTime'))  STORED,
  ADD COLUMN TemporalScheduledById      VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>'TemporalScheduledById')                   STORED,
  ADD COLUMN TemporalSchedulePaused     BOOLEAN       GENERATED ALWAYS AS ((search_attributes->'TemporalSchedulePaused')::boolean)        STORED,
  ADD COLUMN TemporalNamespaceDivision  VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>'TemporalNamespaceDivision')               STORED;

-- Pre-allocated custom search attributes
ALTER TABLE executions_visibility
  ADD COLUMN Bool01         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'Bool01')::boolean)        STORED,
  ADD COLUMN Bool02         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'Bool02')::boolean)        STORED,
  ADD COLUMN Bool03         BOOLEAN         GENERATED ALWAYS AS ((search_attributes->'Bool03')::boolean)        STORED,
  ADD COLUMN Datetime01     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'Datetime01'))  STORED,
  ADD COLUMN Datetime02     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'Datetime02'))  STORED,
  ADD COLUMN Datetime03     TIMESTAMP       GENERATED ALWAYS AS (convert_ts(search_attributes->>'Datetime03'))  STORED,
  ADD COLUMN Double01       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'Double01')::decimal)      STORED,
  ADD COLUMN Double02       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'Double02')::decimal)      STORED,
  ADD COLUMN Double03       DECIMAL(20, 5)  GENERATED ALWAYS AS ((search_attributes->'Double03')::decimal)      STORED,
  ADD COLUMN Int01          BIGINT          GENERATED ALWAYS AS ((search_attributes->'Int01')::bigint)          STORED,
  ADD COLUMN Int02          BIGINT          GENERATED ALWAYS AS ((search_attributes->'Int02')::bigint)          STORED,
  ADD COLUMN Int03          BIGINT          GENERATED ALWAYS AS ((search_attributes->'Int03')::bigint)          STORED,
  ADD COLUMN Keyword01      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword01')               STORED,
  ADD COLUMN Keyword02      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword02')               STORED,
  ADD COLUMN Keyword03      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword03')               STORED,
  ADD COLUMN Keyword04      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword04')               STORED,
  ADD COLUMN Keyword05      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword05')               STORED,
  ADD COLUMN Keyword06      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword06')               STORED,
  ADD COLUMN Keyword07      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword07')               STORED,
  ADD COLUMN Keyword08      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword08')               STORED,
  ADD COLUMN Keyword09      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword09')               STORED,
  ADD COLUMN Keyword10      VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>'Keyword10')               STORED,
  ADD COLUMN Text01         TSVECTOR        GENERATED ALWAYS AS ((search_attributes->>'Text01')::tsvector)      STORED,
  ADD COLUMN Text02         TSVECTOR        GENERATED ALWAYS AS ((search_attributes->>'Text02')::tsvector)      STORED,
  ADD COLUMN Text03         TSVECTOR        GENERATED ALWAYS AS ((search_attributes->>'Text03')::tsvector)      STORED,
  ADD COLUMN KeywordList01  JSONB           GENERATED ALWAYS AS (search_attributes->'KeywordList01')            STORED,
  ADD COLUMN KeywordList02  JSONB           GENERATED ALWAYS AS (search_attributes->'KeywordList02')            STORED,
  ADD COLUMN KeywordList03  JSONB           GENERATED ALWAYS AS (search_attributes->'KeywordList03')            STORED;

-- Drop existing indexes
DROP INDEX by_type_start_time;
DROP INDEX by_workflow_id_start_time;
DROP INDEX by_status_by_start_time;
DROP INDEX by_type_close_time;
DROP INDEX by_workflow_id_close_time;
DROP INDEX by_status_by_close_time;
DROP INDEX by_close_time_by_status;

-- Create new indexes
CREATE INDEX default_idx        ON executions_visibility (namespace_id, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_time  ON executions_visibility (namespace_id, execution_time,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_id     ON executions_visibility (namespace_id, workflow_id,        (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_type   ON executions_visibility (namespace_id, workflow_type_name, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_status          ON executions_visibility (namespace_id, status,             (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_history_length  ON executions_visibility (namespace_id, history_length,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_task_queue      ON executions_visibility (namespace_id, task_queue,         (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);

-- Indexes for the predefined search attributes
CREATE INDEX by_temporal_change_version       ON executions_visibility USING GIN (namespace_id, TemporalChangeVersion jsonb_path_ops);
CREATE INDEX by_binary_checksums              ON executions_visibility USING GIN (namespace_id, BinaryChecksums jsonb_path_ops);
CREATE INDEX by_batcher_user                  ON executions_visibility (namespace_id, BatcherUser,                (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_start_time ON executions_visibility (namespace_id, TemporalScheduledStartTime, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_by_id      ON executions_visibility (namespace_id, TemporalScheduledById,      (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_schedule_paused      ON executions_visibility (namespace_id, TemporalSchedulePaused,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_namespace_division   ON executions_visibility (namespace_id, TemporalNamespaceDivision,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);

-- Indexes for the pre-allocated custom search attributes
CREATE INDEX by_bool_01         ON executions_visibility (namespace_id, Bool01,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_bool_02         ON executions_visibility (namespace_id, Bool02,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_bool_03         ON executions_visibility (namespace_id, Bool03,     (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_01     ON executions_visibility (namespace_id, Datetime01, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_02     ON executions_visibility (namespace_id, Datetime02, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_03     ON executions_visibility (namespace_id, Datetime03, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_double_01       ON executions_visibility (namespace_id, Double01,   (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_double_02       ON executions_visibility (namespace_id, Double02,   (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_double_03       ON executions_visibility (namespace_id, Double03,   (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_int_01          ON executions_visibility (namespace_id, Int01,      (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_int_02          ON executions_visibility (namespace_id, Int02,      (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_int_03          ON executions_visibility (namespace_id, Int03,      (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_01      ON executions_visibility (namespace_id, Keyword01,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_02      ON executions_visibility (namespace_id, Keyword02,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_03      ON executions_visibility (namespace_id, Keyword03,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_04      ON executions_visibility (namespace_id, Keyword04,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_05      ON executions_visibility (namespace_id, Keyword05,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_06      ON executions_visibility (namespace_id, Keyword06,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_07      ON executions_visibility (namespace_id, Keyword07,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_08      ON executions_visibility (namespace_id, Keyword08,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_09      ON executions_visibility (namespace_id, Keyword09,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_10      ON executions_visibility (namespace_id, Keyword10,  (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX by_text_01         ON executions_visibility USING GIN (namespace_id, Text01);
CREATE INDEX by_text_02         ON executions_visibility USING GIN (namespace_id, Text02);
CREATE INDEX by_text_03         ON executions_visibility USING GIN (namespace_id, Text03);
CREATE INDEX by_keyword_list_01 ON executions_visibility USING GIN (namespace_id, KeywordList01 jsonb_path_ops);
CREATE INDEX by_keyword_list_02 ON executions_visibility USING GIN (namespace_id, KeywordList02 jsonb_path_ops);
CREATE INDEX by_keyword_list_03 ON executions_visibility USING GIN (namespace_id, KeywordList03 jsonb_path_ops);
