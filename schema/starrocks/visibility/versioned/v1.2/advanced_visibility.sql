-- Add search attributes related columns
ALTER TABLE executions_visibility
  ADD COLUMN search_attributes          JSON NULL,
  ADD COLUMN TemporalChangeVersion      JSON          GENERATED ALWAYS AS (search_attributes->"$.TemporalChangeVersion"),
  ADD COLUMN BinaryChecksums            JSON          GENERATED ALWAYS AS (search_attributes->"$.BinaryChecksums"),
  ADD COLUMN BatcherUser                VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>"$.BatcherUser"),
  ADD COLUMN TemporalScheduledStartTime DATETIME(6)   GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.TemporalScheduledStartTime", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.TemporalScheduledStartTime", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  ADD COLUMN TemporalScheduledById      VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>"$.TemporalScheduledById"),
  ADD COLUMN TemporalSchedulePaused     BOOLEAN       GENERATED ALWAYS AS (search_attributes->"$.TemporalSchedulePaused"),
  ADD COLUMN TemporalNamespaceDivision  VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>"$.TemporalNamespaceDivision");

-- Drop existing indexes
DROP INDEX by_type_start_time         ON executions_visibility;
DROP INDEX by_workflow_id_start_time  ON executions_visibility;
DROP INDEX by_status_by_start_time    ON executions_visibility;
DROP INDEX by_type_close_time         ON executions_visibility;
DROP INDEX by_workflow_id_close_time  ON executions_visibility;
DROP INDEX by_status_by_close_time    ON executions_visibility;
DROP INDEX by_close_time_by_status    ON executions_visibility;

-- Create new indexes
CREATE INDEX default_idx        ON executions_visibility (namespace_id, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_time  ON executions_visibility (namespace_id, execution_time,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_id     ON executions_visibility (namespace_id, workflow_id,        (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_type   ON executions_visibility (namespace_id, workflow_type_name, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_status          ON executions_visibility (namespace_id, status,             (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_history_length  ON executions_visibility (namespace_id, history_length,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_task_queue      ON executions_visibility (namespace_id, task_queue,         (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);

-- Indexes for the predefined search attributes
CREATE INDEX by_temporal_change_version       ON executions_visibility (namespace_id, (CAST(TemporalChangeVersion AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_binary_checksums              ON executions_visibility (namespace_id, (CAST(BinaryChecksums AS CHAR(255) ARRAY)),       (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_batcher_user                  ON executions_visibility (namespace_id, BatcherUser,                (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_start_time ON executions_visibility (namespace_id, TemporalScheduledStartTime, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_by_id      ON executions_visibility (namespace_id, TemporalScheduledById,      (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_schedule_paused      ON executions_visibility (namespace_id, TemporalSchedulePaused,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_namespace_division   ON executions_visibility (namespace_id, TemporalNamespaceDivision,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);


-- Custom search attributes
CREATE TABLE custom_search_attributes (
  namespace_id      CHAR(64)  NOT NULL,
  run_id            CHAR(64)  NOT NULL,
  search_attributes JSON      NULL,
  Bool01            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.Bool01"),
  Bool02            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.Bool02"),
  Bool03            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.Bool03"),
  Datetime01        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.Datetime01", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.Datetime01", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  Datetime02        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.Datetime02", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.Datetime02", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  Datetime03        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.Datetime03", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.Datetime03", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  Double01          DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.Double01"),
  Double02          DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.Double02"),
  Double03          DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.Double03"),
  Int01             BIGINT          GENERATED ALWAYS AS (search_attributes->"$.Int01"),
  Int02             BIGINT          GENERATED ALWAYS AS (search_attributes->"$.Int02"),
  Int03             BIGINT          GENERATED ALWAYS AS (search_attributes->"$.Int03"),
  Keyword01         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword01"),
  Keyword02         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword02"),
  Keyword03         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword03"),
  Keyword04         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword04"),
  Keyword05         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword05"),
  Keyword06         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword06"),
  Keyword07         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword07"),
  Keyword08         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword08"),
  Keyword09         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword09"),
  Keyword10         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.Keyword10"),
  Text01            TEXT            GENERATED ALWAYS AS (search_attributes->>"$.Text01") STORED,
  Text02            TEXT            GENERATED ALWAYS AS (search_attributes->>"$.Text02") STORED,
  Text03            TEXT            GENERATED ALWAYS AS (search_attributes->>"$.Text03") STORED,
  KeywordList01     JSON            GENERATED ALWAYS AS (search_attributes->"$.KeywordList01"),
  KeywordList02     JSON            GENERATED ALWAYS AS (search_attributes->"$.KeywordList02"),
  KeywordList03     JSON            GENERATED ALWAYS AS (search_attributes->"$.KeywordList03"),

  PRIMARY KEY (namespace_id, run_id)
);

CREATE INDEX by_bool_01           ON custom_search_attributes (namespace_id, Bool01);
CREATE INDEX by_bool_02           ON custom_search_attributes (namespace_id, Bool02);
CREATE INDEX by_bool_03           ON custom_search_attributes (namespace_id, Bool03);
CREATE INDEX by_datetime_01       ON custom_search_attributes (namespace_id, Datetime01);
CREATE INDEX by_datetime_02       ON custom_search_attributes (namespace_id, Datetime02);
CREATE INDEX by_datetime_03       ON custom_search_attributes (namespace_id, Datetime03);
CREATE INDEX by_double_01         ON custom_search_attributes (namespace_id, Double01);
CREATE INDEX by_double_02         ON custom_search_attributes (namespace_id, Double02);
CREATE INDEX by_double_03         ON custom_search_attributes (namespace_id, Double03);
CREATE INDEX by_int_01            ON custom_search_attributes (namespace_id, Int01);
CREATE INDEX by_int_02            ON custom_search_attributes (namespace_id, Int02);
CREATE INDEX by_int_03            ON custom_search_attributes (namespace_id, Int03);
CREATE INDEX by_keyword_01        ON custom_search_attributes (namespace_id, Keyword01);
CREATE INDEX by_keyword_02        ON custom_search_attributes (namespace_id, Keyword02);
CREATE INDEX by_keyword_03        ON custom_search_attributes (namespace_id, Keyword03);
CREATE INDEX by_keyword_04        ON custom_search_attributes (namespace_id, Keyword04);
CREATE INDEX by_keyword_05        ON custom_search_attributes (namespace_id, Keyword05);
CREATE INDEX by_keyword_06        ON custom_search_attributes (namespace_id, Keyword06);
CREATE INDEX by_keyword_07        ON custom_search_attributes (namespace_id, Keyword07);
CREATE INDEX by_keyword_08        ON custom_search_attributes (namespace_id, Keyword08);
CREATE INDEX by_keyword_09        ON custom_search_attributes (namespace_id, Keyword09);
CREATE INDEX by_keyword_10        ON custom_search_attributes (namespace_id, Keyword10);
CREATE FULLTEXT INDEX by_text_01  ON custom_search_attributes (Text01);
CREATE FULLTEXT INDEX by_text_02  ON custom_search_attributes (Text02);
CREATE FULLTEXT INDEX by_text_03  ON custom_search_attributes (Text03);
CREATE INDEX by_keyword_list_01   ON custom_search_attributes (namespace_id, (CAST(KeywordList01 AS CHAR(255) ARRAY)));
CREATE INDEX by_keyword_list_02   ON custom_search_attributes (namespace_id, (CAST(KeywordList02 AS CHAR(255) ARRAY)));
CREATE INDEX by_keyword_list_03   ON custom_search_attributes (namespace_id, (CAST(KeywordList03 AS CHAR(255) ARRAY)));
