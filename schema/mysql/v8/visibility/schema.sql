CREATE TABLE executions_visibility (
  namespace_id            CHAR(64)      NOT NULL,
  run_id                  CHAR(64)      NOT NULL,
  _version                BIGINT        NOT NULL DEFAULT 0, -- increasing version, used to reject upserts which are out of order
  start_time              DATETIME(6)   NOT NULL,
  execution_time          DATETIME(6)   NOT NULL,
  workflow_id             VARCHAR(255)  NOT NULL,
  workflow_type_name      VARCHAR(255)  NOT NULL,
  status                  INT           NOT NULL,  -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  close_time              DATETIME(6)   NULL,
  history_length          BIGINT        NULL,
  history_size_bytes      BIGINT        NULL,
  execution_duration      BIGINT        NULL,
  state_transition_count  BIGINT        NULL,
  memo                    BLOB          NULL,
  encoding                VARCHAR(64)   NOT NULL,
  task_queue              VARCHAR(255)  NOT NULL DEFAULT '',
  search_attributes       JSON          NULL,
  parent_workflow_id      VARCHAR(255)  NULL,
  parent_run_id           VARCHAR(255)  NULL,
  root_workflow_id        VARCHAR(255)  NOT NULL DEFAULT '',
  root_run_id             VARCHAR(255)  NOT NULL DEFAULT '',

  -- Each search attribute has its own generated column.
  -- For string types (keyword and text), we need to unquote the json string,
  -- ie., use `->>` instead of `->` operator.
  -- For text types, the generated column need to be STORED instead of VIRTUAL,
  -- so we can create a full-text search index.
  -- For datetime type, MySQL can't cast datetime string with timezone to
  -- datetime type directly, so we need to call CONVERT_TZ to convert to UTC.
  -- Check the `custom_search_attributes` table for complete set of examples.

  -- Predefined search attributes
  TemporalChangeVersion         JSON          GENERATED ALWAYS AS (search_attributes->"$.TemporalChangeVersion"),
  BinaryChecksums               JSON          GENERATED ALWAYS AS (search_attributes->"$.BinaryChecksums"),
  BatcherUser                   VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>"$.BatcherUser"),
  TemporalScheduledStartTime    DATETIME(6)   GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.TemporalScheduledStartTime", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.TemporalScheduledStartTime", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  TemporalScheduledById         VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>"$.TemporalScheduledById"),
  TemporalSchedulePaused        BOOLEAN       GENERATED ALWAYS AS (search_attributes->"$.TemporalSchedulePaused"),
  TemporalNamespaceDivision     VARCHAR(255)  GENERATED ALWAYS AS (search_attributes->>"$.TemporalNamespaceDivision"),
  BuildIds                      JSON          GENERATED ALWAYS AS (search_attributes->"$.BuildIds"),
  TemporalPauseInfo             JSON          GENERATED ALWAYS AS (search_attributes->"$.TemporalPauseInfo"),
  TemporalReportedProblems      JSON          GENERATED ALWAYS AS (search_attributes->"$.TemporalReportedProblems"),
  TemporalWorkerDeploymentVersion    VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalWorkerDeploymentVersion"),
  TemporalWorkflowVersioningBehavior VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalWorkflowVersioningBehavior"),
  TemporalWorkerDeployment           VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalWorkerDeployment"),
  TemporalUsedWorkerDeploymentVersions JSON GENERATED ALWAYS AS (search_attributes->'$.TemporalUsedWorkerDeploymentVersions'),

  PRIMARY KEY (namespace_id, run_id)
);

CREATE INDEX default_idx                ON executions_visibility (namespace_id, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_time          ON executions_visibility (namespace_id, execution_time,         (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_id             ON executions_visibility (namespace_id, workflow_id,            (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_type           ON executions_visibility (namespace_id, workflow_type_name,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_status                  ON executions_visibility (namespace_id, status,                 (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_history_length          ON executions_visibility (namespace_id, history_length,         (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_history_size_bytes      ON executions_visibility (namespace_id, history_size_bytes,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_duration      ON executions_visibility (namespace_id, execution_duration,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_state_transition_count  ON executions_visibility (namespace_id, state_transition_count, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_task_queue              ON executions_visibility (namespace_id, task_queue,             (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_workflow_id      ON executions_visibility (namespace_id, parent_workflow_id,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_run_id           ON executions_visibility (namespace_id, parent_run_id,          (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_root_workflow_id        ON executions_visibility (namespace_id, root_workflow_id,       (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_root_run_id             ON executions_visibility (namespace_id, root_run_id,            (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);

-- Indexes for the predefined search attributes
CREATE INDEX by_temporal_change_version       ON executions_visibility (namespace_id, (CAST(TemporalChangeVersion AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_binary_checksums              ON executions_visibility (namespace_id, (CAST(BinaryChecksums AS CHAR(255) ARRAY)),       (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_build_ids                     ON executions_visibility (namespace_id, (CAST(BuildIds AS CHAR(255) ARRAY)),              (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_pause_info           ON executions_visibility (namespace_id, (CAST(TemporalPauseInfo AS CHAR(255) ARRAY)),    (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_reported_problems    ON executions_visibility (namespace_id, (CAST(TemporalReportedProblems AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment_version    ON executions_visibility (namespace_id, TemporalWorkerDeploymentVersion,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, TemporalWorkflowVersioningBehavior,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment            ON executions_visibility (namespace_id, TemporalWorkerDeployment,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_used_deployment_versions      ON executions_visibility (namespace_id,(CAST(TemporalUsedWorkerDeploymentVersions AS CHAR(255) ARRAY)), (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_batcher_user                  ON executions_visibility (namespace_id, BatcherUser,                (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_start_time ON executions_visibility (namespace_id, TemporalScheduledStartTime, (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_by_id      ON executions_visibility (namespace_id, TemporalScheduledById,      (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_schedule_paused      ON executions_visibility (namespace_id, TemporalSchedulePaused,     (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_namespace_division   ON executions_visibility (namespace_id, TemporalNamespaceDivision,  (COALESCE(close_time, CAST('9999-12-31 23:59:59' AS DATETIME))) DESC, start_time DESC, run_id);


CREATE TABLE custom_search_attributes (
  namespace_id      CHAR(64)  NOT NULL,
  run_id            CHAR(64)  NOT NULL,
  _version           BIGINT    NOT NULL DEFAULT 0,
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

CREATE TABLE chasm_search_attributes (
  namespace_id      CHAR(64)        NOT NULL,
  run_id            CHAR(64)        NOT NULL,
  _version          BIGINT          NOT NULL DEFAULT 0,
  search_attributes JSON            NULL,

  -- Pre-allocated CHASM search attributes
  TemporalBool01            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.TemporalBool01"),
  TemporalBool02            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.TemporalBool02"),
  TemporalDatetime01        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.TemporalDatetime01", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.TemporalDatetime01", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  TemporalDatetime02        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.TemporalDatetime02", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.TemporalDatetime02", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  TemporalDouble01                DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.TemporalDouble01"),
  TemporalDouble02                DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.TemporalDouble02"),
  TemporalInt01                   BIGINT          GENERATED ALWAYS AS (search_attributes->"$.TemporalInt01"),
  TemporalInt02                   BIGINT          GENERATED ALWAYS AS (search_attributes->"$.TemporalInt02"),
  TemporalKeyword01               VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword01"),
  TemporalKeyword02               VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword02"),
  TemporalKeyword03               VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword03"),
  TemporalKeyword04               VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword04"),
  TemporalLowCardinalityKeyword01 VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalLowCardinalityKeyword01"),
  TemporalKeywordList01           JSON            GENERATED ALWAYS AS (search_attributes->"$.TemporalKeywordList01"),
  TemporalKeywordList02           JSON            GENERATED ALWAYS AS (search_attributes->"$.TemporalKeywordList02"),

  PRIMARY KEY (namespace_id, run_id)
);

CREATE INDEX by_temporal_bool_01                    ON chasm_search_attributes (namespace_id, TemporalBool01);
CREATE INDEX by_temporal_bool_02                    ON chasm_search_attributes (namespace_id, TemporalBool02);
CREATE INDEX by_temporal_datetime_01                ON chasm_search_attributes (namespace_id, TemporalDatetime01);
CREATE INDEX by_temporal_datetime_02                ON chasm_search_attributes (namespace_id, TemporalDatetime02);
CREATE INDEX by_temporal_double_01                  ON chasm_search_attributes (namespace_id, TemporalDouble01);
CREATE INDEX by_temporal_double_02                  ON chasm_search_attributes (namespace_id, TemporalDouble02);
CREATE INDEX by_temporal_int_01                     ON chasm_search_attributes (namespace_id, TemporalInt01);
CREATE INDEX by_temporal_int_02                     ON chasm_search_attributes (namespace_id, TemporalInt02);
CREATE INDEX by_temporal_keyword_01                 ON chasm_search_attributes (namespace_id, TemporalKeyword01);
CREATE INDEX by_temporal_keyword_02                 ON chasm_search_attributes (namespace_id, TemporalKeyword02);
CREATE INDEX by_temporal_keyword_03                 ON chasm_search_attributes (namespace_id, TemporalKeyword03);
CREATE INDEX by_temporal_keyword_04                 ON chasm_search_attributes (namespace_id, TemporalKeyword04);
CREATE INDEX by_temporal_low_cardinality_keyword_01 ON chasm_search_attributes (namespace_id, TemporalLowCardinalityKeyword01);
CREATE INDEX by_temporal_keyword_list_01            ON chasm_search_attributes (namespace_id, (CAST(TemporalKeywordList01 AS CHAR(255) ARRAY)));
CREATE INDEX by_temporal_keyword_list_02            ON chasm_search_attributes (namespace_id, (CAST(TemporalKeywordList02 AS CHAR(255) ARRAY)));
