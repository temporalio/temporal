-- Temporal visibility schema for Microsoft SQL Server 2019+ (plugin "mssql2019").
--
-- Layout mirrors the PostgreSQL visibility schema: a SINGLE executions_visibility
-- table holds the base execution fields plus every predefined, pre-allocated
-- custom, and pre-allocated CHASM search-attribute column.
--
-- search_attributes is an NVARCHAR(MAX) JSON document (validated with ISJSON).
-- Each scalar search attribute is a computed PERSISTED column extracted with
-- JSON_VALUE so it can back a regular B-tree index. Datetime attributes are
-- parsed with TRY_CONVERT(datetimeoffset(6), <value>, 127) -- style 127 keeps
-- the conversion deterministic, which is required for PERSISTED columns --
-- and normalized to UTC with SWITCHOFFSET before the offset is dropped, so
-- values written with a non-UTC offset index correctly.
-- JSON-array attributes are computed (non-persisted) JSON_QUERY projections
-- because SQL Server cannot index inside JSON arrays -- queries against them
-- use OPENJSON at runtime.
--
-- NOTE: the full-text catalog and full-text index at the bottom of this file
-- require the SQL Server Full-Text Search feature to be installed on the
-- instance.

CREATE TABLE executions_visibility (
  namespace_id            CHAR(64)        NOT NULL,
  run_id                  CHAR(64)        NOT NULL,
  _version                BIGINT          NOT NULL DEFAULT 0, -- increasing version, used to reject upserts which are out of order
  start_time              DATETIME2(6)    NOT NULL,
  execution_time          DATETIME2(6)    NOT NULL,
  workflow_id             VARCHAR(255)    NOT NULL,
  workflow_type_name      VARCHAR(255)    NOT NULL,
  status                  INT             NOT NULL,  -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  close_time              DATETIME2(6)    NULL,
  history_length          BIGINT          NULL,
  history_size_bytes      BIGINT          NULL,
  execution_duration      BIGINT          NULL,
  state_transition_count  BIGINT          NULL,
  memo                    VARBINARY(MAX)  NULL,
  encoding                VARCHAR(64)     NOT NULL,
  task_queue              VARCHAR(255)    NOT NULL DEFAULT '',
  search_attributes       NVARCHAR(MAX)   NULL,
  parent_workflow_id      VARCHAR(255)    NULL,
  parent_run_id           VARCHAR(255)    NULL,
  root_workflow_id        VARCHAR(255)    NOT NULL DEFAULT '',
  root_run_id             VARCHAR(255)    NOT NULL DEFAULT '',

  -- row_key exists only to provide the single-column, unique, non-nullable
  -- KEY INDEX required by CREATE FULLTEXT INDEX at the bottom of this file.
  -- It is never referenced by the plugin DML.
  row_key                 BIGINT          IDENTITY(1,1) NOT NULL,

  -- coalesce_close_time materializes COALESCE(close_time, '9999-12-31 23:59:59').
  -- T-SQL cannot index an inline expression, so the visibility query converter
  -- and every index below reference THIS COLUMN by name instead of inlining the
  -- COALESCE expression the way the MySQL/PostgreSQL schemas do.
  coalesce_close_time AS ISNULL(close_time, CAST('9999-12-31 23:59:59' AS DATETIME2(6))) PERSISTED,

  -- Each search attribute has its own computed column.
  -- Scalar attributes are PERSISTED so regular B-tree indexes can be built on
  -- them. JSON-array attributes (Keyword lists and similar) are non-persisted
  -- JSON_QUERY projections: SQL Server cannot index inside JSON arrays, so
  -- queries against them use OPENJSON at runtime.

  -- Predefined search attributes
  TemporalChangeVersion         AS JSON_QUERY(search_attributes, '$.TemporalChangeVersion'),
  BinaryChecksums               AS JSON_QUERY(search_attributes, '$.BinaryChecksums'),
  BatcherUser                   AS CAST(JSON_VALUE(search_attributes, '$.BatcherUser') AS VARCHAR(255)) PERSISTED,
  TemporalScheduledStartTime    AS CAST(SWITCHOFFSET(TRY_CONVERT(datetimeoffset(6), JSON_VALUE(search_attributes, '$.TemporalScheduledStartTime'), 127), '+00:00') AS DATETIME2(6)) PERSISTED,
  TemporalScheduledById         AS CAST(JSON_VALUE(search_attributes, '$.TemporalScheduledById') AS VARCHAR(255)) PERSISTED,
  TemporalSchedulePaused        AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalSchedulePaused') AS BIT) PERSISTED,
  TemporalNamespaceDivision     AS CAST(JSON_VALUE(search_attributes, '$.TemporalNamespaceDivision') AS VARCHAR(255)) PERSISTED,
  BuildIds                      AS JSON_QUERY(search_attributes, '$.BuildIds'),
  TemporalPauseInfo             AS JSON_QUERY(search_attributes, '$.TemporalPauseInfo'),
  TemporalReportedProblems      AS JSON_QUERY(search_attributes, '$.TemporalReportedProblems'),
  TemporalWorkerDeploymentVersion    AS CAST(JSON_VALUE(search_attributes, '$.TemporalWorkerDeploymentVersion') AS VARCHAR(255)) PERSISTED,
  TemporalWorkflowVersioningBehavior AS CAST(JSON_VALUE(search_attributes, '$.TemporalWorkflowVersioningBehavior') AS VARCHAR(255)) PERSISTED,
  TemporalWorkerDeployment           AS CAST(JSON_VALUE(search_attributes, '$.TemporalWorkerDeployment') AS VARCHAR(255)) PERSISTED,
  TemporalUsedWorkerDeploymentVersions AS JSON_QUERY(search_attributes, '$.TemporalUsedWorkerDeploymentVersions'),
  TemporalExternalPayloadSizeBytes AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalExternalPayloadSizeBytes') AS BIGINT) PERSISTED,
  TemporalExternalPayloadCount     AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalExternalPayloadCount') AS BIGINT) PERSISTED,

  -- Pre-allocated custom search attributes
  Bool01          AS TRY_CAST(JSON_VALUE(search_attributes, '$.Bool01') AS BIT) PERSISTED,
  Bool02          AS TRY_CAST(JSON_VALUE(search_attributes, '$.Bool02') AS BIT) PERSISTED,
  Bool03          AS TRY_CAST(JSON_VALUE(search_attributes, '$.Bool03') AS BIT) PERSISTED,
  Datetime01      AS CAST(SWITCHOFFSET(TRY_CONVERT(datetimeoffset(6), JSON_VALUE(search_attributes, '$.Datetime01'), 127), '+00:00') AS DATETIME2(6)) PERSISTED,
  Datetime02      AS CAST(SWITCHOFFSET(TRY_CONVERT(datetimeoffset(6), JSON_VALUE(search_attributes, '$.Datetime02'), 127), '+00:00') AS DATETIME2(6)) PERSISTED,
  Datetime03      AS CAST(SWITCHOFFSET(TRY_CONVERT(datetimeoffset(6), JSON_VALUE(search_attributes, '$.Datetime03'), 127), '+00:00') AS DATETIME2(6)) PERSISTED,
  Double01        AS TRY_CAST(JSON_VALUE(search_attributes, '$.Double01') AS DECIMAL(20, 5)) PERSISTED,
  Double02        AS TRY_CAST(JSON_VALUE(search_attributes, '$.Double02') AS DECIMAL(20, 5)) PERSISTED,
  Double03        AS TRY_CAST(JSON_VALUE(search_attributes, '$.Double03') AS DECIMAL(20, 5)) PERSISTED,
  Int01           AS TRY_CAST(JSON_VALUE(search_attributes, '$.Int01') AS BIGINT) PERSISTED,
  Int02           AS TRY_CAST(JSON_VALUE(search_attributes, '$.Int02') AS BIGINT) PERSISTED,
  Int03           AS TRY_CAST(JSON_VALUE(search_attributes, '$.Int03') AS BIGINT) PERSISTED,
  Keyword01       AS CAST(JSON_VALUE(search_attributes, '$.Keyword01') AS VARCHAR(255)) PERSISTED,
  Keyword02       AS CAST(JSON_VALUE(search_attributes, '$.Keyword02') AS VARCHAR(255)) PERSISTED,
  Keyword03       AS CAST(JSON_VALUE(search_attributes, '$.Keyword03') AS VARCHAR(255)) PERSISTED,
  Keyword04       AS CAST(JSON_VALUE(search_attributes, '$.Keyword04') AS VARCHAR(255)) PERSISTED,
  Keyword05       AS CAST(JSON_VALUE(search_attributes, '$.Keyword05') AS VARCHAR(255)) PERSISTED,
  Keyword06       AS CAST(JSON_VALUE(search_attributes, '$.Keyword06') AS VARCHAR(255)) PERSISTED,
  Keyword07       AS CAST(JSON_VALUE(search_attributes, '$.Keyword07') AS VARCHAR(255)) PERSISTED,
  Keyword08       AS CAST(JSON_VALUE(search_attributes, '$.Keyword08') AS VARCHAR(255)) PERSISTED,
  Keyword09       AS CAST(JSON_VALUE(search_attributes, '$.Keyword09') AS VARCHAR(255)) PERSISTED,
  Keyword10       AS CAST(JSON_VALUE(search_attributes, '$.Keyword10') AS VARCHAR(255)) PERSISTED,
  -- Text01-03 are capped at NVARCHAR(4000) as a deliberate trade-off:
  -- NVARCHAR(MAX) computed columns cannot be full-text indexed unless
  -- PERSISTED. KNOWN LIMITATION: JSON_VALUE (lax mode) returns NULL -- it does
  -- NOT truncate -- when the extracted value exceeds 4000 characters, so Text
  -- values longer than 4000 characters are not full-text searchable at all.
  -- The full value is still preserved inside search_attributes and returned
  -- to callers; only CONTAINS() search misses such rows. Making them
  -- searchable would require real columns maintained by the plugin DML.
  Text01          AS CAST(JSON_VALUE(search_attributes, '$.Text01') AS NVARCHAR(4000)) PERSISTED,
  Text02          AS CAST(JSON_VALUE(search_attributes, '$.Text02') AS NVARCHAR(4000)) PERSISTED,
  Text03          AS CAST(JSON_VALUE(search_attributes, '$.Text03') AS NVARCHAR(4000)) PERSISTED,
  KeywordList01   AS JSON_QUERY(search_attributes, '$.KeywordList01'),
  KeywordList02   AS JSON_QUERY(search_attributes, '$.KeywordList02'),
  KeywordList03   AS JSON_QUERY(search_attributes, '$.KeywordList03'),

  -- Pre-allocated CHASM search attributes
  TemporalBool01                  AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalBool01') AS BIT) PERSISTED,
  TemporalBool02                  AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalBool02') AS BIT) PERSISTED,
  TemporalDatetime01              AS CAST(SWITCHOFFSET(TRY_CONVERT(datetimeoffset(6), JSON_VALUE(search_attributes, '$.TemporalDatetime01'), 127), '+00:00') AS DATETIME2(6)) PERSISTED,
  TemporalDatetime02              AS CAST(SWITCHOFFSET(TRY_CONVERT(datetimeoffset(6), JSON_VALUE(search_attributes, '$.TemporalDatetime02'), 127), '+00:00') AS DATETIME2(6)) PERSISTED,
  TemporalDouble01                AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalDouble01') AS DECIMAL(20, 5)) PERSISTED,
  TemporalDouble02                AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalDouble02') AS DECIMAL(20, 5)) PERSISTED,
  TemporalInt01                   AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalInt01') AS BIGINT) PERSISTED,
  TemporalInt02                   AS TRY_CAST(JSON_VALUE(search_attributes, '$.TemporalInt02') AS BIGINT) PERSISTED,
  TemporalKeyword01               AS CAST(JSON_VALUE(search_attributes, '$.TemporalKeyword01') AS VARCHAR(255)) PERSISTED,
  TemporalKeyword02               AS CAST(JSON_VALUE(search_attributes, '$.TemporalKeyword02') AS VARCHAR(255)) PERSISTED,
  TemporalKeyword03               AS CAST(JSON_VALUE(search_attributes, '$.TemporalKeyword03') AS VARCHAR(255)) PERSISTED,
  TemporalKeyword04               AS CAST(JSON_VALUE(search_attributes, '$.TemporalKeyword04') AS VARCHAR(255)) PERSISTED,
  TemporalLowCardinalityKeyword01 AS CAST(JSON_VALUE(search_attributes, '$.TemporalLowCardinalityKeyword01') AS VARCHAR(255)) PERSISTED,
  TemporalKeywordList01           AS JSON_QUERY(search_attributes, '$.TemporalKeywordList01'),
  TemporalKeywordList02           AS JSON_QUERY(search_attributes, '$.TemporalKeywordList02'),

  PRIMARY KEY (namespace_id, run_id),
  CONSTRAINT executions_visibility_search_attributes_is_json CHECK (search_attributes IS NULL OR ISJSON(search_attributes) = 1)
);

CREATE INDEX default_idx                ON executions_visibility (namespace_id, coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_execution_time          ON executions_visibility (namespace_id, execution_time,         coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_id             ON executions_visibility (namespace_id, workflow_id,            coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_type           ON executions_visibility (namespace_id, workflow_type_name,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_status                  ON executions_visibility (namespace_id, status,                 coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_history_length          ON executions_visibility (namespace_id, history_length,         coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_history_size_bytes      ON executions_visibility (namespace_id, history_size_bytes,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_execution_duration      ON executions_visibility (namespace_id, execution_duration,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_state_transition_count  ON executions_visibility (namespace_id, state_transition_count, coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_task_queue              ON executions_visibility (namespace_id, task_queue,             coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_parent_workflow_id      ON executions_visibility (namespace_id, parent_workflow_id,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_parent_run_id           ON executions_visibility (namespace_id, parent_run_id,          coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_root_workflow_id        ON executions_visibility (namespace_id, root_workflow_id,       coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_root_run_id             ON executions_visibility (namespace_id, root_run_id,            coalesce_close_time DESC, start_time DESC, run_id);

-- Indexes for the predefined search attributes
--
-- SQL Server cannot build indexes that look inside JSON arrays, so the
-- following PostgreSQL GIN / MySQL multi-valued indexes have no SQL Server
-- equivalent and are deliberately skipped. Queries against these attributes
-- use OPENJSON over the JSON_QUERY column at runtime:
--   by_temporal_change_version    (TemporalChangeVersion)
--   by_binary_checksums           (BinaryChecksums)
--   by_build_ids                  (BuildIds)
--   by_temporal_pause_info        (TemporalPauseInfo)
--   by_temporal_reported_problems (TemporalReportedProblems)
--   by_used_deployment_versions   (TemporalUsedWorkerDeploymentVersions)
CREATE INDEX by_temporal_worker_deployment_version    ON executions_visibility (namespace_id, TemporalWorkerDeploymentVersion,    coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, TemporalWorkflowVersioningBehavior, coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment            ON executions_visibility (namespace_id, TemporalWorkerDeployment,           coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_batcher_user                          ON executions_visibility (namespace_id, BatcherUser,                        coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_start_time         ON executions_visibility (namespace_id, TemporalScheduledStartTime,         coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_by_id              ON executions_visibility (namespace_id, TemporalScheduledById,              coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_schedule_paused              ON executions_visibility (namespace_id, TemporalSchedulePaused,             coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_namespace_division           ON executions_visibility (namespace_id, TemporalNamespaceDivision,          coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_external_payload_size_bytes  ON executions_visibility (namespace_id, TemporalExternalPayloadSizeBytes,   coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_external_payload_count       ON executions_visibility (namespace_id, TemporalExternalPayloadCount,       coalesce_close_time DESC, start_time DESC, run_id);

-- Indexes for the pre-allocated custom search attributes
--
-- by_keyword_list_01 through by_keyword_list_03 are skipped (JSON arrays --
-- see the note above). by_text_01 through by_text_03 are replaced by the
-- full-text index at the bottom of this file.
CREATE INDEX by_bool_01         ON executions_visibility (namespace_id, Bool01,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_bool_02         ON executions_visibility (namespace_id, Bool02,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_bool_03         ON executions_visibility (namespace_id, Bool03,     coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_01     ON executions_visibility (namespace_id, Datetime01, coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_02     ON executions_visibility (namespace_id, Datetime02, coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_03     ON executions_visibility (namespace_id, Datetime03, coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_double_01       ON executions_visibility (namespace_id, Double01,   coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_double_02       ON executions_visibility (namespace_id, Double02,   coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_double_03       ON executions_visibility (namespace_id, Double03,   coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_int_01          ON executions_visibility (namespace_id, Int01,      coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_int_02          ON executions_visibility (namespace_id, Int02,      coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_int_03          ON executions_visibility (namespace_id, Int03,      coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_01      ON executions_visibility (namespace_id, Keyword01,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_02      ON executions_visibility (namespace_id, Keyword02,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_03      ON executions_visibility (namespace_id, Keyword03,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_04      ON executions_visibility (namespace_id, Keyword04,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_05      ON executions_visibility (namespace_id, Keyword05,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_06      ON executions_visibility (namespace_id, Keyword06,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_07      ON executions_visibility (namespace_id, Keyword07,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_08      ON executions_visibility (namespace_id, Keyword08,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_09      ON executions_visibility (namespace_id, Keyword09,  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_10      ON executions_visibility (namespace_id, Keyword10,  coalesce_close_time DESC, start_time DESC, run_id);

-- Indexes for the pre-allocated CHASM search attributes
--
-- by_temporal_keyword_list_01 and by_temporal_keyword_list_02 are skipped
-- (JSON arrays -- see the note above).
CREATE INDEX by_temporal_bool_01                    ON executions_visibility (namespace_id, TemporalBool01,                  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_bool_02                    ON executions_visibility (namespace_id, TemporalBool02,                  coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_datetime_01                ON executions_visibility (namespace_id, TemporalDatetime01,              coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_datetime_02                ON executions_visibility (namespace_id, TemporalDatetime02,              coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_double_01                  ON executions_visibility (namespace_id, TemporalDouble01,                coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_double_02                  ON executions_visibility (namespace_id, TemporalDouble02,                coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_int_01                     ON executions_visibility (namespace_id, TemporalInt01,                   coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_int_02                     ON executions_visibility (namespace_id, TemporalInt02,                   coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_01                 ON executions_visibility (namespace_id, TemporalKeyword01,               coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_02                 ON executions_visibility (namespace_id, TemporalKeyword02,               coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_03                 ON executions_visibility (namespace_id, TemporalKeyword03,               coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_keyword_04                 ON executions_visibility (namespace_id, TemporalKeyword04,               coalesce_close_time DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_low_cardinality_keyword_01 ON executions_visibility (namespace_id, TemporalLowCardinalityKeyword01, coalesce_close_time DESC, start_time DESC, run_id);

-- Full-text search over Text01-03. Replaces the PostgreSQL TSVECTOR GIN
-- indexes and the MySQL FULLTEXT indexes. Requires the SQL Server Full-Text
-- Search feature to be installed on the instance. The unique single-column
-- index on row_key is the KEY INDEX required by CREATE FULLTEXT INDEX.
CREATE UNIQUE INDEX by_row_key ON executions_visibility (row_key);

-- The IF NOT EXISTS guards make setup-schema --overwrite re-runs work: the
-- catalog is a database-scoped object that survives DropAllTables.
IF NOT EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = 'temporal_visibility_fts') CREATE FULLTEXT CATALOG temporal_visibility_fts AS DEFAULT;

IF NOT EXISTS (SELECT 1 FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID('executions_visibility')) CREATE FULLTEXT INDEX ON executions_visibility (Text01, Text02, Text03) KEY INDEX by_row_key ON temporal_visibility_fts;
