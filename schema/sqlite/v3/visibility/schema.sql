CREATE TABLE executions_visibility (
  namespace_id            CHAR(64)      NOT NULL,
  run_id                  CHAR(64)      NOT NULL,
  start_time              TIMESTAMP     NOT NULL,
  execution_time          TIMESTAMP     NOT NULL,
  workflow_id             VARCHAR(255)  NOT NULL,
  workflow_type_name      VARCHAR(255)  NOT NULL,
  status                  INT           NOT NULL,  -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  close_time              TIMESTAMP     NULL,
  history_length          BIGINT        NULL,
  history_size_bytes      BIGINT        NULL,
  execution_duration      BIGINT        NULL,
  state_transition_count  BIGINT        NULL,
  memo                    BLOB          NULL,
  encoding                VARCHAR(64)   NOT NULL,
  task_queue              VARCHAR(255)  NOT NULL DEFAULT '',
  search_attributes       TEXT          NULL,
  parent_workflow_id      VARCHAR(255)  NULL,
  parent_run_id           VARCHAR(255)  NULL,
  root_workflow_id        VARCHAR(255)  NOT NULL DEFAULT '',
  root_run_id             VARCHAR(255)  NOT NULL DEFAULT '',

  -- Predefined search attributes
  TemporalChangeVersion         TEXT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.TemporalChangeVersion")) STORED,
  BinaryChecksums               TEXT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.BinaryChecksums"))       STORED,
  BatcherUser                   VARCHAR(255)  GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.BatcherUser")),
  TemporalScheduledStartTime    TIMESTAMP     GENERATED ALWAYS AS (STRFTIME('%Y-%m-%d %H:%M:%f+00:00', JSON_EXTRACT(search_attributes, "$.TemporalScheduledStartTime"))),
  TemporalScheduledById         VARCHAR(255)  GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.TemporalScheduledById")),
  TemporalSchedulePaused        BOOLEAN       GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.TemporalSchedulePaused")),
  TemporalNamespaceDivision     VARCHAR(255)  GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.TemporalNamespaceDivision")),
  BuildIds                      TEXT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.BuildIds"))              STORED,
  PauseActivityTypes            TEXT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.PauseActivityTypes"))    STORED,

  -- Pre-allocated custom search attributes
  Bool01          BOOLEAN         GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Bool01")),
  Bool02          BOOLEAN         GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Bool02")),
  Bool03          BOOLEAN         GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Bool03")),
  Datetime01      TIMESTAMP       GENERATED ALWAYS AS (STRFTIME('%Y-%m-%d %H:%M:%f+00:00', JSON_EXTRACT(search_attributes, "$.Datetime01"))),
  Datetime02      TIMESTAMP       GENERATED ALWAYS AS (STRFTIME('%Y-%m-%d %H:%M:%f+00:00', JSON_EXTRACT(search_attributes, "$.Datetime02"))),
  Datetime03      TIMESTAMP       GENERATED ALWAYS AS (STRFTIME('%Y-%m-%d %H:%M:%f+00:00', JSON_EXTRACT(search_attributes, "$.Datetime03"))),
  Double01        DECIMAL(20, 5)  GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Double01")),
  Double02        DECIMAL(20, 5)  GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Double02")),
  Double03        DECIMAL(20, 5)  GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Double03")),
  Int01           BIGINT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Int01")),
  Int02           BIGINT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Int02")),
  Int03           BIGINT          GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Int03")),
  Keyword01       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword01")),
  Keyword02       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword02")),
  Keyword03       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword03")),
  Keyword04       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword04")),
  Keyword05       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword05")),
  Keyword06       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword06")),
  Keyword07       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword07")),
  Keyword08       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword08")),
  Keyword09       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword09")),
  Keyword10       VARCHAR(255)    GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Keyword10")),
  Text01          TEXT            GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Text01"))        STORED,
  Text02          TEXT            GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Text02"))        STORED,
  Text03          TEXT            GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.Text03"))        STORED,
  KeywordList01   TEXT            GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.KeywordList01")) STORED,
  KeywordList02   TEXT            GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.KeywordList02")) STORED,
  KeywordList03   TEXT            GENERATED ALWAYS AS (JSON_EXTRACT(search_attributes, "$.KeywordList03")) STORED,

  PRIMARY KEY (namespace_id, run_id)
);

CREATE INDEX default_idx                ON executions_visibility (namespace_id, (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_time          ON executions_visibility (namespace_id, execution_time,         (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_id             ON executions_visibility (namespace_id, workflow_id,            (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_type           ON executions_visibility (namespace_id, workflow_type_name,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_status                  ON executions_visibility (namespace_id, status,                 (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_history_length          ON executions_visibility (namespace_id, history_length,         (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_history_size_bytes      ON executions_visibility (namespace_id, history_size_bytes,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_duration      ON executions_visibility (namespace_id, execution_duration,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_state_transition_count  ON executions_visibility (namespace_id, state_transition_count, (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_task_queue              ON executions_visibility (namespace_id, task_queue,             (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_workflow_id      ON executions_visibility (namespace_id, parent_workflow_id,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_run_id           ON executions_visibility (namespace_id, parent_run_id,          (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_root_workflow_id        ON executions_visibility (namespace_id, root_workflow_id,       (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_root_run_id             ON executions_visibility (namespace_id, root_run_id,            (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);

-- Indexes for the predefined search attributes
CREATE INDEX by_batcher_user                  ON executions_visibility (namespace_id, BatcherUser,                (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_start_time ON executions_visibility (namespace_id, TemporalScheduledStartTime, (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_by_id      ON executions_visibility (namespace_id, TemporalScheduledById,      (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_schedule_paused      ON executions_visibility (namespace_id, TemporalSchedulePaused,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_namespace_division   ON executions_visibility (namespace_id, TemporalNamespaceDivision,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);


-- Indexes for the pre-allocated custom search attributes
CREATE INDEX by_bool_01     ON executions_visibility (namespace_id, Bool01,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_bool_02     ON executions_visibility (namespace_id, Bool02,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_bool_03     ON executions_visibility (namespace_id, Bool03,     (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_01 ON executions_visibility (namespace_id, Datetime01, (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_02 ON executions_visibility (namespace_id, Datetime02, (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_datetime_03 ON executions_visibility (namespace_id, Datetime03, (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_double_01   ON executions_visibility (namespace_id, Double01,   (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_double_02   ON executions_visibility (namespace_id, Double02,   (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_double_03   ON executions_visibility (namespace_id, Double03,   (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_int_01      ON executions_visibility (namespace_id, Int01,      (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_int_02      ON executions_visibility (namespace_id, Int02,      (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_int_03      ON executions_visibility (namespace_id, Int03,      (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_01  ON executions_visibility (namespace_id, Keyword01,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_02  ON executions_visibility (namespace_id, Keyword02,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_03  ON executions_visibility (namespace_id, Keyword03,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_04  ON executions_visibility (namespace_id, Keyword04,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_05  ON executions_visibility (namespace_id, Keyword05,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_06  ON executions_visibility (namespace_id, Keyword06,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_07  ON executions_visibility (namespace_id, Keyword07,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_08  ON executions_visibility (namespace_id, Keyword08,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_09  ON executions_visibility (namespace_id, Keyword09,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);
CREATE INDEX by_keyword_10  ON executions_visibility (namespace_id, Keyword10,  (COALESCE(close_time, '9999-12-31 23:59:59+00:00')) DESC, start_time DESC, run_id);


CREATE VIRTUAL TABLE executions_visibility_fts_text USING fts5 (
  Text01,
  Text02,
  Text03,
  content='executions_visibility',
  tokenize="unicode61 remove_diacritics 2"
);

-- tokenize args:
-- `unicode61`: accepts unicode chars
-- `remove_diacritics 0`: don't remove diacritics, ie., 'a' is different than 'á'
-- `categories 'C* L* M* N* P* S* Z*'`: all chars in all unicode categories are tokens
-- `separators '♡'`: only the heart symbol (U+2661) is a token separator
CREATE VIRTUAL TABLE executions_visibility_fts_keyword_list USING fts5 (
  TemporalChangeVersion,
  BinaryChecksums,
  BuildIds,
  PauseActivityTypes,
  KeywordList01,
  KeywordList02,
  KeywordList03,
  content='executions_visibility',
  tokenize="unicode61 remove_diacritics 0 categories 'C* L* M* N* P* S* Z*' separators '♡'"
);

CREATE TRIGGER executions_visibility_ai AFTER INSERT ON executions_visibility
BEGIN
  -- insert into fts_text table
  INSERT INTO executions_visibility_fts_text (
    rowid,
    Text01,
    Text02,
    Text03
  ) VALUES (
    NEW.rowid,
    NEW.Text01,
    NEW.Text02,
    NEW.Text03
  );
  -- insert into fts_keyword_list table
  INSERT INTO executions_visibility_fts_keyword_list (
    rowid,
    TemporalChangeVersion,
    BinaryChecksums,
    BuildIds,
    PauseActivityTypes,
    KeywordList01,
    KeywordList02,
    KeywordList03
  ) VALUES (
    NEW.rowid,
    NEW.TemporalChangeVersion,
    NEW.BinaryChecksums,
    NEW.BuildIds,
    NEW.PauseActivityTypes,
    NEW.KeywordList01,
    NEW.KeywordList02,
    NEW.KeywordList03
  );
END;

CREATE TRIGGER executions_visibility_ad AFTER DELETE ON executions_visibility
BEGIN
  -- delete from fts_text table
  INSERT INTO executions_visibility_fts_text (
    executions_visibility_fts_text,
    rowid,
    Text01,
    Text02,
    Text03
  ) VALUES (
    'delete',
    OLD.rowid,
    OLD.Text01,
    OLD.Text02,
    OLD.Text03
  );
  -- delete from fts_keyword_list table
  INSERT INTO executions_visibility_fts_keyword_list (
    executions_visibility_fts_keyword_list,
    rowid,
    TemporalChangeVersion,
    BinaryChecksums,
    BuildIds,
    PauseActivityTypes,
    KeywordList01,
    KeywordList02,
    KeywordList03
  ) VALUES (
    'delete',
    OLD.rowid,
    OLD.TemporalChangeVersion,
    OLD.BinaryChecksums,
    OLD.BuildIds,
    OLD.PauseActivityTypes,
    OLD.KeywordList01,
    OLD.KeywordList02,
    OLD.KeywordList03
  );
END;

CREATE TRIGGER executions_visibility_au AFTER UPDATE ON executions_visibility
BEGIN
  -- update fts_text table
  INSERT INTO executions_visibility_fts_text (
    executions_visibility_fts_text,
    rowid,
    Text01,
    Text02,
    Text03
  ) VALUES (
    'delete',
    OLD.rowid,
    OLD.Text01,
    OLD.Text02,
    OLD.Text03
  );
  INSERT INTO executions_visibility_fts_text (
    rowid,
    Text01,
    Text02,
    Text03
  ) VALUES (
    NEW.rowid,
    NEW.Text01,
    NEW.Text02,
    NEW.Text03
  );
  -- update fts_keyword_list table
  INSERT INTO executions_visibility_fts_keyword_list (
    executions_visibility_fts_keyword_list,
    rowid,
    TemporalChangeVersion,
    BinaryChecksums,
    BuildIds,
    PauseActivityTypes,
    KeywordList01,
    KeywordList02,
    KeywordList03
  ) VALUES (
    'delete',
    OLD.rowid,
    OLD.TemporalChangeVersion,
    OLD.BinaryChecksums,
    OLD.BuildIds,
    OLD.PauseActivityTypes,
    OLD.KeywordList01,
    OLD.KeywordList02,
    OLD.KeywordList03
  );
  INSERT INTO executions_visibility_fts_keyword_list (
    rowid,
    TemporalChangeVersion,
    BinaryChecksums,
    BuildIds,
    PauseActivityTypes,
    KeywordList01,
    KeywordList02,
    KeywordList03
  ) VALUES (
    NEW.rowid,
    NEW.TemporalChangeVersion,
    NEW.BinaryChecksums,
    NEW.BuildIds,
    NEW.PauseActivityTypes,
    NEW.KeywordList01,
    NEW.KeywordList02,
    NEW.KeywordList03
  );
END;
