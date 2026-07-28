CREATE TABLE executions_visibility (
  namespace_id         CHAR(64) NOT NULL,
  run_id               CHAR(64) NOT NULL,
  start_time           DATETIME(6) NOT NULL,
  execution_time       DATETIME(6) NOT NULL,
  workflow_id          VARCHAR(255) NOT NULL,
  workflow_type_name   VARCHAR(255) NOT NULL,
  status               INT NOT NULL,  -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  close_time           DATETIME(6) NULL,
  history_length       BIGINT,
  memo                 BLOB,
  encoding             VARCHAR(64) NOT NULL,
  task_queue           VARCHAR(255) DEFAULT '' NOT NULL,

  PRIMARY KEY  (namespace_id, run_id)
);

CREATE INDEX by_type_start_time ON executions_visibility (namespace_id, workflow_type_name, status, start_time DESC, run_id);
CREATE INDEX by_workflow_id_start_time ON executions_visibility (namespace_id, workflow_id, status, start_time DESC, run_id);
CREATE INDEX by_status_by_start_time ON executions_visibility (namespace_id, status, start_time DESC, run_id);
CREATE INDEX by_type_close_time ON executions_visibility (namespace_id, workflow_type_name, status, close_time DESC, run_id);
CREATE INDEX by_workflow_id_close_time ON executions_visibility (namespace_id, workflow_id, status, close_time DESC, run_id);
CREATE INDEX by_status_by_close_time ON executions_visibility (namespace_id, status, close_time DESC, run_id);
