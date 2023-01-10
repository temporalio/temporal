CREATE TABLE executions_visibility (
  NamespaceId         CHAR(64) NOT NULL,
  RunId               CHAR(64) NOT NULL,
  StartTime           TIMESTAMP NOT NULL,
  ExecutionTime       TIMESTAMP NOT NULL,
  WorkflowId          VARCHAR(255) NOT NULL,
  WorkflowType        VARCHAR(255) NOT NULL,
  ExecutionStatus     INTEGER NOT NULL,  -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  CloseTime           TIMESTAMP NULL,
  HistoryLength       BIGINT,
  Memo                BYTEA,
  MemoEncoding        VARCHAR(64) NOT NULL,
  TaskQueue           VARCHAR(255) DEFAULT '' NOT NULL,

  PRIMARY KEY  (NamespaceId, RunId)
);

CREATE INDEX by_type_start_time ON executions_visibility (NamespaceId, WorkflowType, ExecutionStatus, StartTime DESC, RunId);
CREATE INDEX by_workflow_id_start_time ON executions_visibility (NamespaceId, WorkflowId, ExecutionStatus, StartTime DESC, RunId);
CREATE INDEX by_status_by_start_time ON executions_visibility (NamespaceId, ExecutionStatus, StartTime DESC, RunId);
CREATE INDEX by_type_close_time ON executions_visibility (NamespaceId, WorkflowType, ExecutionStatus, CloseTime DESC, RunId);
CREATE INDEX by_workflow_id_close_time ON executions_visibility (NamespaceId, WorkflowId, ExecutionStatus, CloseTime DESC, RunId);
CREATE INDEX by_status_by_close_time ON executions_visibility (NamespaceId, ExecutionStatus, CloseTime DESC, RunId);
CREATE INDEX by_close_time_by_status ON executions_visibility (NamespaceId, CloseTime DESC, RunId, ExecutionStatus);
