CREATE TABLE open_executions (
  namespace_id            uuid,
  namespace_partition     int,
  workflow_id          text,
  run_id               uuid,
  start_time           timestamp,
  execution_time       timestamp,
  workflow_type_name   text,
  memo                 blob,
  encoding             text,
  task_queue            text,
  PRIMARY KEY  ((namespace_id, namespace_partition), start_time, run_id)
) WITH CLUSTERING ORDER BY (start_time DESC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
    'tombstone_threshold': 0.6
  }
  AND GC_GRACE_SECONDS = 60;


CREATE INDEX open_by_workflow_id ON open_executions (workflow_id);
CREATE INDEX open_by_type ON open_executions (workflow_type_name);


CREATE TABLE closed_executions (
  namespace_id            uuid,
  namespace_partition     int,
  workflow_id          text,
  run_id               uuid,
  start_time           timestamp,
  execution_time       timestamp,
  close_time           timestamp,
  status               int,  -- enum WorkflowExecutionStatus {COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  workflow_type_name   text,
  history_length       bigint,
  memo                 blob,
  encoding             text,
  task_queue            text,
  PRIMARY KEY  ((namespace_id, namespace_partition), close_time, run_id)
) WITH CLUSTERING ORDER BY (close_time DESC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  }
  AND GC_GRACE_SECONDS = 172800;

CREATE INDEX closed_by_workflow_id ON closed_executions (workflow_id);
CREATE INDEX closed_by_close_time ON closed_executions (close_time);
CREATE INDEX closed_by_type ON closed_executions (workflow_type_name);
CREATE INDEX closed_by_status ON closed_executions (status);