-- same as closed_executions but order by close_time
CREATE TABLE closed_executions_v2 (
  domain_id            uuid,
  domain_partition     int,
  workflow_id          text,
  run_id               uuid,
  start_time           timestamp,
  close_time           timestamp,
  status               int,  -- enum WorkflowExecutionCloseStatus {COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  workflow_type_name   text,
  history_length       bigint,
  PRIMARY KEY  ((domain_id, domain_partition), close_time, run_id)
) WITH CLUSTERING ORDER BY (close_time DESC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  }
  AND GC_GRACE_SECONDS = 172800;

CREATE INDEX closed_by_workflow_id_v2 ON closed_executions_v2 (workflow_id);
CREATE INDEX closed_by_close_time_v2 ON closed_executions_v2 (close_time);
CREATE INDEX closed_by_type_v2 ON closed_executions_v2 (workflow_type_name);
CREATE INDEX closed_by_status_v2 ON closed_executions_v2 (status);