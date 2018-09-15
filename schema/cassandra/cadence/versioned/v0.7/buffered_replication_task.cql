-- Storage for out of order replication tasks for an execution
CREATE TYPE buffered_replication_task_info (
  first_event_id  bigint,
  next_event_id   bigint,
  version         bigint,
  history         frozen<serialized_event_batch>,
  new_run_history frozen<serialized_event_batch>,
);

-- Buffered replications tasks information part of mutable state
ALTER TABLE executions ADD buffered_replication_tasks_map map<bigint, frozen<buffered_replication_task_info>>;