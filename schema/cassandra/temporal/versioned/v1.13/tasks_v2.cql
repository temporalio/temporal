CREATE TABLE tasks_v2
(
  namespace_id        uuid,
  task_queue_name     text,
  task_queue_type     int, -- enum TaskQueueType {ActivityTask, WorkflowTask}
  type                int, -- enum rowType {Task, TaskQueue} and subqueue id
  pass                bigint, -- pass for tasks (see stride scheduling algorithm for fairness)
  task_id             bigint, -- unique identifier for tasks
  range_id            bigint, -- used to ensure that only one process can write to the table
  ack_level           bigint, -- ack level for the task queue
  task                blob,
  task_encoding       text,
  task_queue          blob,
  task_queue_encoding text,
  PRIMARY KEY ((namespace_id, task_queue_name, task_queue_type), type, pass, task_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };