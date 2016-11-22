CREATE TABLE shards (
  shard_id int,
  PRIMARY KEY (shard_id)
);

CREATE TYPE workflow_execution (
  workflow_id           text,
  run_id                uuid,
  task_list             text,
  history               blob,
  execution_context     blob,
  state                 int,  -- enum WorkflowState {Created, Running, Completed}
  next_event_id         bigint,
  last_processed_event  bigint,
  last_updated_time     timestamp,
  decision_pending      boolean,
);

CREATE TYPE task (
  workflow_id      text,
  run_id           uuid,
  task_id          uuid,
  task_list        text,
  type             int,  -- enum TaskType {ActivityTask, DecisionTask}
  schedule_id      bigint,
  visibility_time  timestamp,
  lock_token       uuid, -- Token for transfer agent to lock the task while it is working on it
  delivery_count   int,
);

CREATE TABLE executions (
  shard_id           int,
  type               int, -- enum RowType { Execution, TransferTask, TimerTask}
  workflow_id        text,
  task_id            uuid, -- unique identifier for transfer and timer tasks for an execution
  execution          frozen<workflow_execution>,
  transfer           frozen<task>,
  next_event_id      bigint,  -- This is needed to make conditional updates on session history
  lock_token         uuid,
  PRIMARY KEY  (shard_id, type, workflow_id, task_id)
);

CREATE TABLE tasks (
  task_list        text,
  type             int, -- enum TaskType {ActivityTask, DecisionTask}
  workflow_id      text,
  run_id           uuid,
  task_id          uuid,
  task             frozen<task>,
  lock_token       uuid, -- Token for engine to dispatch tasks
  PRIMARY KEY (task_list, type, workflow_id, run_id, task_id)
);