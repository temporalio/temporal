CREATE TABLE shards (
  shard_id           int,
  PRIMARY KEY (shard_id)
);

CREATE TYPE shard (
  shard_id            int,
  range_id            bigint,
  transfer_ack_level  bigint,
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

-- TODO: Remove fields that are left over from activitiy and workflow tasks.
CREATE TYPE transfer_task (
  workflow_id      text,
  run_id           uuid,
  task_id          bigint,
  task_list        text,
  type             int,  -- enum TaskType {ActivityTask, DecisionTask}
  schedule_id      bigint,
  visibility_time  timestamp,
  lock_token       uuid, -- Token for transfer agent to lock the task while it is working on it
  delivery_count   int,
);

CREATE TYPE timer_task (
  workflow_id      text,
  run_id           uuid,
  task_id          bigint,
  type             int,  -- enum TaskType {DecisionTaskTimeout, ActivityTaskTimeout, UserTimer}
  timeout_type     int, -- enum TimeoutType in IDL {START_TO_CLOSE, SCHEDULE_TO_START, SCHEDULE_TO_CLOSE, HEARTBEAT}
  event_id         bigint, -- Corresponds to event ID in history that is responsible for this timer.
);

CREATE TYPE activity_info (
  schedule_id               bigint,
  started_id                bigint,
  details                   blob,
  schedule_to_start_timeout int,
  schedule_to_close_timeout int,
  start_to_close_timeout    int,
  heart_beat_timeout        int,
);

-- Activity or workflow task in a task list
CREATE TYPE task (
  workflow_id      text,
  run_id           uuid,
  schedule_id      bigint,
);

CREATE TABLE executions (
  shard_id           int,
  type               int, -- enum RowType { Shard, Execution, TransferTask, TimerTask}
  workflow_id        text,
  run_id             uuid,
  current_run_id     uuid,
  task_id            bigint, -- unique identifier for transfer and timer tasks for an execution
  shard              frozen<shard>,
  execution          frozen<workflow_execution>,
  transfer           frozen<transfer_task>,
  timer              frozen<timer_task>,
  next_event_id      bigint,  -- This is needed to make conditional updates on session history
  range_id           bigint static, -- Increasing sequence identifier for transfer queue, checkpointed into shard info
  lock_token         uuid,
  activity_map       map<bigint, frozen<activity_info>>,
  PRIMARY KEY  (shard_id, type, workflow_id, run_id, task_id)
);

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  task_list        text,
  type             int, -- enum TaskType {ActivityTask, DecisionTask}
  task_id          bigint,  -- unique identifier for tasks, monotonically increasing
  range_id         bigint static, -- Used to ensure that only one process can write to the table
  task             frozen<task>,
  PRIMARY KEY ((task_list, type), task_id)
);