CREATE TABLE shards (
  shard_id           int,
  PRIMARY KEY (shard_id)
);

CREATE TYPE shard (
  shard_id            int,
  owner               text, -- Host identifier processing the shard
  -- Range identifier used for generating ack ids for tasks within shard.
  -- Also used for optimistic concurrency and all writes to a shard are conditional on this value.
  range_id            bigint,
  -- This field keeps track of number of times owner for a shard changes before updating range_id or ack_levels
  stolen_since_renew  int,
  updated_at          timestamp,
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
  create_request_id     uuid,
);

-- TODO: Remove fields that are left over from activitiy and workflow tasks.
CREATE TYPE transfer_task (
  workflow_id      text,
  run_id           uuid,
  task_id          bigint,
  task_list        text,
  type             int,  -- enum TaskType {ActivityTask, DecisionTask}
  schedule_id      bigint,
);

CREATE TYPE timer_task (
  workflow_id      text,
  run_id           uuid,
  task_id          bigint,
  type             int,  -- enum TaskType {DecisionTaskTimeout, ActivityTaskTimeout, UserTimer}
  timeout_type     int, -- enum TimeoutType in IDL {START_TO_CLOSE, SCHEDULE_TO_START, SCHEDULE_TO_CLOSE, HEARTBEAT}
  event_id         bigint, -- Corresponds to event ID in history that is responsible for this timer.
);

-- Workflow activity in progress mutable state
CREATE TYPE activity_info (
  schedule_id               bigint,
  started_id                bigint,
  activity_id               text,    -- Client generated unique ID for the activity.
  request_id                text,    -- Identifier used by matching engine for retrying history service calls for recording task is started
  details                   blob,
  schedule_to_start_timeout int,
  schedule_to_close_timeout int,
  start_to_close_timeout    int,
  heart_beat_timeout        int,
  cancel_requested          boolean, -- If a cancel request is made to cancel the activity in progress.
  cancel_request_id         bigint,  -- Event ID that identifies the cancel request.
);

-- User timer details
CREATE TYPE timer_info (
  timer_id      text,      -- User defined timer ID
  started_id    bigint,    -- The event ID corresponding to timer started.
  expiry_time   timestamp, -- Timestamp at which this timer expires or fires
  task_id       bigint,    -- The task ID if we have one created for this timer
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
  activity_map       map<bigint, frozen<activity_info>>,
  timer_map          map<text, frozen<timer_info>>,
  PRIMARY KEY  (shard_id, type, workflow_id, run_id, task_id)
);

-- Activity or workflow task in a task list
CREATE TYPE task (
  workflow_id      text,
  run_id           uuid,
  schedule_id      bigint,
);

CREATE TYPE task_list (
  name             text,
  type             int, -- enum TaskRowType {ActivityTask, DecisionTask}
  ack_level        bigint, -- task_id of the last acknowledged message
);

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  task_list_name   text,
  task_list_type   int, -- enum TaskListType {ActivityTask, DecisionTask}
  type             int, -- enum rowType {Task, TaskList}
  task_id          bigint,  -- unique identifier for tasks, monotonically increasing
  range_id         bigint static, -- Used to ensure that only one process can write to the table
  task             frozen<task>,
  task_list        frozen<task_list>,
  PRIMARY KEY ((task_list_name, task_list_type), type, task_id)
);