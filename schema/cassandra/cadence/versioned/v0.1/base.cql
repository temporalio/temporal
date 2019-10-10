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
  timer_ack_level     timestamp,
);

--- Workflow execution and mutable state ---
CREATE TYPE workflow_execution (
  domain_id              uuid,
  workflow_id            text,
  run_id                 uuid,
  parent_domain_id       uuid,   -- Domain ID of parent workflow which started the workflow execution
  parent_workflow_id     text,   -- ID of parent workflow which started the workflow execution
  parent_run_id          uuid,   -- RunID of parent workflow which started the workflow execution
  initiated_id           bigint, -- Initiated event ID of parent workflow which started this execution
  completion_event       blob,   -- Completion event used to communicate result to parent workflow execution
  task_list              text,
  workflow_type_name     text,
  decision_task_timeout  int,
  execution_context      blob,
  state                  int,  -- enum WorkflowState {Created, Running, Completed}
  close_status           int,  -- enum WorkflowCloseStatus {None, Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut}
  next_event_id          bigint,
  last_processed_event   bigint,
  start_time             timestamp,
  last_updated_time      timestamp,
  create_request_id      uuid,
  decision_schedule_id   bigint,
  decision_started_id    bigint,
  decision_request_id    text,    -- Identifier used by matching engine for retrying history service calls for recording task is started
  decision_timeout       int,
  cancel_requested       boolean,
  cancel_request_id      text,
);

-- TODO: Remove fields that are left over from activity and workflow tasks.
CREATE TYPE transfer_task (
  domain_id           uuid,   -- The domain ID that this transfer task belongs to
  workflow_id         text,   -- The workflow ID that this transfer task belongs to
  run_id              uuid,   -- The run ID that this transfer task belongs to
  task_id             bigint,
  target_domain_id    uuid,   -- The external domain ID that this transfer task is doing work for.
  target_workflow_id  text,   -- The external workflow ID that this transfer task is doing work for.
  target_run_id       uuid,   -- The external run ID that this transfer task is doing work for.
  task_list           text,
  type                int,    -- enum TaskType {ActivityTask, DecisionTask, DeleteExecution, CancelExecution, StartChildExecution}
  schedule_id         bigint,
);

CREATE TYPE timer_task (
  domain_id        uuid,
  workflow_id      text,
  run_id           uuid,
  visibility_ts    timestamp,
  task_id          bigint,
  type             int,  -- enum TaskType {DecisionTaskTimeout, ActivityTaskTimeout, UserTimer}
  timeout_type     int, -- enum TimeoutType in IDL {START_TO_CLOSE, SCHEDULE_TO_START, SCHEDULE_TO_CLOSE, HEARTBEAT}
  event_id         bigint, -- Corresponds to event ID in history that is responsible for this timer.
);

-- Workflow activity in progress mutable state
CREATE TYPE activity_info (
  schedule_id               bigint,
  scheduled_event           blob,
  scheduled_time            timestamp,
  started_id                bigint,
  started_event             blob,
  started_time              timestamp,
  activity_id               text,    -- Client generated unique ID for the activity.
  request_id                text,    -- Identifier used by matching engine for retrying history service calls for recording task is started
  details                   blob,
  schedule_to_start_timeout int,
  schedule_to_close_timeout int,
  start_to_close_timeout    int,
  heart_beat_timeout        int,
  cancel_requested          boolean, -- If a cancel request is made to cancel the activity in progress.
  cancel_request_id         bigint,  -- Event ID that identifies the cancel request.
  last_hb_updated_time      timestamp, -- Last time the heartbeat is received.
  timer_task_status         int,    -- Indicates wheter timers are created for this activity.
);

-- User timer details
CREATE TYPE timer_info (
  timer_id      text,      -- User defined timer ID
  started_id    bigint,    -- The event ID corresponding to timer started.
  expiry_time   timestamp, -- Timestamp at which this timer expires or fires
  task_id       bigint,    -- The task ID if we have one created for this timer
);

-- Child execution in progress mutable state
CREATE TYPE child_execution_info (
  initiated_id      bigint,
  initiated_event   blob,
  started_id        bigint,
  started_event     blob,
  create_request_id uuid,
);

-- External workflow cancellation in progress mutable state
CREATE TYPE request_cancel_info (
  initiated_id      bigint,
  cancel_request_id text,
);

-- Activity or workflow task in a task list
CREATE TYPE task (
  domain_id        uuid,
  workflow_id      text,
  run_id           uuid,
  schedule_id      bigint,
);

CREATE TYPE task_list (
  domain_id        uuid,
  name             text,
  type             int, -- enum TaskRowType {ActivityTask, DecisionTask}
  ack_level        bigint, -- task_id of the last acknowledged message
);

CREATE TYPE domain (
  id          uuid,
  name        text,
  status      int, -- enum DomainStatus {Registered, Deprecated, Deleted}
  description text,
  owner_email text,
);

CREATE TYPE domain_config (
  retention int,
  emit_metric boolean
);

CREATE TABLE executions (
  shard_id             int,
  type                 int, -- enum RowType { Shard, Execution, TransferTask, TimerTask}
  domain_id            uuid,
  workflow_id          text,
  run_id               uuid,
  current_run_id       uuid,
  visibility_ts        timestamp, -- unique identifier for timer tasks for an execution
  task_id              bigint, -- unique identifier for transfer and timer tasks for an execution
  shard                frozen<shard>,
  execution            frozen<workflow_execution>,
  transfer             frozen<transfer_task>,
  timer                frozen<timer_task>,
  next_event_id        bigint,  -- This is needed to make conditional updates on session history
  range_id             bigint, -- Increasing sequence identifier for transfer queue, checkpointed into shard info
  activity_map         map<bigint, frozen<activity_info>>,
  timer_map            map<text, frozen<timer_info>>,
  child_executions_map map<bigint, frozen<child_execution_info>>,
  request_cancel_map   map<bigint, frozen<request_cancel_info>>,
  PRIMARY KEY  (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE events (
  domain_id      uuid,
  workflow_id    text,
  run_id         uuid,
  -- We insert a batch of events with each append transaction.
  -- This field stores the event id of first event in the batch.
  first_event_id bigint,
  range_id       bigint,
  tx_id          bigint,
  data           blob, -- Batch of workflow execution history events as a blob
  data_encoding  text, -- Protocol used for history serialization
  data_version   int,  -- history blob version
  PRIMARY KEY ((domain_id, workflow_id, run_id), first_event_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  domain_id        uuid,
  task_list_name   text,
  task_list_type   int, -- enum TaskListType {ActivityTask, DecisionTask}
  type             int, -- enum rowType {Task, TaskList}
  task_id          bigint,  -- unique identifier for tasks, monotonically increasing
  range_id         bigint, -- Used to ensure that only one process can write to the table
  task             frozen<task>,
  task_list        frozen<task_list>,
  PRIMARY KEY ((domain_id, task_list_name, task_list_type), type, task_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE domains (
  id     uuid,
  domain frozen<domain>,
  config frozen<domain_config>,
  PRIMARY KEY (id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };
