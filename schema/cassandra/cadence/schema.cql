CREATE TYPE shard (
  shard_id                    int,
  owner                       text, -- Host identifier processing the shard
  -- Range identifier used for generating ack ids for tasks within shard.
  -- Also used for optimistic concurrency and all writes to a shard are conditional on this value.
  range_id                    bigint,
  -- This field keeps track of number of times owner for a shard changes before updating range_id or ack_levels
  stolen_since_renew          int,
  updated_at                  timestamp,
  replication_ack_level       bigint,
  transfer_ack_level          bigint, -- TO BE DEPRECATED, IN FAVOR OF cluster_transfer_ack_level
  timer_ack_level             timestamp, -- TO BE DEPRECATED, IN FAVOR OF cluster_timer_ack_level
  -- Mapping of cluster to corresponding transfer ack level
  cluster_transfer_ack_level  map<text, bigint>,
  -- Mapping of cluster to corresponding timer ack level
  cluster_timer_ack_level     map<text, timestamp>,
  domain_notification_version bigint, -- the global domain change version this shard is aware of
  -- Mapping of (remote) cluster to corresponding replication level (last replicated task_id)
  cluster_replication_level   map<text, bigint>,
  -- Mapping of (remote) cluster to corresponding replication DLQ ack level (last replicated task_id)
  replication_dlq_ack_level   map<text, bigint>,
);

--- Workflow execution and mutable state ---
CREATE TYPE workflow_execution (
  domain_id                        uuid,
  workflow_id                      text,
  run_id                           uuid,
  parent_domain_id                 uuid,   -- Domain ID of parent workflow which started the workflow execution
  parent_workflow_id               text,   -- ID of parent workflow which started the workflow execution
  parent_run_id                    uuid,   -- RunID of parent workflow which started the workflow execution
  initiated_id                     bigint, -- Initiated event ID of parent workflow which started this execution
  completion_event_batch_id        bigint,
  completion_event                 blob,   -- Completion event used to communicate result to parent workflow execution
  completion_event_data_encoding   text, -- Protocol used for history serialization
  task_list                        text,
  workflow_type_name               text,
  workflow_timeout                 int,  -- Workflow ExecutionStartToCloseTimeoutSeconds
  decision_task_timeout            int,  -- decision start to close timeout
  execution_context                blob,
  state                            int,  -- enum WorkflowState {Created, Running, Completed}
  close_status                     int,  -- enum WorkflowCloseStatus {None, Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut}
  last_processed_event             bigint,
  start_time                       timestamp,
  last_updated_time                timestamp,
  create_request_id                uuid,
  decision_version                 bigint,
  decision_schedule_id             bigint,
  decision_started_id              bigint,
  decision_request_id              text,    -- Identifier used by matching engine for retrying history service calls for recording task is started
  decision_timeout                 int,
  decision_attempt                 bigint,
  decision_timestamp               bigint,  -- this is decision started time
  decision_scheduled_timestamp     bigint,   -- this is decision scheduled time
  decision_original_scheduled_timestamp     bigint,   -- this is scheduled time of the first decision during heartbeat
  cancel_requested                 boolean,
  cancel_request_id                text,
  sticky_task_list                 text,   -- sticky worker task list
  sticky_schedule_to_start_timeout int,
  client_library_version           text,
  client_feature_version           text,
  client_impl                      text,
  attempt                          int,    -- starting from 0 (for initial non-retry)
  has_retry_policy                 boolean,-- If there is a retry policy
  init_interval                    int,    -- initial retry interval, in seconds
  backoff_coefficient              double,
  max_interval                     int,    -- max retry interval in seconds
  expiration_time                  timestamp, -- retry expiration time
  max_attempts                     int,    -- max number of attempts including initial non-retry attempt
  non_retriable_errors             list<text>,
  event_store_version              int, -- indicates which version of events persistence is using
  signal_count                     int,
  branch_token                     blob,
  history_size                     bigint,
  last_first_event_id              bigint,
  next_event_id                    bigint,
  cron_schedule                    text,
  expiration_seconds               int,    -- retry expiration duration in seconds
  last_event_task_id               bigint,
  auto_reset_points                blob, -- the resetting points for auto-reset feature
  auto_reset_points_encoding       text, -- encoding for auto_reset_points_data
  search_attributes                map<text, blob>,
  memo                             map<text, blob>
);

-- Replication information for each cluster
CREATE TYPE replication_info (
  version       bigint,
  last_event_id bigint,
);

-- This is used to store replication information for a workflow execution
CREATE TYPE replication_state (
  current_version                  bigint, -- current version for domain, incremented on failover
  start_version                    bigint, -- version of domain when the workflow execution was started
  last_write_version               bigint, -- version of domain when the last event was written to history
  last_write_event_id              bigint, -- last written event id for a given version
  last_replication_info            map<text, frozen<replication_info>>, -- information about replication events from other clusters
);

-- TODO: Remove fields that are left over from activity and workflow tasks.
CREATE TYPE transfer_task (
  domain_id                  uuid,   -- The domain ID that this transfer task belongs to
  workflow_id                text,   -- The workflow ID that this transfer task belongs to
  run_id                     uuid,   -- The run ID that this transfer task belongs to
  task_id                    bigint,
  visibility_ts              timestamp, -- The timestamp when the transfer task is generated
  target_domain_id           uuid,   -- The external domain ID that this transfer task is doing work for.
  target_workflow_id         text,   -- The external workflow ID that this transfer task is doing work for.
  target_run_id              uuid,   -- The external run ID that this transfer task is doing work for.
  target_child_workflow_only boolean, -- The whether target child workflow only.
  task_list                  text,
  type                       int,    -- enum TaskType {ActivityTask, DecisionTask, DeleteExecution, CancelExecution, StartChildExecution, ReplicationTask}
  schedule_id                bigint,
  version                    bigint, -- the failover version when this task is created, used to compare against the mutable state, in case the events got overwritten
  record_visibility          boolean, -- indicates whether or not to create a visibility record
);

CREATE TYPE replication_task (
  domain_id                  uuid,   -- The domain ID that this replication task belongs to
  workflow_id                text,   -- The workflow ID that this replication task belongs to
  run_id                     uuid,   -- The run ID that this replication task belongs to
  task_id                    bigint,
  type                       int,    -- enum TaskType {History, Heartbeat}
  first_event_id             bigint,  -- Used by ReplicationTask to set the first event ID of the applied transaction
  next_event_id              bigint,  -- Used by ReplicationTask to set the next event ID of the applied transaction
  version                    bigint,  -- Used by ReplicationTask to set the failover version of the applied transaction
  last_replication_info      map<text, frozen<replication_info>>, -- Used by replication task to snapshot replication information when the transaction was applied
  scheduled_id               bigint, -- Used by ReplicationTask to sync activity info
  event_store_version        int, -- indicates which version of event store to query
  branch_token               blob, -- if eventV2, then query with this token
  new_run_event_store_version        int, -- indicates which version of event store to query for new run(continueAsNew)
  new_run_branch_token               blob, -- if eventV2, then query with this token for new run(continueAsNew)
  reset_workflow             boolean, -- whether the task is for resetWorkflowExecution
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
  schedule_attempt bigint, -- Used to retry failed decision tasks using mutable state
  version          bigint, -- the failover version when this task is created, used to compare against the mutable state, in case the events got overwritten
);

-- Workflow activity in progress mutable state
CREATE TYPE activity_info (
  version                   bigint,
  schedule_id               bigint,
  scheduled_event_batch_id  bigint,
  scheduled_event           blob,  -- deprecated
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
  timer_task_status         int,    -- Indicates whether timers are created for this activity.
  attempt                   int,    -- starting from 0 (for initial non-retry)
  task_list                 text,
  started_identity          text,   -- last started poller's identity
  has_retry_policy          boolean,-- If there is a retry policy
  init_interval             int,    -- initial retry interval, in seconds
  backoff_coefficient       double,
  max_interval              int,    -- max retry interval in seconds
  expiration_time           timestamp, -- retry expiration time
  max_attempts              int,    -- max number of attempts including initial non-retry attempt
  non_retriable_errors      list<text>,
  last_failure_reason       text,
  last_worker_identity      text, -- Worker that returns the last failure reason
  last_failure_details      blob,
  event_data_encoding       text, -- Protocol used for history serialization
);

-- User timer details
CREATE TYPE timer_info (
  version       bigint,
  timer_id      text,      -- User defined timer ID
  started_id    bigint,    -- The event ID corresponding to timer started.
  expiry_time   timestamp, -- Timestamp at which this timer expires or fires
  -- task_id is a misleading variable, it actually serves
  -- the purpose of indicating whether a timer task is
  -- generated for this timer info
  task_id       bigint,
);

-- Child execution in progress mutable state
CREATE TYPE child_execution_info (
  version                   bigint,
  initiated_id              bigint,
  initiated_event_batch_id  bigint,
  initiated_event           blob,
  started_id                bigint,
  started_workflow_id       text,
  started_run_id            uuid,
  started_event             blob, -- deprecated
  create_request_id         uuid,
  event_data_encoding       text, -- Protocol used for history serialization
  domain_name               text,
  workflow_type_name        text,
  parent_close_policy       int,
);

-- External workflow cancellation in progress mutable state
CREATE TYPE request_cancel_info (
  version                   bigint,
  initiated_event_batch_id  bigint,
  initiated_id              bigint,
  cancel_request_id         text,
);

-- External workflow signal in progress mutable state
CREATE TYPE signal_info (
  version                   bigint,
  initiated_event_batch_id  bigint,
  initiated_id              bigint,
  signal_request_id         uuid,
  signal_name               text,
  input                     blob,
  control                   blob,
);

-- Activity or workflow task in a task list
CREATE TYPE task (
  domain_id        uuid,
  workflow_id      text,
  run_id           uuid,
  schedule_id      bigint,
  created_time     timestamp
);

CREATE TYPE task_list (
  domain_id        uuid,
  name             text,
  type             int, -- enum TaskRowType {ActivityTask, DecisionTask}
  ack_level        bigint, -- task_id of the last acknowledged message
  kind             int, -- enum TaskListKind {Normal, Sticky}
  last_updated     timestamp
);

CREATE TYPE domain (
  id          uuid,
  name        text,
  status      int, -- enum DomainStatus {Registered, Deprecated, Deleted}
  description text,
  data        map<text,text>, -- Used for customized domain information, key values pair
  owner_email text,
);

CREATE TYPE domain_config (
  retention   int,
  emit_metric boolean,
  archival_bucket text, -- deprecated, use the two uri fields below
  archival_status int, -- deprecated, use the two status fields below
  history_archival_status int,
  history_archival_uri text,
  visibility_archival_status int,
  visibility_archival_uri text,
  bad_binaries    blob,
  bad_binaries_encoding blob,
);

CREATE TYPE cluster_replication_config (
  cluster_name text,
);

CREATE TYPE domain_replication_config (
  active_cluster_name text,
  clusters            list<frozen<cluster_replication_config>>
);

CREATE TYPE serialized_event_batch (
  encoding_type text,
  version       int,
  data          blob,
);

-- Storage for out of order replication tasks for an execution
CREATE TYPE buffered_replication_task_info (
  first_event_id  bigint,
  next_event_id   bigint,
  version         bigint,
  history         frozen<serialized_event_batch>,
  new_run_history frozen<serialized_event_batch>,
  event_store_version                int, -- indicates which version of event store to query
  new_run_event_store_version        int, -- indicates which version of event store to query for new run(continueAsNew)
);

-- for history v2 events
CREATE TYPE branch_range (
  branch_id   uuid,
  end_node_id bigint, -- exclusive node_id to represent the stopping point for this range
);

CREATE TYPE checksum (
  version int,  -- version of the payload used to generate checksum
  flavor  int,  -- type of checksum e.g crc32OverThrift
  value   blob, -- checksum bytes
);

CREATE TABLE executions (
  shard_id                       int,
  type                           int, -- enum RowType { Shard, Execution, TransferTask, TimerTask, ReplicationTask}
  domain_id                      uuid,
  workflow_id                    text,
  run_id                         uuid,
  current_run_id                 uuid,
  visibility_ts                  timestamp, -- unique identifier for timer tasks for an execution
  task_id                        bigint, -- unique identifier for transfer and timer tasks for an execution
  shard                          frozen<shard>,
  execution                      frozen<workflow_execution>,
  transfer                       frozen<transfer_task>,
  replication                    frozen<replication_task>,
  timer                          frozen<timer_task>,
  next_event_id                  bigint,  -- This is needed to make conditional updates on session history
  range_id                       bigint, -- Increasing sequence identifier for transfer queue, checkpointed into shard info
  activity_map                   map<bigint, frozen<activity_info>>,
  timer_map                      map<text, frozen<timer_info>>,
  child_executions_map           map<bigint, frozen<child_execution_info>>,
  request_cancel_map             map<bigint, frozen<request_cancel_info>>,
  signal_map                     map<bigint, frozen<signal_info>>,
  signal_requested               set<uuid>,
  buffered_events_list           list<frozen<serialized_event_batch>>,
  replication_state              frozen<replication_state>, -- Replication information part of mutable state
  buffered_replication_tasks_map map<bigint, frozen<buffered_replication_task_info>>,
  workflow_last_write_version    bigint,
  workflow_state                 int,
  version_histories              blob, -- the metadata of history branching
  version_histories_encoding     text,
  checksum                       frozen<checksum>,
  PRIMARY KEY  (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE history_node (
  tree_id           uuid,
  branch_id         uuid,
  node_id           bigint, -- node_id: first eventID in a batch of events
  txn_id            bigint, -- for override the same node_id: bigger txn_id wins
  data                blob, -- Batch of workflow execution history events as a blob
  data_encoding       text, -- Protocol used for history serialization
  PRIMARY KEY ((tree_id), branch_id, node_id, txn_id )
  ) WITH CLUSTERING ORDER BY (branch_id ASC, node_id ASC, txn_id DESC)
    AND COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };

CREATE TABLE history_tree (
  tree_id           uuid,
  branch_id         uuid,
  ancestors         list<frozen<branch_range>>,
  fork_time         timestamp, -- For fork operation to prevent race condition to leak event data when forking branches
  info              text, -- For background cleanup when fork operation cannot finish self cleanup due to crash
  PRIMARY KEY ((tree_id), branch_id )
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

-- this table is only used for storage of mapping of domain uuid to domain name
CREATE TABLE domains (
  id     uuid,
  domain frozen<domain>,
  config frozen<domain_config>,
  PRIMARY KEY (id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE domains_by_name_v2 (
  domains_partition             int,
  name                          text,
  domain                        frozen<domain>,
  config                        frozen<domain_config>,
  replication_config            frozen<domain_replication_config>, -- indicating active cluster and standby cluster used for replication
  is_global_domain              boolean, -- indicating whether a domain is a global domain
  config_version                bigint, -- indicating the version of domain config, excluding the failover / change of active cluster name
  failover_version              bigint, -- indicating the version of active domain only, used for domain failover
  failover_notification_version bigint, -- indicating the last change related to domain failover
  notification_version          bigint,
  PRIMARY KEY (domains_partition, name)
)  WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
   };

INSERT INTO domains_by_name_v2 (
   domains_partition,
   name,
   domain,
   config,
   is_global_domain,
   config_version,
   failover_version,
   failover_notification_version,
   notification_version
) VALUES (
   0,
   'cadence-system',
   {
       id: 32049b68-7872-4094-8e63-d0dd59896a83,
       name: 'cadence-system',
       description: 'cadence system workflow domain',
       owner_email: 'cadence-dev-group@uber.com'
   },
   {
       retention:3,
       emit_metric:False
   },
   False,
   0,
   -24,
   -1,
   -1
) IF NOT EXISTS;

INSERT INTO domains (
   id,
   domain
) VALUES (
   32049b68-7872-4094-8e63-d0dd59896a83,
   {
       name: 'cadence-system'
   }
) IF NOT EXISTS;

CREATE TABLE queue (
  queue_type      int,
  message_id      int,
  message_payload blob,
  PRIMARY KEY  (queue_type, message_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE queue_metadata (
  queue_type        int,
  cluster_ack_level map<text, bigint>,
  version           bigint,
PRIMARY KEY (queue_type)
)  WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
   };
