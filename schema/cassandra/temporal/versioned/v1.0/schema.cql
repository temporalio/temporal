CREATE TYPE serialized_event_batch (
  encoding_type text,
  version       int,
  data          blob
);

CREATE TABLE executions (
  shard_id                       int,
  type                           int, -- enum RowType { Shard, Execution, TransferTask, TimerTask, ReplicationTask}
  namespace_id                      uuid,
  workflow_id                    text,
  run_id                         uuid,
  current_run_id                 uuid,
  visibility_ts                  timestamp, -- unique identifier for timer tasks for an execution
  task_id                        bigint, -- unique identifier for transfer and timer tasks for an execution
  shard                          blob,
  shard_encoding                 text,
  execution                      blob,
  execution_encoding             text,
  execution_state                blob,
  execution_state_encoding       text,
  transfer                       blob,
  transfer_encoding              text,
  replication                    blob,
  replication_encoding           text,
  timer                          blob,
  timer_encoding                 text,
  next_event_id                  bigint,  -- This is needed to make conditional updates on session history
  range_id                       bigint, -- Increasing sequence identifier for transfer queue, checkpointed into shard info
  activity_map                   map<bigint, blob>,
  activity_map_encoding          text,
  timer_map                      map<text, blob>,
  timer_map_encoding             text,
  child_executions_map           map<bigint, blob>,
  child_executions_map_encoding  text,
  request_cancel_map             map<bigint, blob>,
  request_cancel_map_encoding    text,
  signal_map                     map<bigint, blob>,
  signal_map_encoding            text,
  signal_requested               set<uuid>,
  buffered_events_list           list<frozen<serialized_event_batch>>,
  replication_metadata           blob, -- Contains versions for replicationState
  replication_metadata_encoding  text,
  workflow_last_write_version    bigint,
  workflow_state                 int,
  checksum                       blob,
  checksum_encoding              text,
  PRIMARY KEY  (shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id)
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
  tree_id               uuid,
  branch_id             uuid,
  branch                blob,
  branch_encoding       text,
  PRIMARY KEY ((tree_id), branch_id )
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  namespace_id             uuid,
  task_queue_name        text,
  task_queue_type        int, -- enum TaskQueueType {ActivityTask, WorkflowTask}
  type                  int, -- enum rowType {Task, TaskQueue}
  task_id               bigint,  -- unique identifier for tasks, monotonically increasing
  range_id              bigint, -- Used to ensure that only one process can write to the table
  task                  blob,
  task_encoding         text,
  task_queue             blob,
  task_queue_encoding    text,
  PRIMARY KEY ((namespace_id, task_queue_name, task_queue_type), type, task_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

-- this table is only used for storage of mapping of namespace uuid to namespace name
CREATE TABLE namespaces_by_id (
  id     uuid,
  name   text,
  PRIMARY KEY (id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'  };


CREATE TABLE namespaces (
  namespaces_partition          int,
  name                          text,
  id                            uuid,
  detail                        blob,
  detail_encoding               text,
  is_global_namespace           boolean,
  notification_version          bigint,
  PRIMARY KEY (namespaces_partition, name)
)  WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
   };

CREATE TABLE queue (
  queue_type      int,
  message_id      bigint,
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

CREATE TABLE cluster_metadata (
  metadata_partition      int,
  immutable_data          blob,
  immutable_data_encoding text,
  PRIMARY KEY  (metadata_partition)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };

CREATE TABLE cluster_membership
(
    membership_partition tinyint,
    host_id              uuid,
    rpc_address          inet,
    rpc_port             smallint,
    role                 tinyint,
    session_start        timestamp,
    last_heartbeat       timestamp,
    PRIMARY KEY (membership_partition, role, host_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };

CREATE INDEX cm_lastheartbeat_idx on cluster_membership (last_heartbeat);
CREATE INDEX cm_sessionstart_idx on cluster_membership (session_start);

