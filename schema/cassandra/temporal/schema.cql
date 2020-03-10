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
  bad_binaries_encoding text,
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

CREATE TABLE executions (
  shard_id                       int,
  type                           int, -- enum RowType { Shard, Execution, TransferTask, TimerTask, ReplicationTask}
  domain_id                      uuid,
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
  replication_state              frozen<replication_state>, -- Replication information part of mutable state
  buffered_replication_tasks_map map<bigint, frozen<buffered_replication_task_info>>,
  workflow_last_write_version    bigint,
  workflow_state                 int,
  version_histories              blob, -- the metadata of history branching
  version_histories_encoding     text,
  checksum                       blob,
  checksum_encoding              text,
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
  domain_id          uuid,
  task_list_name     text,
  task_list_type     int, -- enum TaskListType {ActivityTask, DecisionTask}
  type               int, -- enum rowType {Task, TaskList}
  task_id            bigint,  -- unique identifier for tasks, monotonically increasing
  range_id           bigint, -- Used to ensure that only one process can write to the table
  task               blob,
  task_encoding      text,
  task_list          blob,
  task_list_encoding text,
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

-- Create metadata for system domain
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