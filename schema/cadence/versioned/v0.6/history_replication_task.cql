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

-- Transfer task created for processing of replication event
CREATE TYPE replication_task (
  domain_id                  uuid,   -- The domain ID that this transfer task belongs to
  workflow_id                text,   -- The workflow ID that this transfer task belongs to
  run_id                     uuid,   -- The run ID that this transfer task belongs to
  task_id                    bigint,
  type                       int,    -- enum TaskType {History, Heartbeat}
  first_event_id             bigint,  -- Used by ReplicationTask to set the first event ID of the applied transaction
  next_event_id              bigint,  -- Used by ReplicationTask to set the next event ID of the applied transaction
  version                    bigint,  -- Used by ReplicationTask to set the failover version of the applied transaction
  last_replication_info      map<text, frozen<replication_info>>, -- Used by replication task to snapshot replication information when the transaction was applied
);

-- Replication information part of mutable state
ALTER TABLE executions ADD replication_state frozen<replication_state>;

-- Replication transfer task kept as part of executions table
ALTER TABLE executions ADD replication frozen<replication_task>;

-- Ack level checkpoint for replication task processor
ALTER TYPE shard ADD replication_ack_level bigint;