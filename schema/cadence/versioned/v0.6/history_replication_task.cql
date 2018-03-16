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

-- Replication information part of mutable state
ALTER TABLE executions ADD replication_state frozen<replication_state>;

ALTER TYPE transfer_task ADD first_event_id bigint;
ALTER TYPE transfer_task ADD next_event_id bigint;
ALTER TYPE transfer_task ADD version bigint;
ALTER TYPE transfer_task ADD last_replication_info map<text, frozen<replication_info>>;