CREATE TYPE branch_range (
  branch_id   uuid,
  end_node_id bigint, -- exclusive node_id to represent the stopping point for this range
);

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
  PRIMARY KEY ((tree_id), branch_id )
) WITH COMPACTION = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

--- using eventsV2
ALTER TYPE workflow_execution ADD  event_store_version           int;
ALTER TYPE workflow_execution ADD  branch_token                  blob;
ALTER TYPE replication_task ADD  event_store_version             int;
ALTER TYPE replication_task ADD  branch_token                    blob;
ALTER TYPE replication_task ADD  new_run_event_store_version     int;
ALTER TYPE replication_task ADD  new_run_branch_token            blob;
ALTER TYPE buffered_replication_task_info ADD  event_store_version     int;
ALTER TYPE buffered_replication_task_info ADD  new_run_event_store_version     int;