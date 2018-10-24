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
  row_type          bigint, -- 0: root node row, 1: branch row
  branch_id         uuid, -- for branch row
  ancestors         list<frozen<branch_range>>, -- Only for branch row.
  deleted           boolean, -- Only for branch row to mark a branch as deleted
  txn_id            bigint, -- Only for root node: a tree operation transactionID
  PRIMARY KEY ((tree_id), row_type, branch_id )
) WITH COMPACTION = {
'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};