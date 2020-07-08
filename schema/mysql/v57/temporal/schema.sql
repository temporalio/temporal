CREATE TABLE namespaces(
  partition_id INT NOT NULL,
  id BINARY(16) NOT NULL,
  name VARCHAR(255) UNIQUE NOT NULL,
  notification_version BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  is_global TINYINT(1) NOT NULL,
  PRIMARY KEY(partition_id, id)
);

CREATE TABLE namespace_metadata (
  partition_id INT NOT NULL,
  notification_version BIGINT NOT NULL,
  PRIMARY KEY(partition_id)
);

INSERT INTO namespace_metadata (partition_id, notification_version) VALUES (54321, 1);

CREATE TABLE shards (
  shard_id INT NOT NULL,
  --
  range_id BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id)
);

CREATE TABLE transfer_tasks(
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  --
  next_event_id BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  state BLOB NOT NULL,
  state_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id BINARY(16) NOT NULL,
  create_request_id VARCHAR(64) NOT NULL,
  state INT NOT NULL,
  status INT NOT NULL,
  start_version BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id)
);

CREATE TABLE buffered_events (
  id BIGINT AUTO_INCREMENT NOT NULL,
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX buffered_events_by_events_ids ON buffered_events(shard_id, namespace_id, workflow_id, run_id);

CREATE TABLE tasks (
  namespace_id BINARY(16) NOT NULL,
  task_queue_name VARCHAR(255) NOT NULL,
  task_type TINYINT NOT NULL, -- {Activity, Decision}
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (namespace_id, task_queue_name, task_type, task_id)
);

CREATE TABLE task_queues (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  name VARCHAR(255) NOT NULL,
  task_type TINYINT NOT NULL, -- {Activity, Decision}
  --
  range_id BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, name, task_type)
);

CREATE TABLE replication_tasks (
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE replication_tasks_dlq (
  source_cluster_name VARCHAR(255) NOT NULL,
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (source_cluster_name, shard_id, task_id)
);

CREATE TABLE timer_tasks (
  shard_id INT NOT NULL,
  visibility_timestamp DATETIME(6) NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, visibility_timestamp, task_id)
);

CREATE TABLE activity_info_maps (
-- each row corresponds to one key of one map<string, ActivityInfo>
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  schedule_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, schedule_id)
);

CREATE TABLE timer_info_maps (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  timer_id VARCHAR(255) NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, timer_id)
);

CREATE TABLE child_execution_info_maps (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE request_cancel_info_maps (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signal_info_maps (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signals_requested_sets (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  signal_id VARCHAR(64) NOT NULL,
  --
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, signal_id)
);

-- history eventsV2: history_node stores history event data
CREATE TABLE history_node (
  shard_id       INT NOT NULL,
  tree_id        BINARY(16) NOT NULL,
  branch_id      BINARY(16) NOT NULL,
  node_id        BIGINT NOT NULL,
  txn_id         BIGINT NOT NULL,
  --
  data           MEDIUMBLOB NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, tree_id, branch_id, node_id, txn_id)
);

-- history eventsV2: history_tree stores branch metadata
CREATE TABLE history_tree (
  shard_id       INT NOT NULL,
  tree_id        BINARY(16) NOT NULL,
  branch_id      BINARY(16) NOT NULL,
  --
  data           BLOB NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, tree_id, branch_id)
);

CREATE TABLE queue (
  queue_type INT NOT NULL,
  message_id BIGINT NOT NULL,
  message_payload BLOB NOT NULL,
  PRIMARY KEY(queue_type, message_id)
);

CREATE TABLE queue_metadata (
  queue_type INT NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY(queue_type)
);

CREATE TABLE cluster_metadata (
  metadata_partition        INT NOT NULL,
  immutable_data            BLOB NOT NULL,
  immutable_data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);

CREATE TABLE cluster_membership
(
    host_id              BINARY(16) NOT NULL,
    rpc_address          VARCHAR(15) NOT NULL,
    rpc_port             SMALLINT NOT NULL,
    role                 TINYINT NOT NULL,
    session_start        TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    last_heartbeat       TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    record_expiry        TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    insertion_order      BIGINT NOT NULL AUTO_INCREMENT UNIQUE,
    INDEX (role, host_id),
    INDEX (role, last_heartbeat),
    INDEX (rpc_address, role),
    INDEX (last_heartbeat),
    INDEX (record_expiry),
    PRIMARY KEY (host_id)
);

