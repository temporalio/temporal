CREATE TABLE namespaces(
  partition_id INT NOT NULL,
  id BINARY(16) NOT NULL,
  name VARCHAR(255) UNIQUE NOT NULL,
  notification_version BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
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
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id)
);

CREATE TABLE executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  --
  next_event_id BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  state MEDIUMBLOB NOT NULL,
  state_encoding VARCHAR(16) NOT NULL,
  db_record_version BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id BINARY(16) NOT NULL,
  create_request_id VARCHAR(255) NOT NULL,
  state INT NOT NULL,
  status INT NOT NULL,
  start_version BIGINT NOT NULL DEFAULT 0,
  last_write_version BIGINT NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id)
);

CREATE TABLE buffered_events (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  id BIGINT AUTO_INCREMENT NOT NULL UNIQUE,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, id)
);

CREATE TABLE tasks (
  range_hash INT UNSIGNED NOT NULL,
  task_queue_id VARBINARY(272) NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id, task_id)
);

-- Stores ephemeral task queue information such as ack levels and expiry times
CREATE TABLE task_queues (
  range_hash INT UNSIGNED NOT NULL,
  task_queue_id VARBINARY(272) NOT NULL,
  --
  range_id BIGINT NOT NULL,
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id)
);

-- Stores task queue information such as user provided versioning data
CREATE TABLE task_queue_user_data (
  namespace_id    BINARY(16) NOT NULL,
  task_queue_name VARCHAR(255) NOT NULL,
  data            MEDIUMBLOB NOT NULL,  -- temporal.server.api.persistence.v1.TaskQueueUserData
  data_encoding   VARCHAR(16) NOT NULL, -- Encoding type used for serialization, in practice this should always be proto3
  version         BIGINT NOT NULL,      -- Version of this row, used for optimistic concurrency
  PRIMARY KEY (namespace_id, task_queue_name)
);

-- Stores a mapping between build ids and task queues
CREATE TABLE build_id_to_task_queue (
  namespace_id    BINARY(16) NOT NULL,
  build_id        VARCHAR(255) NOT NULL,
  task_queue_name VARCHAR(255) NOT NULL,
  PRIMARY KEY (namespace_id, build_id, task_queue_name)
);

CREATE TABLE history_immediate_tasks(
  shard_id INT NOT NULL,
  category_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, category_id, task_id)
);

CREATE TABLE history_scheduled_tasks (
  shard_id INT NOT NULL,
  category_id INT NOT NULL,
  visibility_timestamp DATETIME(6) NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, category_id, visibility_timestamp, task_id)
);

CREATE TABLE transfer_tasks(
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE timer_tasks (
  shard_id INT NOT NULL,
  visibility_timestamp DATETIME(6) NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, visibility_timestamp, task_id)
);

CREATE TABLE replication_tasks (
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE replication_tasks_dlq (
  source_cluster_name VARCHAR(255) NOT NULL,
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (source_cluster_name, shard_id, task_id)
);

CREATE TABLE visibility_tasks(
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE activity_info_maps (
-- each row corresponds to one key of one map<string, ActivityInfo>
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  schedule_id BIGINT NOT NULL,
--
  data MEDIUMBLOB NOT NULL,
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
  data MEDIUMBLOB NOT NULL,
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
  data MEDIUMBLOB NOT NULL,
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
  data MEDIUMBLOB NOT NULL,
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
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signals_requested_sets (
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  signal_id VARCHAR(255) NOT NULL,
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
  prev_txn_id    BIGINT NOT NULL DEFAULT 0,
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
  data           MEDIUMBLOB NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, tree_id, branch_id)
);

CREATE TABLE queue (
  queue_type        INT NOT NULL,
  message_id        BIGINT NOT NULL,
  message_payload   MEDIUMBLOB NOT NULL,
  message_encoding  VARCHAR(16) NOT NULL,
  PRIMARY KEY(queue_type, message_id)
);

CREATE TABLE queue_metadata (
  queue_type     INT NOT NULL,
  data           MEDIUMBLOB NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
  version        BIGINT NOT NULL,
  PRIMARY KEY(queue_type)
);

CREATE TABLE cluster_metadata_info (
  metadata_partition        INT NOT NULL,
  cluster_name              VARCHAR(255) NOT NULL,
  data                      MEDIUMBLOB NOT NULL,
  data_encoding             VARCHAR(16) NOT NULL,
  version                   BIGINT NOT NULL,
  PRIMARY KEY(metadata_partition, cluster_name)
);

CREATE TABLE cluster_membership (
    membership_partition INT NOT NULL,
    host_id              BINARY(16) NOT NULL,
    rpc_address          VARCHAR(128) NOT NULL,
    rpc_port             SMALLINT NOT NULL,
    role                 TINYINT NOT NULL,
    session_start        TIMESTAMP DEFAULT '1970-01-02 00:00:01',
    last_heartbeat       TIMESTAMP DEFAULT '1970-01-02 00:00:01',
    record_expiry        TIMESTAMP DEFAULT '1970-01-02 00:00:01',
    INDEX (role, host_id),
    INDEX (role, last_heartbeat),
    INDEX (rpc_address, role),
    INDEX (last_heartbeat),
    INDEX (record_expiry),
    PRIMARY KEY (membership_partition, host_id)
);

CREATE TABLE queues (
    queue_type INT NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    metadata_payload MEDIUMBLOB NOT NULL,
    metadata_encoding VARCHAR(16) NOT NULL,
    PRIMARY KEY (queue_type, queue_name)
);

CREATE TABLE queue_messages (
    queue_type INT NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    queue_partition BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    message_payload MEDIUMBLOB NOT NULL,
    message_encoding VARCHAR(16) NOT NULL,
    PRIMARY KEY (
        queue_type,
        queue_name,
        queue_partition,
        message_id
    )
);

-- Stores information about Nexus endpoints
CREATE TABLE nexus_endpoints (
    id            BINARY(16) NOT NULL,
    data          MEDIUMBLOB NOT NULL,  -- temporal.server.api.persistence.v1.NexusEndpoint
    data_encoding VARCHAR(16) NOT NULL, -- Encoding type used for serialization, in practice this should always be proto3
    version       BIGINT NOT NULL,      -- Version of this row, used for optimistic concurrency
    PRIMARY KEY (id)
);

-- Stores the version of Nexus endpoints table as a whole
CREATE TABLE nexus_endpoints_partition_status (
    id      INT NOT NULL DEFAULT 0 CHECK (id = 0),  -- Restrict the table to a single row since it will only be used for endpoints
    version BIGINT NOT NULL,                        -- Version of the nexus_endpoints table
    PRIMARY KEY (id)
);
