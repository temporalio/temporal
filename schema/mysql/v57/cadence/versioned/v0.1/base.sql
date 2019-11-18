CREATE TABLE domains(
  shard_id INT NOT NULL DEFAULT 54321,
  id BINARY(16) NOT NULL,
  name VARCHAR(255) UNIQUE NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  is_global TINYINT(1) NOT NULL,
  PRIMARY KEY(shard_id, id)
);

CREATE TABLE domain_metadata (
  notification_version BIGINT NOT NULL
);

INSERT INTO domain_metadata (notification_version) VALUES (1);

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
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  --
  next_event_id BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id BINARY(16) NOT NULL,
  create_request_id VARCHAR(64) NOT NULL,
  state INT NOT NULL,
  close_status INT NOT NULL,
  start_version BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id)
);

CREATE TABLE buffered_events (
  id BIGINT AUTO_INCREMENT NOT NULL,
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX buffered_events_by_events_ids ON buffered_events(shard_id, domain_id, workflow_id, run_id);

CREATE TABLE tasks (
  domain_id BINARY(16) NOT NULL,
  task_list_name VARCHAR(255) NOT NULL,
  task_type TINYINT NOT NULL, -- {Activity, Decision}
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (domain_id, task_list_name, task_type, task_id)
);

CREATE TABLE task_lists (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  name VARCHAR(255) NOT NULL,
  task_type TINYINT NOT NULL, -- {Activity, Decision}
  --
  range_id BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, name, task_type)
);

CREATE TABLE replication_tasks (
  shard_id INT NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
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

-- Deprecated in favor of history eventsV2
CREATE TABLE events (
  domain_id      BINARY(16) NOT NULL,
  workflow_id    VARCHAR(255) NOT NULL,
  run_id         BINARY(16) NOT NULL,
  first_event_id BIGINT NOT NULL,
  --
  batch_version  BIGINT,
  range_id       BIGINT NOT NULL,
  tx_id          BIGINT NOT NULL,
  data MEDIUMBLOB NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
	PRIMARY KEY (domain_id, workflow_id, run_id, first_event_id)
);

CREATE TABLE activity_info_maps (
-- each row corresponds to one key of one map<string, ActivityInfo>
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  schedule_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  last_heartbeat_details BLOB,
  last_heartbeat_updated_time DATETIME(6) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, schedule_id)
);

CREATE TABLE timer_info_maps (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  timer_id VARCHAR(255) NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, timer_id)
);

CREATE TABLE child_execution_info_maps (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE request_cancel_info_maps (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signal_info_maps (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE buffered_replication_task_maps (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  first_event_id BIGINT NOT NULL,
--
  version BIGINT NOT NULL,
  next_event_id BIGINT NOT NULL,
  history MEDIUMBLOB,
  history_encoding VARCHAR(16) NOT NULL,
  new_run_history BLOB,
  new_run_history_encoding VARCHAR(16) NOT NULL DEFAULT 'json',
  event_store_version          INT NOT NULL, -- indiciates which version of event store to query
  new_run_event_store_version  INT NOT NULL, -- indiciates which version of event store to query for new run(continueAsNew)
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, first_event_id)
);

CREATE TABLE signals_requested_sets (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  signal_id VARCHAR(64) NOT NULL,
  --
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, signal_id)
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