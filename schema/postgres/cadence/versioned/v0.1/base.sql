CREATE TABLE domains(
  shard_id INTEGER NOT NULL DEFAULT 54321,
  id BYTEA NOT NULL,
  name VARCHAR(255) UNIQUE NOT NULL,
  --
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  is_global BOOLEAN NOT NULL,
  PRIMARY KEY(shard_id, id)
);

CREATE TABLE domain_metadata (
  notification_version BIGINT NOT NULL
);

INSERT INTO domain_metadata (notification_version) VALUES (1);

CREATE TABLE shards (
  shard_id INTEGER NOT NULL,
  --
  range_id BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id)
);

CREATE TABLE transfer_tasks(
  shard_id INTEGER NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE executions(
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  --
  next_event_id BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id BYTEA NOT NULL,
  create_request_id VARCHAR(64) NOT NULL,
  state INTEGER NOT NULL,
  close_status INTEGER NOT NULL,
  start_version BIGINT NOT NULL,
  last_write_version BIGINT NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id)
);

CREATE TABLE buffered_events (
  id BIGSERIAL NOT NULL,
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  --
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX buffered_events_by_events_ids ON buffered_events(shard_id, domain_id, workflow_id, run_id);

CREATE TABLE tasks (
  domain_id BYTEA NOT NULL,
  task_list_name VARCHAR(255) NOT NULL,
  task_type SMALLINT NOT NULL, -- {Activity, Decision}
  task_id BIGINT NOT NULL,
  --
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (domain_id, task_list_name, task_type, task_id)
);

CREATE TABLE task_lists (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  name VARCHAR(255) NOT NULL,
  task_type SMALLINT NOT NULL, -- {Activity, Decision}
  --
  range_id BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, name, task_type)
);

CREATE TABLE replication_tasks (
  shard_id INTEGER NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE timer_tasks (
  shard_id INTEGER NOT NULL,
  visibility_timestamp TIMESTAMP NOT NULL,
  task_id BIGINT NOT NULL,
  --
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, visibility_timestamp, task_id)
);

CREATE TABLE activity_info_maps (
-- each row corresponds to one key of one map<string, ActivityInfo>
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  schedule_id BIGINT NOT NULL,
--
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16),
  last_heartbeat_details BYTEA,
  last_heartbeat_updated_time TIMESTAMP NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, schedule_id)
);

CREATE TABLE timer_info_maps (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  timer_id VARCHAR(255) NOT NULL,
--
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, timer_id)
);

CREATE TABLE child_execution_info_maps (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE request_cancel_info_maps (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signal_info_maps (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  initiated_id BIGINT NOT NULL,
--
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE buffered_replication_task_maps (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  first_event_id BIGINT NOT NULL,
--
  version BIGINT NOT NULL,
  next_event_id BIGINT NOT NULL,
  history BYTEA,
  history_encoding VARCHAR(16) NOT NULL,
  new_run_history BYTEA,
  new_run_history_encoding VARCHAR(16) NOT NULL DEFAULT 'json',
  event_store_version          INTEGER NOT NULL, -- indiciates which version of event store to query
  new_run_event_store_version  INTEGER NOT NULL, -- indiciates which version of event store to query for new run(continueAsNew)
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, first_event_id)
);

CREATE TABLE signals_requested_sets (
  shard_id INTEGER NOT NULL,
  domain_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  signal_id VARCHAR(64) NOT NULL,
  --
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, signal_id)
);

-- history eventsV2: history_node stores history event data
CREATE TABLE history_node (
  shard_id       INTEGER NOT NULL,
  tree_id        BYTEA NOT NULL,
  branch_id      BYTEA NOT NULL,
  node_id        BIGINT NOT NULL,
  txn_id         BIGINT NOT NULL,
  --
  data           BYTEA NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, tree_id, branch_id, node_id, txn_id)
);

-- history eventsV2: history_tree stores branch metadata
CREATE TABLE history_tree (
  shard_id       INTEGER NOT NULL,
  tree_id        BYTEA NOT NULL,
  branch_id      BYTEA NOT NULL,
  --
  data           BYTEA NOT NULL,
  data_encoding  VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, tree_id, branch_id)
);
