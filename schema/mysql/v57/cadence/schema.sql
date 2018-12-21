CREATE TABLE domains(
/* domain */
  id CHAR(36) PRIMARY KEY NOT NULL,
  name VARCHAR(255) UNIQUE NOT NULL,
  status INT NOT NULL,
  description VARCHAR(255) NOT NULL,
  owner_email VARCHAR(255) NOT NULL,
  data BLOB,
/* end domain */
  retention INT NOT NULL,
  emit_metric TINYINT(1) NOT NULL,
  archival_bucket VARCHAR(255) NOT NULL,
  archival_status TINYINT NOT NULL,
/* end domain_config */
  config_version BIGINT NOT NULL,
  notification_version BIGINT NOT NULL,
  failover_notification_version BIGINT NOT NULL,
  failover_version BIGINT NOT NULL,
  is_global_domain TINYINT(1) NOT NULL,
/* domain_replication_config */
  active_cluster_name VARCHAR(255) NOT NULL,
  clusters BLOB
/* end domain_replication_config */
) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

CREATE TABLE domain_metadata (
  notification_version BIGINT NOT NULL
);

INSERT INTO domain_metadata (notification_version) VALUES (0);

CREATE TABLE shards (
	shard_id INT NOT NULL,
	owner VARCHAR(255) NOT NULL,
	range_id BIGINT NOT NULL,
	stolen_since_renew INT NOT NULL,
	updated_at TIMESTAMP(3) NOT NULL,
	replication_ack_level BIGINT NOT NULL,
	transfer_ack_level BIGINT NOT NULL,
	timer_ack_level TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
	cluster_transfer_ack_level BLOB NOT NULL,
	cluster_timer_ack_level BLOB NOT NULL,
	domain_notification_version BIGINT NOT NULL,
	PRIMARY KEY (shard_id)
);

CREATE TABLE transfer_tasks(
	shard_id INT NOT NULL,
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	task_id BIGINT NOT NULL,
	task_type TINYINT NOT NULL,
	target_domain_id CHAR(64) NOT NULL,
	target_workflow_id CHAR(64) NOT NULL,
	target_run_id CHAR(64) NOT NULL,
	target_child_workflow_only TINYINT(1) NOT NULL,
	task_list VARCHAR(255) NOT NULL,
	schedule_id BIGINT NOT NULL,
	version BIGINT NOT NULL,
	visibility_timestamp TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
	PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE executions(
  shard_id INT NOT NULL,
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	--
	parent_domain_id CHAR(64), -- 1.
	parent_workflow_id VARCHAR(255), -- 2.
	parent_run_id CHAR(64), -- 3.
	initiated_id BIGINT, -- 4. these (parent-related fields) are nullable as their default values are not checked by tests
	completion_event BLOB, -- 5.
	completion_event_encoding VARCHAR(64),
	task_list VARCHAR(255) NOT NULL,
	workflow_type_name VARCHAR(255) NOT NULL,
	workflow_timeout_seconds INT UNSIGNED NOT NULL,
	decision_task_timeout_minutes INT UNSIGNED NOT NULL,
	execution_context BLOB, -- nullable because test passes in a null blob.
	state INT NOT NULL,
	close_status INT NOT NULL,
	-- replication_state members
  start_version BIGINT,
  current_version BIGINT,
  last_write_version BIGINT,
  last_write_event_id BIGINT,
  last_replication_info BLOB,
  -- replication_state members end
	last_first_event_id BIGINT NOT NULL,
	next_event_id BIGINT NOT NULL, -- very important! for conditional updates of all the dependent tables.
	last_processed_event BIGINT NOT NULL,
	start_time TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
	last_updated_time TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
	create_request_id CHAR(64) NOT NULL,
	decision_version BIGINT NOT NULL, -- 1.
	decision_schedule_id BIGINT NOT NULL, -- 2.
	decision_started_id BIGINT NOT NULL, -- 3. cannot be nullable as common.EmptyEventID is checked
	decision_request_id VARCHAR(255), -- not checked
	decision_timeout INT NOT NULL, -- 4.
	decision_attempt BIGINT NOT NULL, -- 5.
	decision_timestamp BIGINT NOT NULL, -- 6.
	cancel_requested TINYINT(1), -- a.
	cancel_request_id VARCHAR(255), -- b. default values not checked
	sticky_task_list VARCHAR(255) NOT NULL, -- 1. defualt value is checked
	sticky_schedule_to_start_timeout INT NOT NULL, -- 2.
	client_library_version VARCHAR(255) NOT NULL, -- 3.
	client_feature_version VARCHAR(255) NOT NULL, -- 4.
	client_impl VARCHAR(255) NOT NULL, -- 5.
	signal_count INT NOT NULL,
	cron_schedule VARCHAR(255),
	-- TODO: fix sql to support workflow retry. https://github.com/uber/cadence/issues/1339
	PRIMARY KEY (shard_id, domain_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INT NOT NULL,
  domain_id CHAR(64) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id CHAR(64) NOT NULL,
  create_request_id CHAR(64) NOT NULL,
	state INT NOT NULL,
	close_status INT NOT NULL,
  start_version BIGINT,
	last_write_version BIGINT,
  PRIMARY KEY (shard_id, domain_id, workflow_id)
);

CREATE TABLE buffered_events (
  id BIGINT AUTO_INCREMENT NOT NULL,
  shard_id INT NOT NULL,
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	--
	data MEDIUMBLOB NOT NULL,
	data_encoding VARCHAR(64) NOT NULL,
	PRIMARY KEY (id)
);

CREATE INDEX buffered_events_by_events_ids ON buffered_events(shard_id, domain_id, workflow_id, run_id);

CREATE TABLE tasks (
  shard_id INT NOT NULL DEFAULT 0,
  domain_id CHAR(64) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id CHAR(64) NOT NULL,
  schedule_id BIGINT NOT NULL,
  task_list_name VARCHAR(255) NOT NULL,
  task_list_type TINYINT NOT NULL,
  task_id BIGINT NOT NULL,
  expiry_ts TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, task_list_name, task_list_type, task_id)
);

CREATE TABLE task_lists (
	domain_id CHAR(64) NOT NULL,
	range_id BIGINT NOT NULL,
	name VARCHAR(255) NOT NULL,
	task_type TINYINT NOT NULL, -- {Activity, Decision}
	ack_level BIGINT NOT NULL DEFAULT 0,
	kind TINYINT NOT NULL, -- {Normal, Sticky}
	expiry_ts TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
	PRIMARY KEY (domain_id, name, task_type)
);

CREATE TABLE replication_tasks (
  shard_id INT NOT NULL,
	task_id BIGINT NOT NULL,
	--
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	task_type TINYINT NOT NULL,
	first_event_id BIGINT NOT NULL,
	next_event_id BIGINT NOT NULL,
	version BIGINT NOT NULL,
  last_replication_info BLOB NOT NULL,
PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE timer_tasks (
	shard_id INT NOT NULL,
	visibility_timestamp TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
	task_id BIGINT NOT NULL,
	--
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	task_type TINYINT NOT NULL,
	timeout_type TINYINT NOT NULL,
	event_id BIGINT NOT NULL,
	schedule_attempt BIGINT NOT NULL,
	version BIGINT NOT NULL,
	PRIMARY KEY (shard_id, visibility_timestamp, task_id)
);

CREATE TABLE events (
	domain_id      VARCHAR(64) NOT NULL,
	workflow_id    VARCHAR(255) NOT NULL,
	run_id         VARCHAR(64) NOT NULL,
	first_event_id BIGINT NOT NULL,
	batch_version  BIGINT,
	range_id       INT NOT NULL,
	tx_id          INT NOT NULL,
	data BLOB      NOT NULL,
	data_encoding  VARCHAR(64) NOT NULL,
	PRIMARY KEY (domain_id, workflow_id, run_id, first_event_id)
);

CREATE TABLE activity_info_maps (
-- each row corresponds to one key of one map<string, ActivityInfo>
	shard_id INT NOT NULL,
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
  run_id CHAR(64) NOT NULL,
	schedule_id BIGINT NOT NULL, -- the key.
-- fields of activity_info type follow
version                     BIGINT NOT NULL,
scheduled_event             BLOB,
scheduled_event_encoding    VARCHAR(64),
scheduled_time              TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
started_id                  BIGINT NOT NULL,
started_event               BLOB,
started_event_encoding      VARCHAR(64),
started_time                TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
activity_id                 VARCHAR(255) NOT NULL,
request_id                  VARCHAR(255) NOT NULL,
details                     BLOB,
schedule_to_start_timeout   INT NOT NULL,
schedule_to_close_timeout   INT NOT NULL,
start_to_close_timeout      INT NOT NULL,
heartbeat_timeout           INT NOT NULL,
cancel_requested            TINYINT(1),
cancel_request_id           BIGINT NOT NULL,
last_heartbeat_updated_time TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
timer_task_status           INT NOT NULL,
attempt                     INT NOT NULL,
task_list                   VARCHAR(255) NOT NULL,
started_identity            VARCHAR(255) NOT NULL,
has_retry_policy            BOOLEAN NOT NULL,
init_interval               INT NOT NULL,
backoff_coefficient         DOUBLE,
max_interval                INT NOT NULL,
expiration_time             TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
max_attempts                INT NOT NULL,
non_retriable_errors        BLOB, -- this was a list<text>. The use pattern is to replace, no modifications.
	PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, schedule_id)
);

CREATE TABLE timer_info_maps (
shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
timer_id VARCHAR(255) NOT NULL, -- what string type should this be?
--
  version BIGINT NOT NULL,
  started_id BIGINT NOT NULL,
  expiry_time TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:01.000',
  task_id BIGINT NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, timer_id)
);

CREATE TABLE child_execution_info_maps (
  shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
initiated_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
initiated_event BLOB,
initiated_event_encoding  VARCHAR(64),
started_id BIGINT NOT NULL,
started_event BLOB,
started_event_encoding  VARCHAR(64),
create_request_id CHAR(64),
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE request_cancel_info_maps (
shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
initiated_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
cancel_request_id CHAR(64) NOT NULL, -- a uuid
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);


CREATE TABLE signal_info_maps (
shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
initiated_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
signal_request_id CHAR(64) NOT NULL, -- uuid
signal_name VARCHAR(255) NOT NULL,
input BLOB,
control BLOB,
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE buffered_replication_task_maps (
 shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
first_event_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
next_event_id BIGINT NOT NULL,
history BLOB,
history_encoding VARCHAR(64) NOT NULL,
new_run_history BLOB,
new_run_history_encoding VARCHAR(64) NOT NULL DEFAULT 'json',
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, first_event_id)
);

CREATE TABLE signals_requested_sets (
	shard_id INT NOT NULL,
	domain_id VARCHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id VARCHAR(64) NOT NULL,
	signal_id VARCHAR(64) NOT NULL,
	--
	PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, signal_id)
);