CREATE TABLE current_chasm_executions(
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  business_id VARCHAR(255) NOT NULL,
  archetype_id INT UNSIGNED NOT NULL,
  --
  run_id BINARY(16) NOT NULL,
  create_request_id VARCHAR(255) NOT NULL,
  state INT NOT NULL,
  status INT NOT NULL,
  start_version BIGINT NOT NULL DEFAULT 0,
  start_time DATETIME(6),
  last_write_version BIGINT NOT NULL,
  -- `data` contains the ExecutionState (same as in `executions.state` above)
  data MEDIUMBLOB NULL,
  data_encoding VARCHAR(16) NOT NULL DEFAULT '',
  PRIMARY KEY (shard_id, namespace_id, business_id, archetype_id)
);
