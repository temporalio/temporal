CREATE TABLE current_chasm_executions(
  shard_id INTEGER NOT NULL,
  namespace_id BYTEA NOT NULL,
  business_id VARCHAR(255) NOT NULL,
  archetype_id BIGINT NOT NULL,
  --
  run_id BYTEA NOT NULL,
  create_request_id VARCHAR(255) NOT NULL,
  state INTEGER NOT NULL,
  status INTEGER NOT NULL,
  start_version BIGINT NOT NULL DEFAULT 0,
  start_time TIMESTAMP NULL,
  last_write_version BIGINT NOT NULL,
  -- `data` contains the ExecutionState (same as in `executions.state` above)
  data BYTEA NULL,
  data_encoding VARCHAR(16) NOT NULL DEFAULT '',
  PRIMARY KEY (shard_id, namespace_id, business_id, archetype_id)
);
