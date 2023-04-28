CREATE TABLE update_record_maps (
-- each row corresponds to one key of one map<string, UpdateRecord>
  shard_id INTEGER NOT NULL,
  namespace_id BYTEA NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BYTEA NOT NULL,
  update_id VARCHAR(255) NOT NULL,
--
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, update_id)
);
