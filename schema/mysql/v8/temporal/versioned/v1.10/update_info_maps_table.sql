CREATE TABLE update_info_maps (
-- each row corresponds to one key of one map<string, UpdateInfo>
  shard_id INT NOT NULL,
  namespace_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  update_id VARCHAR(255) NOT NULL,
--
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16),
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, update_id)
);
