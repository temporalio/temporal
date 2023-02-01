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