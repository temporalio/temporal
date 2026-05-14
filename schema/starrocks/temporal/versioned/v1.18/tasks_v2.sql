-- Stores activity or workflow tasks
-- Used for fairness scheduling. (pass, task_id) are monotonically increasing.
CREATE TABLE tasks_v2 (
  range_hash INT UNSIGNED NOT NULL,
  task_queue_id VARBINARY(272) NOT NULL,
  pass BIGINT NOT NULL, -- pass for tasks (see stride scheduling algorithm for fairness)
  task_id BIGINT NOT NULL,
  --
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id, pass, task_id)
);

-- Stores ephemeral task queue information such as ack levels and expiry times
CREATE TABLE task_queues_v2 (
  range_hash INT UNSIGNED NOT NULL,
  task_queue_id VARBINARY(272) NOT NULL,
  --
  range_id BIGINT NOT NULL,
  data MEDIUMBLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id)
);
