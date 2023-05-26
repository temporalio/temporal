-- Stores task queue information such as user provided versioning data
CREATE TABLE task_queue_user_data (
  namespace_id    BYTEA NOT NULL,
  task_queue_name VARCHAR(255) NOT NULL,
  data            BYTEA NOT NULL,       -- temporal.server.api.persistence.v1.TaskQueueUserData
  data_encoding   VARCHAR(16) NOT NULL, -- Encoding type used for serialization, in practice this should always be proto3
  version         BIGINT NOT NULL,      -- Version of this row, used for optimistic concurrency
  PRIMARY KEY (namespace_id, task_queue_name)
);

-- Stores a mapping between build ids and task queues
CREATE TABLE build_id_to_task_queue (
  namespace_id    BYTEA NOT NULL,
  build_id        VARCHAR(255) NOT NULL,
  task_queue_name VARCHAR(255) NOT NULL,
  PRIMARY KEY (namespace_id, build_id, task_queue_name)
);
