-- Stores task queue information such as user provided versioning data
-- OR
-- Used as a mapping from build ID to task queue
CREATE TABLE task_queue_user_data (
  namespace_id     uuid,
  task_queue_name  text,
  build_id         text,   -- If this row is used as a mapping of build ID to task queue, this will not be empty
  data             blob,   -- temporal.server.api.persistence.v1.TaskQueueUserData
  data_encoding    text,   -- Encoding type used for serialization, in practice this should always be proto3
  version          bigint, -- Version of this row, used for optimistic concurrency
  -- task_queue_name is not a part of the parititioning key to allow cheaply iterating all task queues in a single
  -- namespace. Access to this table should be infrequent enough that a single partition per namespace can be used.
  -- Note that this imposes a limit on total task queue user data within one namespace (see the relevant single
  -- partition Cassandra limits).
  PRIMARY KEY ((namespace_id), build_id, task_queue_name)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };