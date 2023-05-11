-- Stores task queue information such as user provided versioning data
CREATE TABLE task_queue_user_data (
  namespace_id    uuid,
  task_queue_name text,
  data            blob,      -- temporal.server.api.persistence.v1.TaskQueueUserData
  data_encoding   text,      -- Encoding type used for serialization, in practice this should always be proto3
  build_ids       set<text>, -- All active build ids in all version sets on this task queue (used in an index below)
  version         bigint,    -- Version of this row, used for optimistic concurrency
  -- task_queue_name is not a part of the parititioning key to allow cheaply iterating all task queues in a single
  -- namespace. Access to this table should be infrequent enough that a single partition per namespace can be used.
  -- Note that this imposes a limit on total task queue user data within one namespace (see the relevant single
  -- partition Cassandra limits).
  PRIMARY KEY ((namespace_id), task_queue_name)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE INDEX task_user_data_build_ids_idx on task_queue_user_data (build_ids);
