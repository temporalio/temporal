CREATE TABLE queue_metadata_v2 (
  queue_type        int,
  queue_name        varchar,
  metadata_payload  blob,
  metadata_encoding text,
  version           bigint,
PRIMARY KEY ((queue_type, queue_name))
)  WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
   };

CREATE TABLE queue_v2 (
  queue_type        int,
  queue_name        varchar,
  queue_partition   int, -- not used but in case we need in the future
  message_id        bigint,
  message_payload   blob,
  message_encoding  text,
  PRIMARY KEY  ((queue_type, queue_name, queue_partition), message_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };