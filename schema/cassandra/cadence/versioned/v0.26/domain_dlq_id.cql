DROP TABLE queue;
CREATE TABLE queue (
  queue_type      int,
  message_id      bigint,
  message_payload blob,
  PRIMARY KEY  (queue_type, message_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };