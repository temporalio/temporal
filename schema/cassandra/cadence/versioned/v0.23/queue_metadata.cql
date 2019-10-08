CREATE TABLE queue_metadata (
  queue_type        int,
  cluster_ack_level map<text, bigint>,
  version           bigint,
PRIMARY KEY (queue_type)
)  WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
   };
