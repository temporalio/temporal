CREATE TABLE cluster_metadata_info (
  metadata_partition      int,
  cluster_name            text,
  data                    blob,
  data_encoding           text,
  version                 bigint,
  PRIMARY KEY  (metadata_partition, cluster_name)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };