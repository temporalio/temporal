CREATE TABLE cluster_metadata (
  metadata_partition        int,
  immutable_data            blob,
  immutable_data_encoding   text,
  PRIMARY KEY  (metadata_partition)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };