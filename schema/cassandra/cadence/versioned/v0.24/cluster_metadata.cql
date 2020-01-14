CREATE TABLE cluster_metadata (
  metadata_partition                    int,
  immutable_metadata_payload            blob,
  immutable_metadata_payload_encoding   text,
  PRIMARY KEY  (metadata_partition)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };