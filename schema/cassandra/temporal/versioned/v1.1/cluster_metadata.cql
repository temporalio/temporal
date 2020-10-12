DROP TABLE cluster_metadata;

CREATE TABLE cluster_metadata (
  metadata_partition      int,
  data                    blob,
  data_encoding           text,
  immutable_data          blob,
  immutable_data_encoding text,
  version                 bigint,
  PRIMARY KEY  (metadata_partition)
) WITH COMPACTION = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};