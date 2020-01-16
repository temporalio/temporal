CREATE TABLE cluster_metadata (
  metadata_partition        INTEGER NOT NULL,
  immutable_data            BYTEA NOT NULL,
  immutable_data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);