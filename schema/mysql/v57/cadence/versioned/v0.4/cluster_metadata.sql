CREATE TABLE cluster_metadata (
  metadata_partition        INT NOT NULL,
  immutable_data            BLOB NOT NULL,
  immutable_data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);