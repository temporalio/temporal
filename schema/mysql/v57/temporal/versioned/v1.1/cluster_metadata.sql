DROP TABLE cluster_metadata;

CREATE TABLE cluster_metadata (
  metadata_partition        INT NOT NULL,
  data                      BLOB NOT NULL,
  data_encoding             VARCHAR(16) NOT NULL,
  immutable_data            BLOB NOT NULL,
  immutable_data_encoding   VARCHAR(16) NOT NULL,
  version                   BIGINT NOT NULL,
  PRIMARY KEY(metadata_partition)
);