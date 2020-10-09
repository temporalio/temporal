DROP TABLE cluster_metadata;

CREATE TABLE cluster_metadata (
  metadata_partition        INTEGER NOT NULL,
  data                      BYTEA NOT NULL,
  data_encoding             VARCHAR(16) NOT NULL,
  version                   BIGINT NOT NULL,
  PRIMARY KEY(metadata_partition)
);