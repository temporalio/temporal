CREATE TABLE cluster_metadata (
  metadata_partition                    INTEGER NOT NULL,
  immutable_metadata_payload            BYTEA NOT NULL,
  immutable_metadata_payload_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);