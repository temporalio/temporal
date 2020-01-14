CREATE TABLE cluster_metadata (
  metadata_partition INT NOT NULL,
  immutable_metadata_payload BLOB NOT NULL,
  immutable_metadata_payload_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);