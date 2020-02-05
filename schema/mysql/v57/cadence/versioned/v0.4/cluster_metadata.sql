CREATE TABLE cluster_metadata (
  metadata_partition        INT NOT NULL,
  immutable_data            BLOB NOT NULL,
  immutable_data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);

CREATE TABLE cluster_membership
(
    host_id              BINARY(16) NOT NULL,
    rpc_address          VARCHAR(15) NOT NULL,
    session_start        TIMESTAMP NOT NULL,
    last_heartbeat       TIMESTAMP NOT NULL,
    record_expiry        TIMESTAMP NOT NULL,
    PRIMARY KEY (host_id)
);