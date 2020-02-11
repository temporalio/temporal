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
    rpc_port             SMALLINT NOT NULL,
    role                 TINYINT NOT NULL,
    session_start        TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    last_heartbeat       TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    record_expiry        TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    insertion_order      BIGINT NOT NULL AUTO_INCREMENT UNIQUE,
    INDEX (role, host_id),
    INDEX (role, last_heartbeat),
    INDEX (rpc_address, role),
    INDEX (last_heartbeat),
    INDEX (record_expiry),
    PRIMARY KEY (host_id)
);