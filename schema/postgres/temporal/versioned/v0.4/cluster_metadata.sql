CREATE TABLE cluster_metadata (
  metadata_partition        INTEGER NOT NULL,
  immutable_data            BYTEA NOT NULL,
  immutable_data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY(metadata_partition)
);

CREATE TABLE cluster_membership
(
    host_id              BYTEA NOT NULL,
    rpc_address          VARCHAR(15) NOT NULL,
    rpc_port             SMALLINT NOT NULL,
    role                 SMALLINT NOT NULL,
    session_start        TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    last_heartbeat       TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    record_expiry        TIMESTAMP DEFAULT '1970-01-01 00:00:01',
    insertion_order      BIGSERIAL NOT NULL UNIQUE,
    PRIMARY KEY (host_id)
);

CREATE UNIQUE INDEX cm_idx_rolehost ON cluster_membership (role, host_id);
CREATE INDEX cm_idx_rolelasthb ON cluster_membership (role, last_heartbeat);
CREATE INDEX cm_idx_rpchost ON cluster_membership (rpc_address, role);
CREATE INDEX cm_idx_lasthb ON cluster_membership (last_heartbeat);
CREATE INDEX cm_idx_recordexpiry ON cluster_membership (record_expiry);