-- Native streams: stream_segments persistence facet.  See
-- native-streams/persistence-abi.html Approach C.

CREATE TABLE stream_segments (
  shard_id        INT NOT NULL,
  namespace_id    BINARY(16) NOT NULL,
  stream_id       VARCHAR(255) NOT NULL,
  segment_id      BIGINT NOT NULL,
  txn_id          BIGINT NOT NULL,
  --
  first_offset    BIGINT NOT NULL,
  last_offset     BIGINT NOT NULL,
  item_count      INT NOT NULL,
  payload_hash    VARBINARY(64) NOT NULL,
  data            MEDIUMBLOB NOT NULL,
  data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, stream_id, segment_id, txn_id)
);
